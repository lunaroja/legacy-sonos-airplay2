"""Real receiver-process loss through main/finally, with modeled speaker I/O."""
from contextlib import redirect_stdout
from dataclasses import replace
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import json,subprocess,sys,tempfile,time,unittest
import office_primary
from controller import Controller
from receiver_diagnostics import StartupStderr
from receiver_runtime import Receiver
from runtime_state import ProductScope,Store
from session import Session
from test_primary import Model,T,C,G,NATIVE
from wav_http import Wave

CHILD="""import sys
print('ready',flush=True)
sys.stdin.buffer.read(1)
sys.stderr.write('error: AirPlay private-room name PRIVATE-RAW-MARKER is already in use.\\n')
sys.stderr.flush()
raise SystemExit(17)
"""

class RuntimeHealth(unittest.TestCase):
    def test_late_pipe_failure_keeps_only_allowlisted_category(self):
        now=[0]
        watch=StartupStderr(None,clock=lambda:now[0])
        watch.feed(b'private-startup-metadata\n')
        now[0]=30
        watch.feed(b'private-runtime-value\nSPTS write failure code=')
        watch.feed(b'4 bytes=98765\n')
        self.assertIsNone(watch.snapshot(None)['receiver_runtime_category'])
        result=watch.snapshot(1)
        self.assertEqual(result['receiver_runtime_category'],'pipe_write_deadline')
        self.assertNotIn('98765',json.dumps(result))
        self.assertNotIn('private',json.dumps(result))
    def fixture(self,mode='idle',*,child_source=CHILD):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);root=Path(tmp.name)
        r=office_primary.Runtime({});self.runtime=r
        r.scope=ProductScope(root/'scope',(T,C));r.store=Store(root/'state','synthetic-health')
        self.model=Model(time.monotonic);self.model.close=lambda:None;self.model.maintain_target=lambda:None;r.peers=self.model
        self.session=Session('health-generation','unproven' if mode=='idle' else 'present',mode!='idle',G,0,0,time.monotonic(),1)
        if mode=='solo':self.model.online=False
        def invalidate():self.session=replace(self.session,state='lost',media=False)
        r.observer=SimpleNamespace(invalidate=invalidate,failure=None)
        r.wave=Wave('127.0.0.1',19091,'127.0.0.2',present=lambda:self.session.state=='present')
        r.controller=Controller(self.model,lambda:self.session,r.wave,r.scope,r.store)
        r.channel=SimpleNamespace(closed=False,failure=None)
        def close_channel():r.channel.closed=True;invalidate()
        r.channel.close=close_channel
        r.drain=SimpleNamespace(error=False,thread=SimpleNamespace(is_alive=lambda:True),close=lambda:None)
        r.receiver=Receiver({},root/'receivers',r.channel)
        child=subprocess.Popen([sys.executable,'-I','-S','-B','-u','-c',child_source],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.child=child;r.receiver.proc=child;r.receiver.stderr=StartupStderr(child.stderr);r.receiver.stderr.start()
        self.assertEqual(child.stdout.readline(),b'ready\n')
        def cleanup():
            if child.poll() is None:child.terminate();child.wait(timeout=2)
            for stream in (child.stdin,child.stdout,child.stderr):
                if stream and not stream.closed:stream.close()
            r.close(cleanup=False)
        self.addCleanup(cleanup)
        r.start=lambda with_receiver=True:None
        self.calls=0
        def wait(_timeout):
            self.calls+=1
            if self.calls==2:self.failure_action()
            # The unpatched idle loop would continue indefinitely. End it so
            # this regression fails as exit 0 instead of hanging the suite.
            if self.calls>=4:r.done.set()
        r.wake=SimpleNamespace(wait=wait,clear=lambda:None,set=lambda:None)
        return r

    def die(self):
        self.child.stdin.write(b'x');self.child.stdin.flush();self.child.wait(timeout=2)

    def execute(self):
        output=StringIO()
        with patch.object(office_primary,'load',return_value={}),patch.object(office_primary,'Runtime',return_value=self.runtime),patch.object(office_primary.signal,'signal'),redirect_stdout(output):
            code=office_primary.main(['--config','unused-synthetic','run'])
        self.assertNotIn('PRIVATE-RAW-MARKER',output.getvalue())
        self.rows=[json.loads(line) for line in output.getvalue().splitlines()]
        return code

    def test_receiver_dies_after_idle_status_and_main_exits_nonzero(self):
        r=self.fixture();self.failure_action=self.die
        self.assertEqual(self.execute(),2)
        self.assertEqual(self.rows[0]['status'],'idle');self.assertTrue(self.rows[0]['receiver_alive'])
        failed=self.rows[-1];self.assertEqual(failed['status'],'runtime_failed');self.assertFalse(failed['receiver_alive'])
        self.assertEqual(failed['reason'],'receiver_exited');self.assertEqual(failed['receiver_exit_status'],17)
        self.assertEqual(failed['receiver_startup_category'],'mdns_name_collision')
        self.assertEqual(self.model.calls,[]);self.assertIsNone(r.store.data['active']);self.assertLessEqual(self.calls,2)

    def test_receiver_death_while_solo_releases_owned_target_in_finally(self):
        r=self.fixture('solo');self.failure_action=self.die
        self.assertEqual(self.execute(),2);self.assertTrue(self.model.peers['target'].idle())
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI','Play','Stop','SetAVTransportURI'])
        self.assertIsNone(r.store.data['active']);self.assertEqual(self.model.peers['coordinator'].uri,NATIVE)

    def test_poisoned_pipe_remains_in_journal_after_solo_cleanup(self):
        child_source=CHILD.replace('error: AirPlay private-room name PRIVATE-RAW-MARKER is already in use.',
                                  'PRIVATE-RAW-MARKER SPTS write failure code=2 bytes=98765')
        r=self.fixture('solo',child_source=child_source);self.failure_action=self.die
        self.assertEqual(self.execute(),2)
        reopened=Store(r.store.root,'synthetic-health')
        self.assertEqual(reopened.public()['status'],'idle')
        text=reopened.audit.read_text()
        self.assertNotIn('PRIVATE-RAW-MARKER',text);self.assertNotIn('98765',text)
        failures=[row for line in text.splitlines() if (row:=json.loads(line))['event']=='receiver_runtime_failed']
        self.assertEqual(len(failures),1)
        failure=failures[0]
        self.assertEqual(failure['receiver_runtime_category'],'pipe_poisoned')
        self.assertEqual(failure['controller_operation'],'Play')
        self.assertEqual(failure['controller_phase'],'runtime_health_check')
        self.assertEqual(failure['checkpoint_phase'],'verified')
        self.assertEqual(failure['retry_decision'],'no_command_replay')

    def test_receiver_death_while_both_detaches_target_preserves_native_source(self):
        r=self.fixture('both');original=self.model.peers['coordinator'];self.failure_action=self.die
        self.assertEqual(self.execute(),2);self.assertTrue(self.model.peers['target'].idle())
        self.assertEqual(self.model.peers['coordinator'],original)
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI','BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
        self.assertIsNone(r.store.data['active'])

    def test_foreign_manual_target_or_coordinator_is_preserved_on_death(self):
        for who in ('target','coordinator'):
            with self.subTest(who=who):
                self.doCleanups();r=self.fixture('both');counts=[]
                def fail():
                    self.model.update(who,uri='http://foreign.invalid/manual',transport='PLAYING')
                    counts.append(len(self.model.calls));self.die()
                self.failure_action=fail
                self.assertEqual(self.execute(),2);self.assertEqual(len(self.model.calls),counts[0])
                self.assertEqual(self.model.peers[who].uri,'http://foreign.invalid/manual')
                self.assertIsNotNone(r.store.data['active']);self.assertEqual(r.store.data['status'],'recovery')

    def test_receiver_dies_during_controller_write_no_stale_success_published(self):
        r=self.fixture('both');self.failure_action=lambda:None
        self.model.after=lambda action:self.die() if self.child.poll() is None else None
        self.assertEqual(self.execute(),2);self.assertEqual([x['status'] for x in self.rows],['runtime_failed'])
        self.assertTrue(self.model.peers['target'].idle());self.assertIsNone(r.store.data['active'])

    def test_channel_or_drain_failure_with_live_child_is_detected(self):
        for kind,reason in (('channel','receiver_channel_lost'),('error','receiver_drain_failed'),('thread','receiver_drain_stopped')):
            with self.subTest(kind=kind):
                self.doCleanups();r=self.fixture()
                def fail():
                    if kind=='channel':
                        r.channel.closed=True
                        r.channel.failure='event_rejected'
                        r.observer.failure='event_continuity'
                    elif kind=='error':r.drain.error=True
                    else:r.drain.thread=SimpleNamespace(is_alive=lambda:False)
                self.failure_action=fail
                self.assertEqual(self.execute(),2);self.assertEqual(self.rows[-1]['reason'],reason)
                self.assertTrue(self.rows[-1]['receiver_alive']);self.assertIsNotNone(self.child.poll())
                if kind=='channel':
                    self.assertEqual(self.rows[-1]['receiver_channel_category'],'event_rejected')
                    self.assertEqual(self.rows[-1]['receiver_event_category'],'event_continuity')

    def test_startup_failure_keeps_bounded_diagnostic_before_receiver_close(self):
        r=self.fixture();self.failure_action=lambda:None;self.die()
        def fail_start(**_):raise RuntimeError('PRIVATE-RAW-MARKER')
        r.start=fail_start
        self.assertEqual(self.execute(),2);self.assertEqual(self.rows[-1]['receiver_exit_status'],17)
        self.assertEqual(self.rows[-1]['receiver_startup_category'],'mdns_name_collision')
        self.assertIsNone(r.receiver.stderr)

    def test_healthy_idle_and_paused_both_do_not_false_fail(self):
        for mode in ('idle','both'):
            with self.subTest(mode=mode):
                self.doCleanups();r=self.fixture(mode)
                def pause():
                    self.session=replace(self.session,state='unproven',media=False)
                    if mode=='both':self.model.update('coordinator',transport='PAUSED_PLAYBACK')
                self.failure_action=pause
                self.assertEqual(self.execute(),0)
                self.assertFalse(any(x['status']=='runtime_failed' for x in self.rows))
                self.assertTrue(self.model.peers['target'].idle());self.assertIsNone(r.store.data['active'])

if __name__=='__main__':unittest.main()
