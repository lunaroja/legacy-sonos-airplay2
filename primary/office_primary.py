"""Portable primary Office daemon: standalone WAV plus explicit native grouping."""
import argparse,json,os,select,signal,sys,threading,time
from pathlib import Path
from configuration import load,digest
from controller import Controller
from failure_diagnostics import diagnostic_fields
from framed_pcm import Records
from gena_runtime import EventError
from peer_runtime import Peers
from receiver_assets import command
from receiver_runtime import Receiver
from runtime_state import ProductScope,Store,StateError
from session import Observer,Session
from session_channel import SessionChannel
from sonos_api import BackendError,WRITES
from wav_http import Wave
import recovery

def startup_allowed(journal):
    active=journal.get('active')
    status=journal.get('status')
    if active is None:
        return status in ('idle','recovery')
    if not isinstance(active,dict):
        return False
    mode=active.get('mode')
    checkpoint=active.get('checkpoint')
    return (mode in ('solo','both') and status in (mode,'recovery')
        and active.get('phase') in ('intent','verified')
        and isinstance(active.get('action'),str) and active['action'] in WRITES
        and isinstance(active.get('generation'),str) and bool(active['generation'])
        and isinstance(checkpoint,dict)
        and set(checkpoint)==({'target','coordinator'} if mode=='both' else {'target'})
        and all(isinstance(entry,dict) and isinstance(entry.get('peer'),dict)
                for entry in checkpoint.values()))

class Drain:
    def __init__(self,observer,channel,fd,wave,wake):
        self.observer,self.channel,self.fd,self.wave,self.wake=observer,channel,fd,wave,wake
        self.done=threading.Event();self.thread=None;self.error=False
    def start(self):
        self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()
    def run(self):
        parser=Records()
        try:
            while not self.done.is_set():
                self.channel.drain()
                if self.channel.closed:raise RuntimeError('receiver_channel_lost')
                if select.select([self.fd],[],[],.02)[0]:
                    data=os.read(self.fd,32768)
                    if data:
                        for record in parser.feed(data,time.monotonic_ns()):
                            if not self.channel.observe_record(record,self.fd):raise RuntimeError('receiver_fifo_lost')
                            e=self.observer.evidence()
                            if record.kind in (3,4):self.wave.flush()
                            elif record.kind==1 and e.media:self.wave.push(e.generation,record.pcm)
                parser.check(time.monotonic_ns())
                e=self.observer.evidence()
                if e.state in ('ended','lost'):self.wave.revoke()
        except Exception:
            self.error=True;self.observer.invalidate();self.wave.revoke();self.wake()
    def close(self):
        self.done.set()
        if self.thread:self.thread.join(2)

class Runtime:
    def __init__(self,config):
        self.config=config;self.done=threading.Event();self.wake=threading.Event()
        self.scope=self.store=self.peers=self.observer=self.wave=self.channel=self.receiver=self.drain=self.controller=None
        self.fd=None
    def _start_peers(self,target_only):
        self.peers=Peers(self.config,wake=self.wake.set)
        self.peers.start(target_only=target_only)
        self.controller=Controller(self.peers,self.observer.evidence,self.wave,self.scope,self.store,
                                   max_volume=self.config['max_volume'])
    def _reconcile_idle_startup(self):
        print(json.dumps({'status':'waiting_for_safe_idle','speaker_writes':False}),flush=True)
        while not self.done.is_set():
            try:
                if self.peers is None:
                    self._start_peers(target_only=True)
                    self.peers.wait_initial(with_coordinator=False)
                preview=recovery.finalize_proposal(self.controller)
                if preview['status']=='reviewable_idle_finalize' and not self.done.is_set():
                    outcome=recovery.finalize(self.controller,preview['fingerprint'])
                    print(json.dumps({**outcome,'automatic':True}),flush=True)
                    return True
            except (BackendError,EventError) as error:
                # Renew the observation after a lost subscription or incoherent
                # snapshot. The preceding process's command is never replayed.
                print(json.dumps({'status':'idle_recovery_observation_failed',
                                  **diagnostic_fields(error)}),flush=True)
                if self.peers:
                    self.peers.close()
                    self.peers=None
            self.done.wait(5)
        return False
    def start(self,with_receiver=True,*,target_only=False):
        c=self.config;ids=[c['target']['uuid']]+([c['coordinator']['uuid']] if c['coordinator'] else [])
        self.scope=ProductScope(c['scope_dir'],ids);self.store=Store(c['state_dir'],digest(c))
        if self.store.data.get('active') is None and self.store.data.get('status')=='recovery':
            self.store.clean() # No command or ownership exists to replay.
            self.store.event('idle_failure_restarted',speaker_writes=False)
        self.observer=Observer(changed=self.wake.set)
        self.wave=Wave(c['local_ip'],c['wav_port'],c['target']['ip'],present=lambda:self.observer.evidence().state=='present')
        if with_receiver and not startup_allowed(self.store.data):
            raise StateError('journal_recovery_required')
        if with_receiver and self.store.data.get('active'):
            if not self._reconcile_idle_startup():return
            self.peers.start_coordinator()
        else:
            self._start_peers(target_only)
        if not with_receiver:
            active=self.store.data.get('active') or {}
            if active and (target_only or (active.get('phase')=='verified' and active.get('mode') in ('both','solo'))):
                self.peers.wait_initial(with_coordinator=not target_only and 'coordinator' in active.get('checkpoint',{}))
            if self.done.is_set():raise RuntimeError('recovery_start_cancelled')
        if with_receiver:
            if self.controller.failed:raise RuntimeError('journal_recovery_required')
            self.wave.start();self.channel=SessionChannel(self.observer)
            self.receiver=Receiver(c,Path(c['state_dir'])/'receivers',self.channel);self.receiver.assert_fifo_owner()
            self.fd=os.open(self.receiver.fifo,os.O_RDWR|os.O_NONBLOCK|os.O_NOFOLLOW)
            self.receiver.start(self.fd);self.drain=Drain(self.observer,self.channel,self.fd,self.wave,self.wake.set);self.drain.start()
            deadline=time.monotonic()+5
            while not self.observer.serial and time.monotonic()<deadline:
                if not self.receiver.alive() or self.drain.error:raise RuntimeError('receiver_startup_failed')
                time.sleep(.02)
            if not self.observer.serial:raise RuntimeError('receiver_hello_timeout')
    def receiver_failed(self):
        alive=self.receiver.alive()
        reason=('receiver_exited' if not alive else
                'receiver_channel_lost' if self.channel.closed else
                'receiver_drain_failed' if self.drain.error else
                'receiver_drain_stopped' if not self.drain.thread or not self.drain.thread.is_alive() else None)
        if reason is None:return False
        # diagnostics() exposes only StartupStderr's fixed categories, bounded
        # exit status and booleans. Never serialize receiver stderr itself.
        report={'status':'runtime_failed','reason':reason,'receiver_alive':alive,
                'target_only':True,'receiver_channel_category':self.channel.failure,
                'receiver_event_category':self.observer.failure,**self.receiver.diagnostics()}
        print(json.dumps(report),flush=True)
        active=self.store.data.get('active') or {}
        action=active.get('action');phase=active.get('phase')
        try:
            self.store.event('receiver_runtime_failed',**report,
                             controller_operation=action if action in WRITES else 'receiver',
                             controller_phase='runtime_health_check',
                             checkpoint_phase=phase if phase in ('verified','intent') else None,
                             retry_decision='no_command_replay')
        except (OSError,StateError):
            print(json.dumps({'status':'receiver_failure_journal_unavailable'}),flush=True)
        self.observer.invalidate();self.wave.revoke()
        # Leave controller ownership intact for main's conditional shutdown.
        # Marking the controller failed here would prevent its safe cleanup.
        return True
    def run(self):
        last=None
        while not self.done.is_set():
            self.wake.wait(.25);self.wake.clear()
            if self.done.is_set():break
            if self.receiver_failed():return 2
            try:self.peers.maintain_target()
            except Exception as error:
                self.controller._fail(error);return 2
            state=self.controller.tick()
            # A bounded controller operation may outlast the receiver process.
            if self.receiver_failed():return 2
            if state!=last:
                print(json.dumps({'status':state,'receiver_alive':True,'target_only':True}),flush=True);last=state
            if self.controller.failed:return 2
        return 0
    def close(self,cleanup=True):
        ok=True
        if cleanup and self.controller and self.receiver:
            try:ok=self.controller.shutdown()
            except Exception:ok=False
        # Release every task-owned local resource even when journal/device
        # cleanup fails. A failed cleanup never becomes a clean state claim.
        for resource in (self.wave,self.drain,self.receiver,self.channel):
            if resource:
                try:resource.close()
                except Exception:ok=False
        if self.fd is not None:
            try:os.close(self.fd)
            except OSError:ok=False
            self.fd=None
        for resource in (self.peers,self.scope):
            if resource:
                try:resource.close()
                except Exception:ok=False
        return ok

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True)
    sub=parser.add_subparsers(dest='action',required=True)
    for name in ('validate','status','run','can-start'):sub.add_parser(name)
    p=sub.add_parser('recover');p.add_argument('--apply',metavar='FINGERPRINT')
    p.add_argument('--finalize-idle',action='store_true',help='Preview/apply local journal finalization only after stable target idle readback; no speaker writes')
    args=parser.parse_args(argv);runtime=None;exit_code=0
    try:
        config=load(args.config)
        if args.action=='validate':command(config,Path('/validation-only.conf'));print(json.dumps({'status':'valid','coordinator_optional':True}));return 0
        if args.action=='can-start':
            store=Store(config['state_dir'],digest(config))
            allowed=startup_allowed(store.data)
            print(json.dumps({'status':'start_allowed' if allowed else 'recovery_required','speaker_writes':False}))
            return 0 if allowed else 1
        if args.action=='status':
            store=Store(config['state_dir'],digest(config));print(json.dumps(store.public()));return 0
        runtime=Runtime(config)
        def stop(*_):runtime.done.set();runtime.wake.set()
        signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
        if args.action=='recover':
            runtime.start(with_receiver=False,target_only=args.finalize_idle)
            if args.finalize_idle:
                result=recovery.finalize(runtime.controller,args.apply) if args.apply else recovery.finalize_proposal(runtime.controller)
            else:
                result=recovery.apply(runtime.controller,args.apply) if args.apply else recovery.proposal(runtime.controller)
            print(json.dumps(result));exit_code=0 if result['status'] in ('already_clean','recovered','reviewable_target_cleanup','reviewable_idle_finalize','finalized_idle') else 2
        else:
            runtime.start(with_receiver=True);exit_code=runtime.run()
    except Exception as error:
        report={'status':'runtime_failed','error_type':type(error).__name__ if type(error).__name__ in ('EventError','BackendError','StateError','RuntimeError','ValueError','OSError') else 'internal_error'}
        report.update(diagnostic_fields(error))
        if type(error) is RuntimeError and error.args==('recovery_start_cancelled',):report['reason']='recovery_start_cancelled'
        if runtime and runtime.receiver:report.update(runtime.receiver.diagnostics())
        print(json.dumps(report),flush=True);exit_code=2
    finally:
        if runtime:
            try:
                if not runtime.close(cleanup=args.action=='run'):exit_code=2
            except Exception:exit_code=2
    return exit_code
if __name__=='__main__':raise SystemExit(main())
