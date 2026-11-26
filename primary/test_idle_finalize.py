"""Explicit journal finalization after verified idle: no LAN or speaker writes."""
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
import io,json,tempfile,unittest
import recovery,office_primary
import test_primary as model
from controller import Controller
from runtime_state import ProductScope,Store,StateError
from sonos_api import BackendError
from session import Session
from wav_http import Wave

class IdleFinalize(unittest.TestCase):
    def fixture(self,phase='verified'):
        self.now=100.;self.tmp=tempfile.TemporaryDirectory(dir=Path(__file__).parent);self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name);self.scope=ProductScope(root/'scope',(model.T,model.C));self.addCleanup(self.scope.close)
        self.store=Store(root/'state','synthetic');self.model=model.Model(lambda:self.now)
        self.session=Session('generation1','present',True,model.G,0,0,self.now,1)
        self.wave=Wave('127.0.0.1',19091,'127.0.0.2',present=lambda:self.session.state=='present');self.addCleanup(self.wave.close)
        self.ctrl=Controller(self.model,lambda:self.session,self.wave,self.scope,self.store,clock=lambda:self.now,sleep=self.advance)
        self.assertEqual(self.ctrl.tick(),'both');self.store.data['active']['phase']=phase;self.store.persist()
        # Simulate operator compensation; the finalizer never performs this.
        self.model.update('target',uri='',transport='STOPPED',coordinator=model.T,members=frozenset((model.T,)))
        self.model.update('coordinator',uri='http://foreign.invalid/current',members=frozenset((model.C,)),transport='PLAYING')
        self.ctrl=Controller(self.model,lambda:self.session,self.wave,self.scope,self.store,clock=lambda:self.now,sleep=self.advance)
        self.model.calls.clear();self.model.reads.clear();self.model.write=Mock(side_effect=AssertionError('remote write forbidden'))
        return self.ctrl
    def advance(self,n):self.now+=n
    def test_verified_checkpoint_can_finalize_idle_without_replaying_cleanup(self):
        c=self.fixture();self.assertEqual(recovery.proposal(c)['status'],'checkpoint_changed')
        self.model.reads.clear();before=self.model.peers['coordinator'];p=recovery.finalize_proposal(c)
        self.assertEqual(p['status'],'reviewable_idle_finalize');self.assertFalse(p['speaker_writes'])
        self.assertEqual(recovery.finalize(c,p['fingerprint'])['status'],'finalized_idle')
        self.assertEqual(self.model.peers['coordinator'],before);self.assertEqual(set(self.model.reads),{'target'});self.model.write.assert_not_called()
        self.assertIsNone(self.store.data['active']);self.assertEqual(c.mode,'idle');self.assertFalse(c.failed)
        events=[json.loads(x)['event'] for x in self.store.audit.read_text().splitlines()]
        self.assertEqual(events[-2:],['idle_finalize_intent','idle_finalized'])
    def test_ambiguous_intent_has_separate_idle_acknowledgement_only(self):
        c=self.fixture('intent');self.assertEqual(recovery.proposal(c)['status'],'uncertain_write_requires_manual_review')
        p=recovery.finalize_proposal(c);self.assertEqual(recovery.finalize(c,p['fingerprint'])['status'],'finalized_idle');self.model.write.assert_not_called()
    def test_auto_recovery_waits_for_physical_idle_then_clears_pending_join(self):
        c=self.fixture('intent')
        self.store.data['active']['action']='SetAVTransportURI'
        self.store.persist()
        self.model.update('target',uri='x-rincon:'+model.C,transport='PLAYING',
                          coordinator=model.C,members=frozenset((model.T,model.C)))
        def finish_playback(delay):
            self.assertEqual(delay,5)
            self.assertIsNotNone(self.store.data['active'])
            self.model.write.assert_not_called()
            self.model.update('target',uri='',transport='STOPPED',
                              coordinator=model.T,members=frozenset((model.T,)))
        runtime=office_primary.Runtime({})
        runtime.peers=Mock()
        runtime.controller=c
        runtime.done=SimpleNamespace(is_set=lambda:False,wait=Mock(side_effect=finish_playback))
        with redirect_stdout(io.StringIO()) as output:
            self.assertTrue(runtime._reconcile_idle_startup())
        self.assertIn('"automatic": true',output.getvalue())
        runtime.done.wait.assert_called_once()
        self.assertIsNone(self.store.data['active'])
        self.assertEqual(set(self.model.reads),{'target'})
        self.model.write.assert_not_called()
    def test_nonidle_intent_refused_without_replay_or_journal_clear(self):
        for change in (dict(uri='http://foreign.invalid/current'),dict(transport='PLAYING'),dict(coordinator=model.C,members=frozenset((model.T,model.C)))):
            with self.subTest(change=change):
                c=self.fixture('intent');self.model.update('target',**change);before=json.loads(json.dumps(self.store.data))
                p=recovery.finalize_proposal(c);self.assertEqual(p['status'],'idle_finalize_target_not_idle');self.assertIsNone(p['fingerprint'])
                with self.assertRaises(BackendError):recovery.finalize(c,'a'*64)
                self.assertEqual(self.store.data,before);self.model.write.assert_not_called()
    def test_target_still_idle_but_changed_after_preview_requires_new_preview(self):
        for change in (dict(volume=62),dict(mute=True),dict(boot=99)):
            with self.subTest(change=change):
                c=self.fixture();p=recovery.finalize_proposal(c);self.model.update('target',**change)
                with self.assertRaisesRegex(BackendError,'fingerprint_changed'):recovery.finalize(c,p['fingerprint'])
                self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_journal_change_invalidates_preview(self):
        c=self.fixture();p=recovery.finalize_proposal(c);self.store.data['active']['action']='Stop';self.store.persist()
        with self.assertRaisesRegex(BackendError,'fingerprint_changed'):recovery.finalize(c,p['fingerprint'])
        self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_foreign_or_offline_coordinator_is_not_read_or_written(self):
        c=self.fixture('intent');self.model.online=False;before=self.model.peers['coordinator'];p=recovery.finalize_proposal(c)
        recovery.finalize(c,p['fingerprint']);self.assertEqual(set(self.model.reads),{'target'});self.assertEqual(self.model.peers['coordinator'],before);self.model.write.assert_not_called()
    def test_fingerprint_survives_new_subscription_revision_tokens(self):
        c=self.fixture();p=recovery.finalize_proposal(c)
        for revisions in (self.model.uri_rev,self.model.top_rev,self.model.source_rev):revisions['target']+=10
        self.now+=5;self.assertEqual(recovery.finalize_proposal(c)['fingerprint'],p['fingerprint']);self.model.write.assert_not_called()
    def test_change_between_stable_reads_refuses_preview(self):
        c=self.fixture();original=c.sleep
        def change(n):original(n);self.model.update('target',volume=44)
        c.sleep=change
        with self.assertRaisesRegex(BackendError,'target_changed'):recovery.finalize_proposal(c)
        self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_change_after_audit_intent_refuses_clear(self):
        c=self.fixture();p=recovery.finalize_proposal(c);original=self.store.event
        def event(name,**data):
            original(name,**data)
            if name=='idle_finalize_intent':self.model.update('target',uri='http://foreign.invalid/changed')
        self.store.event=event
        with self.assertRaisesRegex(BackendError,'fingerprint_changed'):recovery.finalize(c,p['fingerprint'])
        self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_scope_loss_and_existing_owner_prevent_finalization(self):
        c=self.fixture()
        with self.assertRaisesRegex(StateError,'another_product_instance'):ProductScope(self.scope.root,(model.T,model.C))
        self.scope.close()
        with self.assertRaisesRegex(StateError,'scope_lost'):recovery.finalize_proposal(c)
        self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_gena_discontinuity_refuses_even_visibly_idle_target(self):
        c=self.fixture();self.model.continuous=False
        with self.assertRaises(BackendError):recovery.finalize_proposal(c)
        self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_audit_failure_preserves_journal_and_never_writes_speakers(self):
        c=self.fixture();p=recovery.finalize_proposal(c);self.store.event=Mock(side_effect=StateError('journal_full_recovery_required'))
        with self.assertRaises(StateError):recovery.finalize(c,p['fingerprint'])
        self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_missing_fingerprint_cannot_finalize(self):
        c=self.fixture()
        with self.assertRaisesRegex(BackendError,'fingerprint_changed'):recovery.finalize(c,None)
        self.assertIsNotNone(self.store.data['active']);self.model.write.assert_not_called()
    def test_already_clean_is_idempotent_and_still_requires_scope(self):
        c=self.fixture();p=recovery.finalize_proposal(c);recovery.finalize(c,p['fingerprint']);self.model.reads.clear()
        self.assertEqual(recovery.finalize(c,p['fingerprint'])['status'],'already_clean');self.assertEqual(self.model.reads,[]);self.model.write.assert_not_called()
        self.scope.close()
        with self.assertRaises(StateError):recovery.finalize_proposal(c)

class CLI(unittest.TestCase):
    def test_explicit_flag_uses_local_finalizer_without_coordinator_wait(self):
        for apply in (False,True):
            with self.subTest(apply=apply):
                runtime=Mock();runtime.store.data={'active':{'checkpoint':{'coordinator':{}}}};runtime.peers.events={}
                result={'status':'finalized_idle' if apply else 'reviewable_idle_finalize','fingerprint':'a'*64,'speaker_writes':False}
                method='finalize' if apply else 'finalize_proposal'
                with patch.object(office_primary,'load',return_value={}),patch.object(office_primary,'Runtime',return_value=runtime),patch.object(recovery,method,return_value=result) as call,patch.object(office_primary.time,'sleep',side_effect=AssertionError('unexpected coordinator wait')),redirect_stdout(io.StringIO()):
                    args=['--config','unused','recover','--finalize-idle']+(['--apply','a'*64] if apply else [])
                    self.assertEqual(office_primary.main(args),0)
                runtime.start.assert_called_once_with(with_receiver=False,target_only=True);runtime.close.assert_called_once_with(cleanup=False);self.assertEqual(call.call_count,1)
    def test_ordinary_recovery_does_not_implicitly_finalize(self):
        runtime=Mock();runtime.store.data={'active':{'checkpoint':{}}}
        with patch.object(office_primary,'load',return_value={}),patch.object(office_primary,'Runtime',return_value=runtime),patch.object(recovery,'proposal',return_value={'status':'uncertain_write_requires_manual_review'}) as proposal,patch.object(recovery,'finalize_proposal') as finalize,redirect_stdout(io.StringIO()):
            self.assertEqual(office_primary.main(['--config','unused','recover']),2)
        proposal.assert_called_once();finalize.assert_not_called()

if __name__=='__main__':unittest.main()
