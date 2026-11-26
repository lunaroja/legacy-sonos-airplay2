"""One read-only resume-race retry against real SOAP/GENA models, no LAN access."""
from dataclasses import replace
from unittest.mock import Mock
import copy,unittest
from controller import Controller,packed
from failure_diagnostics import diagnostic_fields
from runtime_state import StateError
from sonos_api import BackendError
from test_primary import T,C,NATIVE
import test_owned_native_hold as H

TRUE_FIELDS=('gena_events_available','gena_snapshot_available','gena_subscriptions_initialized',
 'gena_subscriptions_continuous','gena_subscriptions_unexpired','gena_uri_matches',
 'gena_topology_matches','gena_revision_tokens_present','gena_uri_revision_matches','gena_topology_revision_matches')
def race_error(**overrides):
    error=BackendError('peer_evidence_changed');error.evidence_diagnostics=dict.fromkeys(TRUE_FIELDS,True)
    error.evidence_diagnostics.update(gena_peer='target',gena_read_stage='group_recheck',gena_transport='PLAYING',
        soap_transport='STOPPED',gena_transport_matches=False,gena_source_revision_matches=False,
        gena_revision_matches=False,gena_subscription_lost=False,peer_evidence_age_ms=453,peer_evidence_age_clipped=False)
    error.evidence_diagnostics.update(overrides);return error

class TransportRaceRetry(unittest.TestCase):
    fixture=H.NativeHold.fixture;rows=H.NativeHold.rows;writes=H.NativeHold.writes
    hold=H.NativeHold.hold;resume=H.NativeHold.resume;fake_clock=H.NativeHold.fake_clock;end=H.NativeHold.end
    def setup_race(self):
        c=self.fixture();self.advance=self.fake_clock();self.errors=[];self.reads=[]
        self.hold();self.assertEqual(c.tick(),'both');self.model.end_mismatch=False
        self.model.update('target',transport='STOPPED');self.model.emit_all()
        original=self.peers.read;triggered=[False];read=c._read
        def resume_after_target(who):
            fact=original(who);self.reads.append(who)
            if who=='target' and not triggered[0]:
                triggered[0]=True;self.model.update('target',transport='PLAYING')
                self.model.update('coordinator',transport='PLAYING');self.model.emit_all();self.session=replace(self.session,media=True)
            return fact
        def record(*args,**kwargs):
            try:return read(*args,**kwargs)
            except BackendError as error:self.errors.append(diagnostic_fields(error));raise
        self.peers.read=resume_after_target;c._read=record
        return c
    def injected(self,error=None):
        c=self.fixture();self.advance=self.fake_clock();c._mark_native_hold();original=c._owned_read
        c._owned_read=Mock(side_effect=error or race_error());return c
    def no_write(self):self.assertEqual(self.writes(),[])
    def events(self,name):return [r for r in self.rows() if r['event']==name]
    def test_actual_resume_event_advances_between_soap_and_group_recheck(self):
        c=self.setup_race();before=copy.deepcopy(self.store.data);expected=packed(c.expected)
        self.assertEqual(c.tick(),'both');self.no_write();self.assertEqual(self.store.data,before);self.assertEqual(packed(c.expected),expected)
        self.assertEqual(self.reads,['target','coordinator','target','coordinator'])
        first=self.errors[0];self.assertEqual(first['gena_read_stage'],'group_recheck')
        self.assertEqual(first['gena_transport'],'PLAYING');self.assertEqual(first['soap_transport'],'STOPPED')
        self.assertFalse(first['gena_source_revision_matches']);self.assertTrue(first['gena_uri_revision_matches']);self.assertTrue(first['gena_topology_revision_matches'])
        self.assertEqual(len(self.events('fresh_evidence_retry')),1);self.assertEqual(len(self.events('fresh_evidence_revalidated')),1)
    def test_roam_removal_during_hold_retry_proves_native_end(self):
        c=self.injected()
        def remove_roam(seconds):
            self.model.update('coordinator',uri='',transport='STOPPED')
            self.model.update('target',transport='STOPPED')
            self.model.emit_all()
            self.advance(seconds)
        c.sleep=remove_roam
        self.assertEqual(c.tick(),'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
        retry=self.events('fresh_evidence_cleanup_proven')
        self.assertEqual([row['retry_kind'] for row in retry],['target_transport_revision_race'])
        self.assertFalse(retry[0]['speaker_writes'])

    def test_outside_active_hold_is_not_eligible(self):
        c=self.injected();c.native_hold=False;c._read=Mock()
        self.assertEqual(c.tick(),'recovery');c._read.assert_not_called();self.no_write();self.assertFalse(self.events('fresh_evidence_retry'))
    def test_second_race_is_not_retried(self):
        c=self.injected();c._read=Mock(side_effect=race_error())
        self.assertEqual(c.tick(),'recovery');self.no_write();self.assertEqual(c._read.call_count,1)
        self.assertEqual(len(self.events('fresh_evidence_retry')),1);self.assertFalse(self.events('fresh_evidence_revalidated'))
    def test_persistent_transport_disagreement_fails_fresh_peer_read(self):
        c=self.setup_race();original=self.peers.read
        def persistent(who):
            if len(self.reads)==2:
                self.model.end_mismatch=True;self.model.event_transport='STOPPED';self.model.emit_all()
            return original(who)
        self.peers.read=persistent
        self.assertEqual(c.tick(),'recovery');self.no_write();self.assertEqual(self.errors[-1]['gena_read_stage'],'peer_read')
    def test_every_safety_flag_must_have_exact_required_value(self):
        cases=[{key:False} for key in TRUE_FIELDS]+[
            {'gena_peer':'coordinator'},{'gena_read_stage':'peer_read'},{'gena_transport':'STOPPED'},
            {'soap_transport':'PLAYING'},{'gena_transport_matches':True},{'gena_source_revision_matches':True},
            {'gena_revision_matches':True},{'gena_subscription_lost':True},{'peer_evidence_age_clipped':True},
            {'peer_evidence_age_ms':2001},{'peer_evidence_age_ms':True},{'peer_evidence_age_ms':-1}]
        for fields in cases:
            with self.subTest(fields=fields):
                self.doCleanups();c=self.injected(race_error(**fields));c._read=Mock()
                self.assertEqual(c.tick(),'recovery');self.no_write();c._read.assert_not_called();self.assertFalse(self.events('fresh_evidence_retry'))
    def test_http_and_expired_evidence_are_not_retried(self):
        for reason in ('http_deadline','http_transport','peer_evidence_expired'):
            with self.subTest(reason=reason):
                self.doCleanups();error=race_error();error.args=(reason,);c=self.injected(error);c._read=Mock()
                self.assertEqual(c.tick(),'recovery');c._read.assert_not_called();self.no_write()
    def test_foreign_source_and_away_back_both_peers_refuse_original_checkpoint(self):
        for who in ('target','coordinator'):
            for back in (False,True):
                with self.subTest(who=who,back=back):
                    self.doCleanups();c=self.injected();uri=self.model.peers[who].uri
                    self.model.update(who,uri='http://foreign.invalid/source');self.model.emit_all()
                    if back:self.model.update(who,uri=uri);self.model.emit_all()
                    self.assertEqual(c.tick(),'recovery');self.no_write();self.assertFalse(self.events('fresh_evidence_revalidated'))
    def test_topology_away_back_refuses_even_when_final_values_match(self):
        c=self.injected()
        self.model.update('target',coordinator=T,members=frozenset((T,)));self.model.update('coordinator',members=frozenset((C,)));self.model.emit_all()
        self.model.update('target',coordinator=C,members=frozenset((T,C)));self.model.update('coordinator',members=frozenset((T,C)));self.model.emit_all()
        self.assertEqual(c.tick(),'recovery');self.no_write()
    def test_boot_identity_and_subscription_discontinuity_refuse(self):
        for who in ('target','coordinator'):
            for kind in ('boot','uuid','lost','expired'):
                with self.subTest(who=who,kind=kind):
                    self.doCleanups();c=self.injected()
                    if kind=='boot':self.model.update(who,boot=99);self.model.emit_all()
                    elif kind=='uuid':self.model.update(who,uuid='RINCON_00000000000301400')
                    elif kind=='lost':self.peers.events[who].subscriptions[0].lost=True
                    else:self.peers.events[who].subscriptions[0].deadline=0
                    self.assertEqual(c.tick(),'recovery');self.no_write()
    def test_backoff_has_no_peer_read_or_speaker_write(self):
        c=self.injected();original=self.peers.read;self.peers.read=Mock(side_effect=original)
        def sleeping(n):
            self.assertEqual(n,.5);self.peers.read.assert_not_called();self.no_write();self.advance(n)
        c.sleep=sleeping;self.assertEqual(c.tick(),'both');self.no_write();self.assertEqual(self.peers.read.call_count,2)
    def test_budget_includes_initial_failed_read_and_backoff(self):
        c=self.injected();c._read=Mock()
        def delayed(**kwargs):self.advance(4.6);raise race_error()
        c._owned_read=delayed;self.assertEqual(c.tick(),'recovery');c._read.assert_not_called();self.no_write()
        self.assertEqual(self.rows()[-1]['controller_failure_reason'],'read_retry_exhausted')
    def test_late_success_cannot_authorize_pending_volume(self):
        c=self.injected();original=c._read
        def slow(*args,**kwargs):
            facts=original(*args,**kwargs);self.advance(5);return facts
        c._read=slow;self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        self.assertEqual(c.tick(),'recovery');self.no_write();self.assertEqual(self.rows()[-1]['controller_failure_reason'],'read_retry_exhausted')
    def test_checkpoint_change_during_backoff_or_fresh_read_refuses(self):
        for stage in ('backoff','read'):
            with self.subTest(stage=stage):
                self.doCleanups();c=self.injected();original=c._read
                def mutate():self.store.data['active']['action']='Stop';self.store.persist()
                if stage=='backoff':c.sleep=lambda n:mutate()
                else:
                    def read(*args,**kwargs):facts=original(*args,**kwargs);mutate();return facts
                    c._read=read
                self.assertEqual(c.tick(),'recovery');self.no_write();self.assertEqual(self.rows()[-1]['controller_failure_reason'],'read_retry_checkpoint_changed')
    def test_checkpoint_change_during_initial_failed_read_refuses_retry(self):
        c=self.injected()
        def changed(**kwargs):self.store.data['active']['action']='Stop';raise race_error()
        c._owned_read=changed;self.assertEqual(c.tick(),'recovery');self.no_write();self.assertFalse(self.events('fresh_evidence_retry'))
    def test_existing_intent_and_restart_do_not_get_retry(self):
        c=self.injected();self.store.data['active']['phase']='intent';self.store.persist()
        self.assertEqual(c.tick(),'recovery');self.no_write();self.assertFalse(self.events('fresh_evidence_retry'))
        fresh=Controller(self.peers,lambda:self.session,self.wave,self.scope,self.store)
        self.assertEqual(fresh.tick(),'recovery');self.assertFalse(fresh.shutdown());self.no_write()
    def test_write_guard_and_post_intent_reads_never_use_this_retry(self):
        for at in (1,2):
            with self.subTest(read=at):
                self.doCleanups();c=self.fixture();self.fake_clock();original=c._read;count=[0]
                def read(*args,**kwargs):
                    count[0]+=1
                    if count[0]==at:raise race_error()
                    return original(*args,**kwargs)
                c._read=read
                with self.assertRaises(BackendError):c._write('SetVolume',lambda s:True,group=True,DesiredVolume=50)
                self.no_write();self.assertFalse(self.events('fresh_evidence_retry'))
                self.assertEqual(self.store.data['active']['phase'],'verified' if at==1 else 'intent')
    def test_lost_write_ack_is_not_replayed(self):
        c=self.setup_race();self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        def fail(action):
            if action=='SetVolume':raise BackendError('http_deadline')
        self.model.post_write=fail;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),['SetVolume'])
        self.assertEqual(self.store.data['active']['phase'],'intent');c.tick();c.shutdown();self.assertEqual(self.writes(),['SetVolume'])
    def test_new_local_pause_during_retry_stays_hold_and_defers_volume(self):
        c=self.setup_race();self.session=replace(self.session,volume_revision=1,volume_milli=-15000);original=c.sleep
        def pause(n):
            original(n);self.session=replace(self.session,media=False);self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
        c.sleep=pause;self.assertEqual(c.tick(),'both');self.assertTrue(c.native_hold);self.no_write();self.assertFalse(self.events('owned_native_hold_finished'))
    def test_local_generation_change_is_resampled_before_resume_or_cleanup(self):
        c=self.setup_race();original=c.sleep
        def change(n):original(n);self.session=replace(self.session,generation='next')
        c.sleep=change;self.assertEqual(c.tick(),'recovery');self.assertFalse(self.events('owned_native_hold_finished'));self.no_write()
        self.assertEqual(self.rows()[-1]['controller_failure_reason'],'read_retry_local_changed')
    def test_positive_resume_requires_fresh_local_media(self):
        c=self.setup_race();c.native_hold=True;original=c.sleep
        def change(n):original(n);self.session=replace(self.session,media=False)
        c.sleep=change;self.assertEqual(c.tick(),'both');self.assertTrue(c.native_hold);self.assertFalse(self.events('owned_native_hold_finished'));self.no_write()
    def test_pending_volume_uses_ordinary_guard_after_successful_fresh_proof(self):
        c=self.setup_race();self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        self.assertEqual(c.tick(),'both');self.assertEqual(self.writes(),['SetVolume']);self.assertEqual(self.model.peers['coordinator'].volume,31)
        names=[r['event'] for r in self.rows()];self.assertLess(names.index('fresh_evidence_revalidated'),len(names)-1)
    def test_audit_failure_aborts_before_any_retry_action(self):
        for name in ('fresh_evidence_retry','fresh_evidence_revalidated'):
            with self.subTest(name=name):
                self.doCleanups();c=self.injected();original=self.store.event
                def event(kind,**fields):
                    if kind==name:raise StateError('journal_full_recovery_required')
                    return original(kind,**fields)
                self.store.event=event;self.assertEqual(c.tick(),'recovery');self.no_write()
    def test_cleanup_and_shutdown_fail_without_read_retry(self):
        for operation in ('cleanup','shutdown'):
            with self.subTest(operation=operation):
                self.doCleanups();c=self.injected()
                if operation=='cleanup':
                    with self.assertRaises(BackendError):c._cleanup()
                else:self.assertFalse(c.shutdown())
                self.no_write();self.assertFalse(self.events('fresh_evidence_retry'))
    def test_coherent_paused_and_incoherent_paused_oscillation_is_one_hold(self):
        c=self.fixture();self.fake_clock();self.hold();self.assertEqual(c.tick(),'both')
        self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        for _ in range(3):
            self.model.end_mismatch=False;self.model.emit_all();self.assertEqual(c.tick(),'both')
            self.assertTrue(c.native_hold);self.assertFalse(self.events('owned_native_hold_finished'));self.no_write()
            self.model.end_mismatch=True;self.model.emit_all();self.assertEqual(c.tick(),'both')
        self.assertEqual(len(self.events('owned_native_hold')),1)
        self.resume();self.assertEqual(c.tick(),'both');self.assertFalse(c.native_hold)
        self.assertEqual([e['observation'] for e in self.events('owned_native_hold_finished')],['strict_read']);self.assertEqual(self.writes(),['SetVolume'])
    def test_only_one_resume_signal_does_not_exit_hold(self):
        for local_media,coordinator_transport in ((False,'PLAYING'),(True,'PAUSED_PLAYBACK')):
            with self.subTest(local_media=local_media):
                self.doCleanups();c=self.fixture();self.fake_clock();self.hold();self.assertEqual(c.tick(),'both')
                self.model.end_mismatch=False;self.model.update('coordinator',transport=coordinator_transport);self.model.emit_all()
                self.session=replace(self.session,media=local_media,volume_revision=1,volume_milli=-15000)
                self.assertEqual(c.tick(),'both');self.assertTrue(c.native_hold);self.no_write();self.assertFalse(self.events('owned_native_hold_finished'))
    def test_coherent_pause_enters_readonly_hold_without_needing_mismatch(self):
        c=self.fixture();self.fake_clock();self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
        self.session=replace(self.session,media=False);self.assertEqual(c.tick(),'both');self.assertTrue(c.native_hold);self.no_write()
        self.assertEqual(len(self.events('owned_native_hold')),1)

if __name__=='__main__':unittest.main()
