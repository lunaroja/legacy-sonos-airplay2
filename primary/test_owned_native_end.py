"""Real SOAP/GENA lifecycle under the observed owned-member END mismatch."""
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
from xml.etree import ElementTree as X
import json,time,unittest
from controller import Controller,packed
from failure_diagnostics import diagnostic_fields
from gena_runtime import Events
from sonos_api import BackendError
from runtime_state import StateError
from peer_runtime import Advertisement
from test_primary import T,C,G,NATIVE
from test_wire import SOAPModel,event_body
import test_wire as W

class EndSOAPModel(SOAPModel):
    def __init__(self):
        super().__init__();self.end_mismatch=False;self.event_transport='STOPPED'
        self.keep_playing_after_stop=False;self.post_write=lambda action:None
    def body(self,sub):
        if sub.who!='target' or sub.service!='AVTransport' or not self.end_mismatch:
            return super().body(sub)
        p=self.peers['target'];root=X.Element('Event');i=X.SubElement(root,'InstanceID',val='0')
        X.SubElement(i,'AVTransportURI',val=p.uri)
        # Keep the mismatch only while SOAP still says PLAYING.
        X.SubElement(i,'TransportState',val=self.event_transport if p.transport=='PLAYING' else p.transport)
        return event_body('LastChange',X.tostring(root,encoding='unicode'))
    def write(self,action,**values):
        before=self.peers['target'];super().write(action,**values)
        if self.end_mismatch and action=='BecomeCoordinatorOfStandaloneGroup':
            self.update('target',uri=before.uri,transport='PLAYING')
        if self.keep_playing_after_stop and action=='Stop':self.update('target',transport='PLAYING')
    def request(self,*args,**kwargs):
        count=len(self.calls);result=super().request(*args,**kwargs)
        if len(self.calls)>count:self.post_write(self.calls[-1][0])
        return result

class OwnedNativeEndTests(unittest.TestCase):
    def fixture(self):
        with patch('test_wire.SOAPModel',EndSOAPModel):c=W.WireIntegration.fixture(self)
        self.assertEqual(c.tick(),'both');self.baseline=len(self.model.calls)
        return c
    def end(self):
        self.model.end_mismatch=True;self.model.update('coordinator',uri='',transport='STOPPED');self.model.emit_all()
    def writes(self):return [a for a,_ in self.model.calls[self.baseline:]]
    def rows(self):return [json.loads(s) for s in self.store.audit.read_text().splitlines()]
    def assert_no_cleanup(self):
        self.assertEqual(self.writes(),[]);self.assertEqual(self.store.data['active']['phase'],'verified')
        self.assertFalse(any(r['event']=='intent' and r.get('action') in ('Stop','BecomeCoordinatorOfStandaloneGroup') for r in self.rows()))
    def fake_clock(self):
        now=[time.monotonic()]
        def clock():return now[0]
        def advance(n):now[0]+=n
        self.c.clock=self.peers.clock=self.peers.wire.clock=clock;self.c.sleep=advance
        for event in self.peers.events.values():event.clock=clock
        return advance
    def test_observed_end_mismatch_detach_stop_clear_and_clean(self):
        c=self.fixture();self.end();before=self.model.peers['coordinator']
        self.assertEqual(c.tick(),'idle',self.store.public())
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
        self.assertTrue(self.model.peers['target'].idle());self.assertIsNone(self.store.data['active'])
        self.assertEqual(self.model.peers['coordinator'],replace(before,members=frozenset((C,))))
        self.assertTrue(any(r['event']=='owned_native_end_cleanup' and r['target_transport_exception'] for r in self.rows()))
        self.assertFalse(any(who=='coordinator' and not action.startswith('Get') for who,action,_ in self.model.all_calls))
    def test_roam_stop_before_event_reconciles_then_office_plays_standalone(self):
        c=self.fixture();advance=self.fake_clock()
        self.model.update('coordinator',uri='',transport='STOPPED')
        caught_up=False
        def settle(n):
            nonlocal caught_up
            if not caught_up:
                self.assertEqual(self.writes(),[])
                self.end()
                caught_up=True
            advance(n)
        c.sleep=settle
        self.assertEqual(c.tick(),'idle')
        self.model.end_mismatch=False
        self.assertEqual(c.tick(),'solo')
        self.assertTrue(self.model.peers['target'].standalone())
        self.assertEqual(self.model.peers['target'].transport,'PLAYING')
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertTrue(any(r.get('retry_kind')=='coordinator_end_event_pending'
                            and r['event']=='fresh_evidence_cleanup_proven' for r in self.rows()))
    def test_roam_end_event_absent_or_foreign_source_never_authorizes_writes(self):
        for uri in ('','http://foreign.invalid/stream'):
            with self.subTest(uri_empty=not uri):
                c=self.fixture();self.fake_clock()
                self.model.update('coordinator',uri=uri,transport='STOPPED')
                self.assertEqual(c.tick(),'recovery')
                self.assert_no_cleanup()
                self.assertEqual(sum(r['event']=='fresh_evidence_retry' for r in self.rows()),
                                 1 if not uri else 0)
    def test_end_arriving_between_peer_read_and_group_recheck_matches_live_failure(self):
        c=self.fixture();original=self.peers.read;triggered=[False];failures=[];read=c._read
        def end_after_target(who):
            f=original(who)
            if who=='target' and not triggered[0]:triggered[0]=True;self.end()
            return f
        def record_failure(*args,**kwargs):
            try:return read(*args,**kwargs)
            except BackendError as error:failures.append(diagnostic_fields(error));raise
        self.peers.read=end_after_target;c._read=record_failure
        self.assertEqual(c.tick(),'idle');self.assertTrue(failures)
        first=failures[0];self.assertEqual(first['gena_read_stage'],'group_recheck')
        self.assertEqual(first['gena_transport'],'STOPPED');self.assertEqual(first['soap_transport'],'PLAYING')
        self.assertFalse(first['gena_source_revision_matches']);self.assertTrue(first['gena_uri_revision_matches'])
        self.assertTrue(first['gena_topology_revision_matches']);self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
    def test_raw_transport_and_global_recheck_remain_unchanged(self):
        c=self.fixture();self.end();context=c._begin_owned_end();facts=c._read(True,end=context)
        self.assertEqual(facts['target'].peer.transport,'PLAYING');self.assertFalse(facts['target'].revision.continuous)
        with self.assertRaises(BackendError):self.peers.recheck('target',facts['target'])
        self.assert_no_cleanup()
    def test_shutdown_uses_same_complete_end_cleanup(self):
        c=self.fixture();self.end();self.assertTrue(c.shutdown());self.assertTrue(self.model.peers['target'].idle())
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
    def test_receiver_end_with_coordinator_still_playing_does_not_get_exception(self):
        c=self.fixture();self.model.end_mismatch=True;self.model.emit_all();self.session=replace(self.session,state='ended',media=False)
        self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_pause_hold_transport_mismatch_does_not_get_end_exception(self):
        c=self.fixture();self.model.end_mismatch=True
        self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
        self.session=replace(self.session,state='unproven',media=False)
        self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_coherent_source_end_also_requires_direct_native_transition(self):
        c=self.fixture();self.model.update('coordinator',uri='',transport='STOPPED');self.model.emit_all()
        self.assertEqual(c.tick(),'idle');self.assertTrue(self.model.peers['target'].idle())
    def test_coordinator_foreign_or_away_back_before_end_is_rejected(self):
        for sequence in (('http://foreign.invalid/private',''),('http://foreign.invalid/private',NATIVE,'')):
            with self.subTest(sequence_length=len(sequence)):
                self.doCleanups();c=self.fixture();self.model.end_mismatch=True
                for uri in sequence:self.model.update('coordinator',uri=uri,transport='STOPPED' if not uri else 'PLAYING');self.model.emit_all()
                self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
                self.assertNotIn('http://foreign.invalid/private',json.dumps(self.rows()))
    def test_coordinator_aba_is_rejected_even_when_target_is_coherent(self):
        c=self.fixture()
        for uri in ('http://foreign.invalid/private',NATIVE,''):
            self.model.update('coordinator',uri=uri,transport='STOPPED' if not uri else 'PLAYING');self.model.emit_all()
        self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_target_foreign_away_back_before_end_is_rejected(self):
        c=self.fixture()
        for uri in ('http://foreign.invalid/private','x-rincon:'+C):self.model.update('target',uri=uri);self.model.emit_all()
        self.end();self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_identity_boot_and_topology_changes_are_not_end_evidence(self):
        for who,change in (('target',{'boot':98}),('coordinator',{'boot':99}),
                           ('target',{'coordinator':T,'members':frozenset((T,))})):
            with self.subTest(who=who,change=tuple(change)):
                self.doCleanups();c=self.fixture();self.model.update(who,**change)
                if 'members' in change:self.model.update('coordinator',members=frozenset((C,)))
                self.end()
                self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_target_topology_away_back_before_end_is_rejected(self):
        c=self.fixture();self.model.update('target',coordinator=T,members=frozenset((T,)))
        self.model.update('coordinator',members=frozenset((C,)));self.model.emit_all()
        self.model.update('target',coordinator=C,members=frozenset((T,C)))
        self.model.update('coordinator',members=frozenset((T,C)));self.model.emit_all();self.end()
        self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_subscription_loss_or_expiry_is_not_an_exception(self):
        for who in ('target','coordinator'):
            for mode in ('lost','expired'):
                with self.subTest(who=who,mode=mode):
                    self.doCleanups();c=self.fixture();self.end();sub=self.peers.events[who].subscriptions[0]
                    if mode=='lost':sub.lost=True
                    else:sub.deadline=0
                    self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_gap_between_snapshot_and_transition_proof_is_rejected(self):
        c=self.fixture();self.end();context=c._begin_owned_end();original=context._direct
        def gap(who,before,after):
            if who=='target':self.peers.events[who].subscriptions[0].lost=True
            return original(who,before,after)
        context._direct=gap
        with self.assertRaises(BackendError):c._read(True,end=context)
        self.assert_no_cleanup()
    def test_restarted_subscription_cannot_reconstruct_end_transition(self):
        for who in ('target','coordinator'):
            with self.subTest(who=who):
                self.doCleanups();c=self.fixture();self.end();old=self.peers.events[who]
                fresh=Events(old.config,old.ports,old.bind,0,http=self.model)
                self.peers.events[who]=self.model.events[who]=fresh;self.addCleanup(fresh.close)
                for sub in fresh.subscriptions:fresh.subscribe(sub)
                self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_unknown_or_transitioning_transport_disagreement_still_refuses(self):
        for transport in ('TRANSITIONING','PAUSED_PLAYBACK','unrecognized-value'):
            with self.subTest(transport=transport):
                self.doCleanups();c=self.fixture();self.model.event_transport=transport;self.end()
                self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_active_intent_never_enters_end_cleanup(self):
        c=self.fixture();self.store.data['active']['phase']='intent';self.store.persist();self.end()
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertEqual(self.store.data['active']['phase'],'intent')
    def test_play_volume_and_nonempty_uri_cannot_use_end_context(self):
        c=self.fixture();self.end();context=c._begin_owned_end()
        for action,values in (('Play',{'Speed':'1'}),('SetVolume',{'DesiredVolume':40}),
                              ('SetAVTransportURI',{'CurrentURI':NATIVE,'CurrentURIMetaData':''})):
            with self.subTest(action=action):
                with self.assertRaises(BackendError):c._write(action,lambda _:True,group=True,end=context,**values)
        self.assert_no_cleanup()
    def test_foreign_source_after_detach_prevents_stop_clear(self):
        c=self.fixture();self.end()
        def foreign(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':self.model.update('target',uri='http://foreign.invalid/after');self.model.emit_all()
        self.model.post_write=foreign;self.assertEqual(c.tick(),'recovery')
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup']);self.assertEqual(self.store.data['active']['phase'],'intent')
    def test_coordinator_new_source_after_detach_is_preserved(self):
        c=self.fixture();self.end()
        def foreign(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':self.model.update('coordinator',uri='http://foreign.invalid/after',transport='PLAYING');self.model.emit_all()
        self.model.post_write=foreign;self.assertEqual(c.tick(),'recovery')
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup']);self.assertEqual(self.model.peers['coordinator'].uri,'http://foreign.invalid/after')
    def test_target_uri_aba_during_detach_prevents_followup_writes(self):
        c=self.fixture();self.end()
        def foreign(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':
                for uri in ('http://foreign.invalid/after','x-rincon:'+C):
                    self.model.update('target',uri=uri);self.model.emit_all()
        self.model.post_write=foreign;self.assertEqual(c.tick(),'recovery')
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
    def test_group_aba_during_detach_prevents_followup_writes(self):
        c=self.fixture();self.end()
        def regroup(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':
                self.model.update('target',coordinator=C,members=frozenset((T,C)))
                self.model.update('coordinator',members=frozenset((T,C)));self.model.emit_all()
                self.model.update('target',coordinator=T,members=frozenset((T,)))
                self.model.update('coordinator',members=frozenset((C,)));self.model.emit_all()
        self.model.post_write=regroup;self.assertEqual(c.tick(),'recovery')
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
    def test_uuid_change_before_cleanup_is_rejected(self):
        for who in ('target','coordinator'):
            with self.subTest(who=who):
                self.doCleanups();c=self.fixture();self.end()
                self.model.update(who,uuid='RINCON_00000000000301400')
                self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_journal_failure_before_first_cleanup_intent_does_not_write(self):
        c=self.fixture();self.end();original=self.store.event
        def event(name,**fields):
            if name=='owned_native_end_cleanup':raise StateError('journal_full_recovery_required')
            return original(name,**fields)
        self.store.event=event;self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_startup_keeps_strict_transport_proof(self):
        with patch('test_wire.SOAPModel',EndSOAPModel):c=W.WireIntegration.fixture(self)
        self.model.end_mismatch=True;self.model.update('target',transport='PLAYING');self.model.emit_all()
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.model.calls,[]);self.assertIsNone(self.store.data['active'])
    def test_owned_solo_keeps_strict_transport_proof(self):
        with patch('test_wire.SOAPModel',EndSOAPModel):c=W.WireIntegration.fixture(self)
        self.peers.ad=Advertisement(None,0,False);self.assertEqual(c.tick(),'solo');self.baseline=len(self.model.calls)
        self.end();self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup()
    def test_coordinator_transport_aba_after_end_proof_is_rejected(self):
        c=self.fixture();self.end();context=c._begin_owned_end()
        for transport in ('PLAYING','STOPPED'):self.model.update('coordinator',transport=transport);self.model.emit_all()
        with self.assertRaises(BackendError):c._read(True,end=context)
        self.assert_no_cleanup()
    def test_expired_fact_during_end_read_still_refuses(self):
        c=self.fixture();advance=self.fake_clock();self.end();context=c._begin_owned_end();original=self.peers._observe
        def slow(who):
            f=original(who)
            if who=='target':advance(2.001)
            return f
        self.peers._observe=slow
        with self.assertRaises(BackendError) as caught:c._read(True,end=context)
        self.assertEqual(caught.exception.args,('peer_evidence_expired',));self.assert_no_cleanup()
    def test_stop_must_reach_real_coherent_stopped_before_clear(self):
        c=self.fixture();self.fake_clock();self.end();self.model.keep_playing_after_stop=True
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop'])
        self.assertEqual(self.model.peers['target'].transport,'PLAYING');self.assertIsNotNone(self.store.data['active'])
    def test_restart_does_not_resume_or_replay_end_cleanup(self):
        c=self.fixture();self.end();new=Controller(self.peers,lambda:self.session,self.wave,self.scope,self.store)
        self.assertEqual(new.tick(),'recovery');self.assertFalse(new.shutdown());self.assert_no_cleanup()
    def test_detach_response_timeout_finalizes_only_when_target_is_already_idle(self):
        for idle in (True,False):
            with self.subTest(idle=idle):
                self.doCleanups();c=self.fixture();self.fake_clock();self.end()
                def lose_response(action):
                    if action!='BecomeCoordinatorOfStandaloneGroup':return
                    if idle:
                        self.model.update('target',uri='',transport='STOPPED')
                        self.model.emit_all()
                    error=BackendError('http_transport')
                    error.http_diagnostics={'speaker_transport_reason':'timeout',
                                            'speaker_http_phase':'response_headers'}
                    raise error
                self.model.post_write=lose_response
                self.assertEqual(c.tick(),'idle' if idle else 'recovery')
                self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
                self.assertFalse(any(row['event']=='acknowledged' and
                    row.get('action')=='BecomeCoordinatorOfStandaloneGroup' for row in self.rows()))
                self.assertEqual(any(row['event']=='detach_timeout_already_idle' for row in self.rows()),idle)
                if idle:self.assertIsNone(self.store.data['active'])
                else:self.assertEqual(self.store.data['active']['phase'],'intent')

    def test_ambiguous_detach_is_never_replayed(self):
        c=self.fixture();self.end()
        def lose_ack(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':raise BackendError('http_deadline')
        self.model.post_write=lose_ack;self.assertEqual(c.tick(),'recovery');count=len(self.model.calls)
        c.tick();c.shutdown();self.assertEqual(len(self.model.calls),count)
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup']);self.assertEqual(self.store.data['active']['phase'],'intent')

if __name__=='__main__':unittest.main()
