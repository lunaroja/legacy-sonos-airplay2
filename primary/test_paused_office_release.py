"""Explicit authenticated receiver END and target-only paused-group release."""
from dataclasses import replace
import copy,unittest
from unittest.mock import Mock,patch
from controller import Controller,packed
from session import Session
from sonos_api import BackendError
from runtime_state import StateError
import test_owned_native_hold as H
import test_runtime as R
import test_owned_native_end as N
import test_wire as W
from test_primary import T,C,NATIVE

class PausedRelease(unittest.TestCase):
    fixture=H.NativeHold.fixture;hold=H.NativeHold.hold;resume=H.NativeHold.resume
    rows=H.NativeHold.rows;writes=H.NativeHold.writes;fake_clock=H.NativeHold.fake_clock;end=H.NativeHold.end
    def ready(self,coherent=False):
        c=self.fixture();self.advance=self.fake_clock();self.hold()
        if coherent:self.model.end_mismatch=False;self.model.emit_all()
        self.assertEqual(c.tick(),'both');self.assertTrue(c.native_hold);self.assertEqual(self.writes(),[])
        return c
    def local_end(self,**fields):
        self.session=replace(self.session,state='ended',media=False,end_event_confirmed=True,**fields)
    def authorized(self):return [r for r in self.rows() if r['event']=='owned_paused_release_authorized']
    def assert_clean(self,before):
        self.assertTrue(self.model.peers['target'].idle());self.assertIsNone(self.store.data['active'])
        self.assertEqual(self.model.peers['coordinator'],replace(before,members=frozenset((C,))))
        self.assertTrue(all(who=='target' for who,a,_ in self.model.all_calls if not a.startswith('Get')))
        self.assertFalse(any(a in ('Play','SetVolume') for a in self.writes()))
    def test_real_observer_end_full_hold_resume_hold_release_lifecycle(self):
        with patch('test_wire.SOAPModel',N.EndSOAPModel):c=W.WireIntegration.fixture(self)
        self.advance=self.fake_clock()
        receiver=R.SessionTests();receiver.setUp();self.addCleanup(receiver.doCleanups)
        self.session=receiver.o.evidence();c.session=receiver.o.evidence
        self.assertEqual(c.tick(),'both');self.baseline=len(self.model.calls)
        # Observer lineage is the authenticated SessionChannel projection; no
        # fabricated state=ended Session supplies this positive release test.
        receiver.now+=3;receiver.o.lineage_checked();self.session=receiver.o.evidence()
        self.hold();self.assertEqual(c.tick(),'both')
        receiver.record(1);receiver.o.lineage_checked();self.session=receiver.o.evidence()
        self.resume();self.assertEqual(c.tick(),'both')
        receiver.now+=3;receiver.o.lineage_checked();self.session=receiver.o.evidence()
        self.hold();self.assertEqual(c.tick(),'both');before=self.model.peers['coordinator']
        paused=self.peers.read('coordinator')
        receiver.record(4);receiver.event(1,5,14);receiver.o.lineage_checked();self.session=receiver.o.evidence()
        self.assertTrue(self.session.end_event_confirmed);self.assertEqual(self.session.state,'ended')
        self.assertEqual(c.tick(),'idle');self.assert_clean(before)
        final=self.peers.read('coordinator')
        self.assertEqual(final.revision.source,paused.revision.source);self.assertEqual(final.revision.uri_revision,paused.revision.uri_revision)
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
        self.assertEqual(len(self.authorized()),1);self.assertTrue(self.authorized()[0]['local_end_confirmed'])
        exits=[r['observation'] for r in self.rows() if r['event']=='owned_native_hold_finished'];self.assertEqual(exits,['strict_read','local_end'])
    def test_coherent_and_mismatching_target_both_require_positive_end(self):
        for coherent in (False,True):
            with self.subTest(coherent=coherent):
                self.doCleanups();c=self.ready(coherent);before=self.model.peers['coordinator'];self.local_end()
                self.assertEqual(c.tick(),'idle');self.assert_clean(before);self.assertEqual(len(self.authorized()),1)
    def test_every_reviewed_detach_uri_outcome_is_direct_and_target_only(self):
        for uri in ('','x-rincon:'+C,NATIVE):
            with self.subTest(outcome='empty' if not uri else 'owned'):
                self.doCleanups();c=self.ready();self.local_end();before=self.model.peers['coordinator'];write=self.model.write
                def detach(action,**values):
                    write(action,**values)
                    if action=='BecomeCoordinatorOfStandaloneGroup':self.model.update('target',uri=uri,transport='PLAYING')
                self.model.write=detach
                self.assertEqual(c.tick(),'idle');self.assert_clean(before)
                self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop']+(['SetAVTransportURI'] if uri else []))
    def test_plain_ended_session_defaults_false_and_cannot_authorize_mismatch_context(self):
        self.assertFalse(Session('one','ended',False,None,0,0,0,0).end_event_confirmed)
        for coherent in (False,True):
            with self.subTest(coherent=coherent):
                self.doCleanups();c=self.ready(coherent);self.session=replace(self.session,state='ended')
                before=self.model.peers['coordinator']
                self.assertEqual(c.tick(),'idle' if coherent else 'recovery');self.assertFalse(self.authorized())
                if coherent:self.assert_clean(before)
                else:self.assertEqual(self.writes(),[])
    def test_no_hold_loss_pause_or_new_generation_may_supply_end(self):
        for fields in ({'state':'present','media':False},{'state':'lost'},{'state':'unproven'},
                       {'generation':'new'},{'media':True},{'end_event_confirmed':False}):
            with self.subTest(fields=fields):
                self.doCleanups();c=self.ready();self.local_end();self.session=replace(self.session,**fields)
                c.tick();self.assertEqual(self.writes(),[]);self.assertFalse(self.authorized())
    def test_coherent_paused_shutdown_preserves_existing_strict_owned_cleanup(self):
        for target_transport in ('PLAYING','PAUSED_PLAYBACK'):
            with self.subTest(target_transport=target_transport):
                self.doCleanups();c=self.ready(True)
                self.model.update('target',transport=target_transport);self.model.emit_all()
                before=self.model.peers['coordinator'];self.assertTrue(c.shutdown());self.assert_clean(before)
                self.assertFalse(self.authorized());self.assertFalse(self.session.end_event_confirmed)
                self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
    def test_raw_transport_remains_playing_and_global_recheck_refuses(self):
        c=self.ready();self.local_end();context=c._begin_paused_release();facts=context.read(c.expected)
        self.assertEqual(facts['target'].peer.transport,'PLAYING');self.assertFalse(facts['target'].revision.continuous)
        with self.assertRaises(BackendError):self.peers.recheck('target',facts['target'])
        for action,values in (('Play',{'Speed':'1'}),('SetVolume',{'DesiredVolume':50}),('SetAVTransportURI',{'CurrentURI':NATIVE,'CurrentURIMetaData':''})):
            with self.assertRaises(BackendError):context.allow_action(action,values)
        self.assertEqual(self.writes(),[])
    def test_local_end_removal_or_new_begin_during_context_read_refuses_before_intent(self):
        for change in ('present','generation','marker','lost'):
            with self.subTest(change=change):
                self.doCleanups();c=self.ready();self.local_end();original=self.peers._observe;count=[0]
                def read(who):
                    fact=original(who);count[0]+=1
                    if count[0]==2:
                        self.session=replace(self.session,**({'state':'present'} if change=='present' else {'generation':'new'} if change=='generation' else {'state':'lost'} if change=='lost' else {'end_event_confirmed':False}))
                    return fact
                self.peers._observe=read;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_checkpoint_changed_before_or_during_authorization_refuses(self):
        for stage in ('before','during'):
            with self.subTest(stage=stage):
                self.doCleanups();c=self.ready();self.local_end()
                if stage=='before':self.store.data['active']['phase']='intent';self.store.persist()
                else:
                    original=self.peers.begin_owned_paused_release
                    def begin(*args):
                        value=original(*args);self.store.data['active']['action']='Stop';self.store.persist();return value
                    self.peers.begin_owned_paused_release=begin
                self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_foreign_source_and_source_away_back_on_either_peer_refuse(self):
        for who in ('target','coordinator'):
            for back in (False,True):
                with self.subTest(who=who,back=back):
                    self.doCleanups();c=self.ready();self.local_end();uri=self.model.peers[who].uri
                    self.model.update(who,uri='http://foreign.invalid/source');self.model.emit_all()
                    if back:self.model.update(who,uri=uri);self.model.emit_all()
                    self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_topology_away_back_refuses(self):
        c=self.ready();self.local_end()
        self.model.update('target',coordinator=T,members=frozenset((T,)));self.model.update('coordinator',members=frozenset((C,)));self.model.emit_all()
        self.model.update('target',coordinator=C,members=frozenset((T,C)));self.model.update('coordinator',members=frozenset((T,C)));self.model.emit_all()
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_boot_uuid_gap_expiry_or_nonmatching_transport_refuse(self):
        for who in ('target','coordinator'):
            for kind in ('boot','uuid','gap','expiry'):
                with self.subTest(who=who,kind=kind):
                    self.doCleanups();c=self.ready();self.local_end()
                    if kind=='boot':self.model.update(who,boot=99);self.model.emit_all()
                    elif kind=='uuid':self.model.update(who,uuid='RINCON_00000000000301400')
                    elif kind=='gap':self.peers.events[who].subscriptions[0].lost=True
                    else:self.peers.events[who].subscriptions[0].deadline=0
                    self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.doCleanups();c=self.ready();self.local_end();self.model.event_transport='PAUSED_PLAYBACK';self.model.emit_all()
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_stale_context_fact_is_not_redated(self):
        c=self.ready();self.local_end();original=self.peers._observe
        def slow(who):
            fact=original(who)
            if who=='target':self.advance(2.001)
            return fact
        self.peers._observe=slow;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_independent_coordinator_gain_before_pause_is_preserved_on_office_end(self):
        for coherent in (False,True):
            for gain in ({'volume':77},{'mute':True},{'volume':12,'mute':True}):
                with self.subTest(coherent=coherent,gain=gain):
                    self.doCleanups();c=self.fixture();self.fake_clock()
                    old=packed(c.expected);self.model.update('coordinator',**gain);self.model.emit_all()
                    self.hold()
                    if coherent:self.model.end_mismatch=False;self.model.emit_all()
                    self.assertEqual(c.tick(),'both');self.assertEqual(packed(c.expected),old)
                    before=self.model.peers['coordinator'];self.local_end()
                    self.assertEqual(c.tick(),'idle');self.assert_clean(before);self.assertEqual(len(self.authorized()),1)
    def test_strict_owned_shutdown_preserves_current_independent_coordinator_gain(self):
        c=self.ready(True);self.model.update('coordinator',volume=77,mute=True);self.model.emit_all()
        before=self.model.peers['coordinator'];self.assertTrue(c.shutdown());self.assert_clean(before)
        self.assertFalse(self.authorized())
    def test_strict_shutdown_does_not_gain_mismatch_exception_when_next_read_changes(self):
        c=self.ready(True);original=c._read;changed=[False]
        def read(*args,**kwargs):
            facts=original(*args,**kwargs)
            if not changed[0]:changed[0]=True;self.model.end_mismatch=True;self.model.emit_all()
            return facts
        c._read=read;self.assertFalse(c.shutdown());self.assertEqual(self.writes(),[])
        self.assertFalse(self.authorized());self.assertEqual(self.store.data['active']['phase'],'verified')
    def test_strict_shutdown_stops_after_ack_if_transport_becomes_incoherent(self):
        c=self.ready(True)
        def changed(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':
                self.model.update('target',transport='PLAYING');self.model.end_mismatch=True;self.model.emit_all()
        self.model.post_write=changed;self.assertFalse(c.shutdown())
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup']);self.assertFalse(self.authorized())
        self.assertEqual(self.store.data['active']['phase'],'intent');self.assertFalse(c.shutdown())
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
    def test_strict_shutdown_refuses_foreign_ownership_and_discontinuity(self):
        for who in ('target','coordinator'):
            for kind in ('foreign','aba','gap'):
                with self.subTest(who=who,kind=kind):
                    self.doCleanups();c=self.ready(True);uri=self.model.peers[who].uri
                    if kind=='gap':self.peers.events[who].subscriptions[0].lost=True
                    else:
                        self.model.update(who,uri='http://foreign.invalid/source');self.model.emit_all()
                        if kind=='aba':self.model.update(who,uri=uri);self.model.emit_all()
                    self.assertFalse(c.shutdown());self.assertEqual(self.writes(),[]);self.assertFalse(self.authorized())
    def test_coordinator_resume_end_volume_and_mute_during_detach_stop_next_write(self):
        for fields in ({'transport':'PLAYING'},{'uri':'','transport':'STOPPED'},{'volume':77},{'mute':True}):
            with self.subTest(fields=fields):
                self.doCleanups();c=self.ready();self.local_end()
                def after(action):
                    if action=='BecomeCoordinatorOfStandaloneGroup':self.model.update('coordinator',**fields);self.model.emit_all()
                self.model.post_write=after;self.assertEqual(c.tick(),'recovery')
                self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup']);self.assertEqual(self.store.data['active']['phase'],'intent')
                c.tick();c.shutdown();self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
    def test_coordinator_resume_and_pause_away_back_during_detach_refuses_source_revision(self):
        c=self.ready();self.local_end()
        def after(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':
                for state in ('PLAYING','PAUSED_PLAYBACK'):self.model.update('coordinator',transport=state);self.model.emit_all()
        self.model.post_write=after;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
    def test_foreign_then_allowed_uri_after_detach_is_not_a_direct_transition(self):
        for who in ('target','coordinator'):
            with self.subTest(who=who):
                self.doCleanups();c=self.ready();self.local_end()
                def after(action):
                    if action=='BecomeCoordinatorOfStandaloneGroup':
                        uri=self.model.peers[who].uri
                        for value in ('http://foreign.invalid/source',uri):self.model.update(who,uri=value);self.model.emit_all()
                self.model.post_write=after;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
    def test_sequence_gap_after_each_write_is_not_replayed(self):
        actions=('BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI')
        for i,action in enumerate(actions):
            with self.subTest(action=action):
                self.doCleanups();c=self.ready();self.local_end()
                def after(name):
                    if name==action:self.peers.events['coordinator'].subscriptions[0].lost=True
                self.model.post_write=after;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),list(actions[:i+1]))
                c.tick();c.shutdown();self.assertEqual(self.writes(),list(actions[:i+1]))
    def test_local_end_lost_after_each_write_stops_without_replay(self):
        actions=('BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI')
        for i,action in enumerate(actions):
            with self.subTest(action=action):
                self.doCleanups();c=self.ready();self.local_end()
                def after(name):
                    if name==action:self.session=replace(self.session,state='present',media=True,end_event_confirmed=False)
                self.model.post_write=after;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),list(actions[:i+1]))
    def test_failed_or_ambiguous_stop_cannot_be_counted_as_idle(self):
        c=self.ready();self.local_end();self.model.keep_playing_after_stop=True
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop']);self.assertFalse(self.model.peers['target'].idle())
    def test_lost_detach_ack_retains_intent_for_explicit_recovery(self):
        c=self.ready();self.local_end()
        def after(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':raise BackendError('http_deadline')
        self.model.post_write=after;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
        fresh=Controller(self.peers,lambda:self.session,self.wave,self.scope,self.store)
        self.assertEqual(fresh.tick(),'recovery');self.assertFalse(fresh.shutdown());self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup'])
    def test_local_change_after_context_read_cannot_emit_false_end_or_volume(self):
        c=self.ready();self.local_end();original=c._read
        def read(*args,**kwargs):
            facts=original(*args,**kwargs)
            if getattr(kwargs.get('end'),'kind',None)=='paused_release':
                self.session=replace(self.session,state='present',media=True,end_event_confirmed=False,
                                     volume_revision=1,volume_milli=-15000)
            return facts
        c._read=read;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertFalse(any(r['event']=='owned_native_hold_finished' for r in self.rows()))
    def test_new_local_begin_after_intent_prevents_send(self):
        c=self.ready();self.local_end();original=self.store.event
        def event(name,**fields):
            result=original(name,**fields)
            if name=='intent':self.session=replace(self.session,state='present',generation='new',media=True,end_event_confirmed=False)
            return result
        self.store.event=event;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertEqual(self.store.data['active']['phase'],'intent')
    def test_foreign_coordinator_after_intent_prevents_send(self):
        c=self.ready();self.local_end();original=self.store.event
        def event(name,**fields):
            result=original(name,**fields)
            if name=='intent':self.model.update('coordinator',uri='http://foreign.invalid/source');self.model.emit_all()
            return result
        self.store.event=event;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertEqual(self.store.data['active']['phase'],'intent')
    def test_restarted_subscription_cannot_adopt_paused_release(self):
        from gena_runtime import Events
        c=self.ready();self.local_end();old=self.peers.events['coordinator']
        fresh=Events(old.config,old.ports,old.bind,0,http=self.model);self.addCleanup(fresh.close)
        self.peers.events['coordinator']=self.model.events['coordinator']=fresh
        for sub in fresh.subscriptions:fresh.subscribe(sub)
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_marker_or_intent_audit_failure_occurs_before_any_speaker_write(self):
        for fail in ('owned_paused_release_authorized','intent'):
            with self.subTest(fail=fail):
                self.doCleanups();c=self.ready();self.local_end();original=self.store.event
                def event(name,**fields):
                    if name==fail:raise StateError('journal_full_recovery_required')
                    return original(name,**fields)
                self.store.event=event;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_local_end_and_coordinator_uri_end_retain_existing_native_end_context(self):
        c=self.ready();self.local_end();self.end();self.assertEqual(c.tick(),'idle')
        self.assertFalse(self.authorized());self.assertTrue(any(r['event']=='owned_native_end_cleanup' for r in self.rows()))

class PositiveEndProjection(unittest.TestCase):
    def test_real_event_five_only_with_live_lineage_and_cleared_presence(self):
        r=R.SessionTests();r.setUp();self.addCleanup(r.doCleanups)
        self.assertFalse(r.o.evidence().end_event_confirmed)
        r.record(4);self.assertFalse(r.o.evidence().end_event_confirmed)
        r.event(1,5,15);self.assertFalse(r.o.evidence().end_event_confirmed)
        r.event(1,5,14);self.assertTrue(r.o.evidence().end_event_confirmed)
        r.now+=3;self.assertFalse(r.o.evidence().end_event_confirmed)
        r.o.lineage_checked();self.assertTrue(r.o.evidence().end_event_confirmed)
        r.o.invalidate();self.assertFalse(r.o.evidence().end_event_confirmed)

if __name__=='__main__':unittest.main()
