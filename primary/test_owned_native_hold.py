"""Actual HOLD shape on real SOAP/GENA models; no household or network access."""
from dataclasses import replace
from unittest.mock import Mock,patch
import copy,json,unittest
from controller import Controller,packed
from failure_diagnostics import diagnostic_fields
from gena_runtime import Events,EventError
from runtime_state import StateError
from sonos_api import BackendError
from peer_runtime import Advertisement
import test_owned_native_end as N
import test_wire as W
from test_primary import T,C,NATIVE

class NativeHold(unittest.TestCase):
    fixture=N.OwnedNativeEndTests.fixture
    end=N.OwnedNativeEndTests.end
    rows=N.OwnedNativeEndTests.rows
    writes=N.OwnedNativeEndTests.writes
    fake_clock=N.OwnedNativeEndTests.fake_clock
    def hold(self):
        self.model.end_mismatch=True;self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
        self.session=replace(self.session,state='present',media=False)
    def resume(self):
        self.model.end_mismatch=False;self.model.update('coordinator',transport='PLAYING');self.model.emit_all()
        self.session=replace(self.session,state='present',media=True)
    def unchanged(self,checkpoint):
        self.assertEqual(self.writes(),[]);self.assertEqual(self.store.data,checkpoint)
        self.assertEqual(self.model.peers['target'].members,frozenset((T,C)))
        self.assertEqual(self.model.peers['coordinator'].members,frozenset((T,C)))
        self.assertEqual(self.model.peers['coordinator'].uri,NATIVE)
    def test_observed_present_no_media_paused_native_state_holds_without_write_or_rebase(self):
        c=self.fixture();checkpoint=copy.deepcopy(self.store.data);expected=packed(c.expected);self.hold()
        for _ in range(5):self.assertEqual(c.tick(),'both');self.unchanged(checkpoint)
        self.assertFalse(c.failed);self.assertEqual(packed(c.expected),expected);self.assertTrue(c.native_hold)
        events=[r for r in self.rows() if r['event']=='owned_native_hold'];self.assertEqual(len(events),1)
        self.assertEqual({k:events[0][k] for k in ('speaker_writes','checkpoint_unchanged','peer_ownership_unchanged','local_generation_matches')},
                         {'speaker_writes':False,'checkpoint_unchanged':True,'peer_ownership_unchanged':True,'local_generation_matches':True})
        self.assertFalse(any(r['event']=='owned_native_end_cleanup' for r in self.rows()))
    def test_resume_reenters_strict_reads_without_play_or_group_write(self):
        c=self.fixture();checkpoint=copy.deepcopy(self.store.data);self.hold();self.assertEqual(c.tick(),'both')
        self.resume();self.assertEqual(c.tick(),'both');self.unchanged(checkpoint);self.assertFalse(c.native_hold)
        self.assertEqual(self.peers.read('target').peer.transport,'PLAYING')
        exited=[r for r in self.rows() if r['event']=='owned_native_hold_finished'];self.assertEqual(len(exited),1)
        self.assertEqual(exited[0]['observation'],'strict_read')
    def test_second_hold_after_resume_gets_one_new_entry_marker(self):
        c=self.fixture()
        for _ in range(2):
            self.hold();self.assertEqual(c.tick(),'both');self.assertEqual(c.tick(),'both')
            self.resume();self.assertEqual(c.tick(),'both')
        self.assertEqual(sum(r['event']=='owned_native_hold' for r in self.rows()),2);self.assertEqual(self.writes(),[])
    def test_held_volume_update_waits_for_normal_fresh_resume_proof(self):
        c=self.fixture();self.hold();self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        self.assertEqual(c.tick(),'both');self.assertEqual(self.writes(),[]);self.assertEqual(c.volume_revision,0)
        self.resume();self.assertEqual(c.tick(),'both');self.assertEqual(self.writes(),['SetVolume'])
        self.assertEqual(self.model.peers['target'].volume,50);self.assertEqual(self.model.peers['coordinator'].volume,31)
    def test_hold_never_supplies_a_fact_to_write_guards(self):
        c=self.fixture();self.hold();self.assertEqual(c.tick(),'both')
        self.assertIs(self.peers.observe_owned_native_hold(c.expected),True)
        with self.assertRaises(BackendError):self.peers.read('target')
        with self.assertRaises(BackendError):c._write('SetVolume',lambda _:True,group=True,DesiredVolume=50)
        self.assertEqual(self.writes(),[]);self.assertEqual(self.store.data['active']['phase'],'verified')
    def test_hold_then_direct_coordinator_uri_end_cleans_target_only(self):
        c=self.fixture();self.hold();self.assertEqual(c.tick(),'both');self.end()
        self.assertEqual(c.tick(),'idle');self.assertTrue(self.model.peers['target'].idle());self.assertIsNone(self.store.data['active'])
        self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertFalse(any(w=='coordinator' and not a.startswith('Get') for w,a,_ in self.model.all_calls))
    def test_explicit_local_end_with_retained_paused_uri_is_not_misclassified_as_hold(self):
        c=self.fixture();self.hold();self.assertEqual(c.tick(),'both');self.session=replace(self.session,state='ended')
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertEqual(self.rows()[-1]['controller_failure_reason'],'owned_native_end_unproven')
        self.assertEqual(self.rows()[-1]['end_local_state'],'ended');self.assertEqual(self.model.peers['coordinator'].uri,NATIVE)
    def test_resume_then_coherent_local_deselection_retains_existing_cleanup(self):
        c=self.fixture();self.hold();self.assertEqual(c.tick(),'both');self.resume();self.assertEqual(c.tick(),'both')
        self.session=replace(self.session,state='ended',media=False);self.assertEqual(c.tick(),'idle')
        self.assertTrue(self.model.peers['target'].idle());self.assertEqual(self.model.peers['coordinator'].uri,NATIVE)
        self.assertEqual(self.model.peers['coordinator'].transport,'PLAYING')
    def test_only_present_same_generation_no_media_qualifies(self):
        for fields in ({'state':'unproven'},{'state':'lost'},{'state':'ended'},{'generation':'different'},{'media':True}):
            with self.subTest(changed=tuple(fields)):
                self.doCleanups();c=self.fixture();self.hold();self.session=replace(self.session,**fields)
                self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
                self.assertFalse(any(r['event']=='owned_native_hold' for r in self.rows()))
    def test_unobserved_retained_stopped_or_playing_coordinator_does_not_select_hold(self):
        for transport in ('STOPPED','PLAYING','TRANSITIONING'):
            with self.subTest(transport=transport):
                self.doCleanups();c=self.fixture();self.hold();self.model.update('coordinator',transport=transport);self.model.emit_all()
                self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
                self.assertFalse(any(r['event']=='owned_native_hold' for r in self.rows()))
    def test_foreign_target_or_coordinator_during_hold_is_preserved(self):
        for who in ('target','coordinator'):
            with self.subTest(who=who):
                self.doCleanups();c=self.fixture();self.hold();self.assertEqual(c.tick(),'both')
                self.model.update(who,uri='http://foreign.invalid/source');self.model.emit_all()
                self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
                self.assertEqual(self.model.peers[who].uri,'http://foreign.invalid/source')
    def test_source_away_back_during_hold_is_not_adopted(self):
        for who,original in (('target','x-rincon:'+C),('coordinator',NATIVE)):
            with self.subTest(who=who):
                self.doCleanups();c=self.fixture();self.hold();self.assertEqual(c.tick(),'both')
                for uri in ('http://foreign.invalid/source',original):self.model.update(who,uri=uri);self.model.emit_all()
                self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_topology_away_back_during_hold_is_not_adopted(self):
        c=self.fixture();self.hold();self.assertEqual(c.tick(),'both')
        self.model.update('target',coordinator=T,members=frozenset((T,)))
        self.model.update('coordinator',members=frozenset((C,)));self.model.emit_all()
        self.model.update('target',coordinator=C,members=frozenset((T,C)))
        self.model.update('coordinator',members=frozenset((T,C)));self.model.emit_all()
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_boot_or_uuid_change_during_hold_refuses(self):
        for who in ('target','coordinator'):
            for change in ('boot','uuid'):
                with self.subTest(who=who,change=change):
                    self.doCleanups();c=self.fixture();self.hold();self.assertEqual(c.tick(),'both')
                    if change=='boot':self.model.update(who,boot=99);self.model.emit_all()
                    else:self.model.update(who,uuid='RINCON_00000000000301400')
                    self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_gap_or_expired_subscription_during_hold_refuses(self):
        for who in ('target','coordinator'):
            for change in ('lost','expired'):
                with self.subTest(who=who,change=change):
                    self.doCleanups();c=self.fixture();self.hold();self.assertEqual(c.tick(),'both')
                    sub=self.peers.events[who].subscriptions[0]
                    if change=='lost':sub.lost=True
                    else:sub.deadline=0
                    self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_restarted_subscription_cannot_rebase_hold(self):
        c=self.fixture();self.hold();self.assertEqual(c.tick(),'both');old=self.peers.events['coordinator']
        fresh=Events(old.config,old.ports,old.bind,0,http=self.model);self.addCleanup(fresh.close)
        self.peers.events['coordinator']=self.model.events['coordinator']=fresh
        for sub in fresh.subscriptions:fresh.subscribe(sub)
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_expired_fact_in_hold_proof_refuses(self):
        c=self.fixture();advance=self.fake_clock();self.hold();original=self.peers._observe
        def slow(who):
            fact=original(who)
            if who=='coordinator':advance(2.001)
            return fact
        self.peers._observe=slow;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertEqual(self.rows()[-1]['controller_failure_reason'],'peer_evidence_expired')
    def test_local_end_arriving_during_proof_cannot_enter_hold(self):
        c=self.fixture();self.hold();original=self.peers.observe_owned_native_hold
        def changed(expected):
            result=original(expected);self.session=replace(self.session,state='ended');return result
        self.peers.observe_owned_native_hold=changed
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertFalse(any(r['event']=='owned_native_hold' for r in self.rows()))
    def test_changed_checkpoint_during_proof_cannot_enter_hold(self):
        c=self.fixture();self.hold();original=self.peers.observe_owned_native_hold
        def changed(expected):
            result=original(expected);self.store.data['active']['action']='Stop';self.store.persist();return result
        self.peers.observe_owned_native_hold=changed
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertEqual(self.rows()[-1]['controller_failure_reason'],'owned_native_hold_checkpoint')
    def test_active_intent_and_restart_never_resume_hold(self):
        c=self.fixture();self.hold();self.store.data['active']['phase']='intent';self.store.persist()
        self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        new=Controller(self.peers,lambda:self.session,self.wave,self.scope,self.store)
        self.assertEqual(new.tick(),'recovery');self.assertFalse(new.shutdown());self.assertEqual(self.writes(),[])
    def test_shutdown_cannot_turn_readonly_hold_into_cleanup_authority(self):
        c=self.fixture();self.hold();self.assertEqual(c.tick(),'both');self.assertFalse(c.shutdown())
        self.assertEqual(self.writes(),[]);self.assertIsNotNone(self.store.data['active'])
    def test_journal_failure_on_positive_hold_marker_refuses_without_write(self):
        c=self.fixture();self.hold();original=self.store.event
        def event(name,**fields):
            if name=='owned_native_hold':raise StateError('journal_full_recovery_required')
            return original(name,**fields)
        self.store.event=event;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
        self.assertEqual(self.rows()[-1]['controller_exception_class'],'StateError')
    def test_source_event_or_gap_after_snapshot_is_still_refused(self):
        for change in ('source','gap'):
            with self.subTest(change=change):
                self.doCleanups();c=self.fixture();self.hold();events=self.peers.events['target'];original=events.direct_transition_matches
                def late(who,before,after):
                    if change=='gap':events.subscriptions[0].lost=True
                    else:events.states['target']['source_revision']='different-private-revision'
                    return original(who,before,after)
                events.direct_transition_matches=late
                self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_nonmatching_target_disagreement_remains_strict(self):
        for transport in ('PAUSED_PLAYBACK','TRANSITIONING'):
            with self.subTest(transport=transport):
                self.doCleanups();c=self.fixture();self.hold();self.model.event_transport=transport;self.model.emit_all()
                self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
    def test_exact_initial_group_recheck_race_enters_hold_once(self):
        c=self.fixture();original=self.peers.read;triggered=[False]
        self.session=replace(self.session,media=False)
        def pause_after_read(who):
            fact=original(who)
            if who=='target' and not triggered[0]:triggered[0]=True;self.hold()
            return fact
        self.peers.read=pause_after_read;self.assertEqual(c.tick(),'both');self.assertEqual(self.writes(),[])
        self.assertEqual(sum(r['event']=='owned_native_hold' for r in self.rows()),1)

if __name__=='__main__':unittest.main()
