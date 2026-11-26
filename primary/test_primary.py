"""Primary lifecycle on a deterministic speaker model; no LAN or device access."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import ast,json,tempfile,time,unittest
from controller import Controller,packed
from sonos_api import Endpoint,Peer,Fact,Revision,Wire,BackendError
from session import Session
from runtime_state import ProductScope,Store
from wav_http import Wave
import recovery
T='RINCON_00000000000101400';C='RINCON_00000000000201400';G='12345678-1234-1234-1234-123456789abc'
NATIVE=f'x-sonos-vli:{C}:1,airplay:{G.replace("-","")}'

class Model:
    def __init__(self,clock):
        self.clock=clock;self.calls=[];self.online=True;self.same=True;self.continuous=True;self.ad_after=lambda:None
        self.peers={'target':Peer(T,'','STOPPED',T,frozenset((T,)),17,23,False),
                    'coordinator':Peer(C,NATIVE,'PLAYING',C,frozenset((C,)),21,31,False)}
        self.uri_rev={'target':1,'coordinator':1};self.top_rev=dict(self.uri_rev);self.source_rev=dict(self.uri_rev)
        self.after=lambda *_:None;self.ambiguous=None;self.reads=[];self.track_position=120
    def update(self,who,**values):
        before=self.peers[who];after=replace(before,**values);self.peers[who]=after
        if before.uri!=after.uri:self.uri_rev[who]+=1
        if (before.uri,before.transport)!=(after.uri,after.transport):self.source_rev[who]+=1
        if (before.coordinator,before.members,before.boot)!=(after.coordinator,after.members,after.boot):self.top_rev[who]+=1
    def read(self,who):
        self.reads.append(who)
        if who=='coordinator' and not self.online:raise BackendError('offline')
        p=self.peers[who];at=self.clock()
        r=Revision(str(self.top_rev[who]),str(self.source_rev[who]),p.uri,p.coordinator,p.members,at,self.continuous,str(self.uri_rev[who]))
        return Fact(p,r,at)
    def recheck(self,who,f):
        if not self.continuous or not 0<=self.clock()-f.observed<=2 or f.ownership()!=self.read(who).ownership():raise BackendError('changed')
        return f
    def same_group(self,s,f):return self.same and s.media and s.state=='present' and s.group==G and f.peer.uri==NATIVE and f.peer.transport in ('PLAYING','PAUSED_PLAYBACK')
    def write(self,action,**v):
        self.calls.append((action,v));t=self.peers['target'];r=self.peers['coordinator']
        if action=='SetAVTransportURI':
            uri=v['CurrentURI']
            if uri=='x-rincon:'+C:
                self.update('target',uri=uri,transport=r.transport,coordinator=C,members=frozenset((T,C)))
                self.update('coordinator',members=frozenset((T,C)))
            else:self.update('target',uri=uri,transport='STOPPED')
        elif action=='Play':self.update('target',transport='PLAYING')
        elif action=='Stop':self.update('target',transport='STOPPED')
        elif action=='BecomeCoordinatorOfStandaloneGroup':
            self.update('target',uri=r.uri,transport=r.transport,coordinator=T,members=frozenset((T,)))
            self.update('coordinator',members=frozenset((C,)))
        elif action=='SetVolume':self.update('target',volume=v['DesiredVolume'])
        else:raise AssertionError(action)
        self.track_position+=.1;self.after(action)
        if self.ambiguous==action:self.ambiguous=None;raise BackendError('lost_ack')

class Primary(unittest.TestCase):
    def fixture(self):
        self.doCleanups();self.now=100.;self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name);self.scope=ProductScope(root/'scope',(T,C));self.addCleanup(self.scope.close)
        self.store=Store(root/'state','synthetic');self.model=Model(lambda:self.now)
        self.session=Session('generation1','present',True,G,0,0,self.now,1)
        self.wave=Wave('127.0.0.1',19091,'127.0.0.2',present=lambda:self.session.state=='present');self.addCleanup(self.wave.close)
        self.ctrl=Controller(self.model,lambda:self.session,self.wave,self.scope,self.store,clock=lambda:self.now,sleep=self.advance)
        return self.ctrl
    def advance(self,n):self.now+=n
    def solo(self):
        c=self.fixture();self.model.online=False;self.assertEqual(c.tick(),'solo');return c
    def both(self):
        c=self.fixture();self.assertEqual(c.tick(),'both');return c
    def test_solo_with_unavailable_or_unconfigured_coordinator(self):
        c=self.solo();self.assertTrue(self.model.peers['target'].uri.endswith('.wav'))
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI','Play'])
        self.assertTrue(c.shutdown());self.assertTrue(self.model.peers['target'].idle())
    def test_explicit_matching_group_joins_target_only(self):
        c=self.both();self.assertEqual(self.model.peers['target'].uri,'x-rincon:'+C)
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI'])
        self.assertEqual(self.model.peers['coordinator'].uri,NATIVE)
    def test_solo_promotes_when_same_airplay_session_is_selected(self):
        c=self.solo();self.model.online=True;self.assertEqual(c.tick(),'both')
        self.assertIsNone(self.wave.path);self.assertEqual(self.model.peers['coordinator'].uri,NATIVE)
    def test_mid_song_office_deselection_detaches_and_quiets_only_target(self):
        c=self.both();before=self.model.peers['coordinator'];position=self.model.track_position
        self.session=replace(self.session,state='ended',media=False)
        self.assertEqual(c.tick(),'idle');self.assertTrue(self.model.peers['target'].idle())
        self.assertEqual(self.model.peers['coordinator'],replace(before,members=frozenset((C,))))
        self.assertGreater(self.model.track_position,position)
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI','BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
        self.assertIsNone(self.store.data['active'])
    def test_pause_flush_and_quiet_hold_do_not_detach_or_restart_native_track(self):
        c=self.both();count=len(self.model.calls)
        for state,transport in (('present','PAUSED_PLAYBACK'),('unproven','PAUSED_PLAYBACK'),('present','PLAYING')):
            self.session=replace(self.session,state=state,media=False);self.model.update('coordinator',transport=transport)
            self.advance(30);self.assertEqual(c.tick(),'both')
        self.assertEqual(len(self.model.calls),count)
    def test_unmatched_or_unknown_group_stays_solo(self):
        for group,same in ((None,True),(G,False),('22222222-2222-2222-2222-222222222222',True)):
            with self.subTest(group=group):
                c=self.fixture();self.session=replace(self.session,group=group);self.model.same=same
                self.assertEqual(c.tick(),'solo');self.assertTrue(self.model.peers['coordinator'].standalone())
    def test_native_end_releases_target_then_separate_solo_activation(self):
        c=self.both();self.model.update('coordinator',uri='',transport='STOPPED')
        self.assertEqual(c.tick(),'idle');self.assertEqual(c.tick(),'solo')
        self.assertTrue(self.model.peers['coordinator'].idle())
    def test_foreign_target_or_coordinator_source_is_never_overwritten(self):
        for who in ('target','coordinator'):
            with self.subTest(who=who):
                c=self.both();self.model.update(who,uri='http://foreign.invalid/track');count=len(self.model.calls)
                self.assertEqual(c.tick(),'recovery');self.assertEqual(len(self.model.calls),count)
                self.assertEqual(c.tick(),'recovery');self.assertEqual(len(self.model.calls),count)
    def test_away_and_back_source_revision_invalidates_ownership(self):
        c=self.both();self.model.update('coordinator',uri='http://foreign.invalid/track');self.model.update('coordinator',uri=NATIVE)
        count=len(self.model.calls);self.assertEqual(c.tick(),'recovery');self.assertEqual(len(self.model.calls),count)
    def test_preexisting_native_group_is_never_claimed_or_detached(self):
        c=self.fixture();self.model.update('target',uri='x-rincon:'+C,coordinator=C,members=frozenset((T,C)),transport='PLAYING')
        self.model.update('coordinator',members=frozenset((T,C)))
        self.assertEqual(c.tick(),'waiting');self.session=replace(self.session,state='ended',media=False)
        self.assertEqual(c.tick(),'idle');self.assertEqual(self.model.calls,[])
    def test_target_only_volume_deduplication_and_generation_binding(self):
        c=self.both();before=self.model.peers['coordinator'];self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        self.assertEqual(c.tick(),'both');self.assertEqual(self.model.peers['target'].volume,50)
        self.assertEqual(self.model.peers['coordinator'],before);count=len(self.model.calls);c.tick();self.assertEqual(len(self.model.calls),count)
        self.session=replace(self.session,media=False,volume_revision=2,volume_milli=-144000);c.tick();self.assertEqual(len(self.model.calls),count)
        self.session=replace(self.session,media=True);c.tick();self.assertEqual(self.model.peers['target'].volume,0)
    def test_superseded_volume_intent_preserves_group_and_applies_only_new_revision(self):
        c=self.both()
        checkpoint=packed(self.store.data['active'])
        original=self.store.event
        self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        def event(name,**data):
            original(name,**data)
            if name=='intent' and data['action']=='SetVolume' and self.session.volume_revision==1:
                self.session=replace(self.session,volume_revision=2,volume_milli=-18000)
        self.store.event=event
        self.assertEqual(c.tick(),'both')
        self.assertEqual(self.store.data['active'],checkpoint)
        self.assertEqual(c.volume_revision,0)
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI'])
        self.assertEqual(c.tick(),'both')
        self.assertEqual(self.model.calls[-1],('SetVolume',{'DesiredVolume':40}))
        self.assertEqual(c.volume_revision,2)
    def test_volume_deferral_does_not_clear_a_changed_native_source(self):
        c=self.both()
        original=self.store.event
        self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        def event(name,**data):
            original(name,**data)
            if name=='intent' and data['action']=='SetVolume':
                self.model.update('coordinator',uri='http://foreign.invalid/track')
        self.store.event=event
        self.assertEqual(c.tick(),'recovery')
        self.assertEqual(self.store.data['active']['phase'],'intent')
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI'])
    def test_changed_physical_gain_supersedes_unsent_volume_without_retry(self):
        c=self.both()
        original=self.store.event
        self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        def event(name,**data):
            original(name,**data)
            if name=='intent' and data['action']=='SetVolume' and self.session.volume_revision==1:
                self.model.update('target',volume=44)
        self.store.event=event
        self.assertEqual(c.tick(),'both')
        self.assertEqual(c.tick(),'both')
        self.assertEqual(self.model.peers['target'].volume,44)
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI'])
        self.assertEqual(self.store.data['active']['phase'],'verified')
        self.assertEqual(self.store.data['active']['volume_revision'],1)
        self.session=replace(self.session,volume_revision=2,volume_milli=-18000)
        self.assertEqual(c.tick(),'both')
        self.assertEqual(self.model.calls[-1],('SetVolume',{'DesiredVolume':40}))
    def test_acknowledged_native_volume_with_concurrent_gains_keeps_verified_group(self):
        c=self.both()
        self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        def after(action):
            if action=='SetVolume':
                self.model.update('target',volume=32)
                self.model.update('coordinator',volume=38)
        self.model.after=after
        self.assertEqual(c.tick(),'both')
        self.assertEqual(c.tick(),'both')
        self.assertEqual(self.model.calls,[('SetAVTransportURI',{'CurrentURI':'x-rincon:'+C,'CurrentURIMetaData':''}),
                                           ('SetVolume',{'DesiredVolume':50})])
        self.assertEqual(self.store.data['active']['phase'],'verified')
        self.assertEqual(self.store.data['active']['checkpoint']['target']['peer']['volume'],32)
        self.assertEqual(self.store.data['active']['checkpoint']['coordinator']['peer']['volume'],38)
        rows=[json.loads(line) for line in self.store.audit.read_text().splitlines()]
        event=next(row for row in rows if row['event']=='volume_settled_with_concurrent_change')
        self.assertFalse(event['requested_gain_observed'])
    def test_acknowledged_native_volume_cannot_adopt_new_coordinator_source(self):
        c=self.both()
        self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        def after(action):
            if action=='SetVolume':
                self.model.update('coordinator',volume=38,uri='http://foreign.invalid/track')
        self.model.after=after
        self.assertEqual(c.tick(),'recovery')
        self.assertEqual(self.store.data['active']['phase'],'intent')
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI','SetVolume'])
    def test_volume_defers_before_intent_if_new_input_wins_the_read(self):
        c=self.both()
        checkpoint=packed(self.store.data['active'])
        self.session=replace(self.session,volume_revision=1,volume_milli=-15000)
        original=c._read
        def read(*args,**kwargs):
            result=original(*args,**kwargs)
            self.session=replace(self.session,volume_revision=2,volume_milli=-18000)
            return result
        c._read=read
        c._volume()
        self.assertEqual(self.store.data['active'],checkpoint)
        self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI'])
        self.assertEqual(c.volume_revision,0)
    def test_manual_volume_survives_solo_to_native_promotion_without_new_airplay_event(self):
        c=self.solo();self.session=replace(self.session,volume_revision=1,volume_milli=-12000);c.tick()
        self.model.update('target',volume=33);before=sum(a=='SetVolume' for a,_ in self.model.calls)
        self.model.online=True;self.assertEqual(c.tick(),'both');self.assertEqual(self.model.peers['target'].volume,33)
        self.assertEqual(sum(a=='SetVolume' for a,_ in self.model.calls),before)
    def test_source_change_after_durable_join_intent_blocks_the_write(self):
        c=self.fixture();original=self.store.event
        def event(name,**data):
            original(name,**data)
            if name=='intent':self.model.update('coordinator',uri='http://foreign.invalid/track')
        self.store.event=event;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.model.calls,[])
    def test_source_change_after_detach_prevents_following_stop_clear(self):
        c=self.both();self.session=replace(self.session,state='ended',media=False)
        def after(action):
            if action=='BecomeCoordinatorOfStandaloneGroup':self.model.update('target',uri='http://foreign.invalid/track')
        self.model.after=after
        self.assertEqual(c.tick(),'recovery');self.assertEqual([a for a,_ in self.model.calls],['SetAVTransportURI','BecomeCoordinatorOfStandaloneGroup'])
    def test_native_join_waits_through_bounded_transition_without_rewriting(self):
        c=self.fixture();original=self.model.read;pending=[False,0]
        def after(action):
            if action=='SetAVTransportURI':self.model.update('target',transport='TRANSITIONING');pending[0]=True
        def read(who):
            if pending[0] and who=='target':
                pending[1]+=1
                if pending[1]>=3:self.model.update('target',transport='PLAYING');pending[0]=False
            return original(who)
        self.model.after=after;self.model.read=read
        self.assertEqual(c.tick(),'both');self.assertEqual(len(self.model.calls),1)
    def test_unexpected_native_join_readback_records_changed_fields_only(self):
        c=self.fixture()
        self.model.after=lambda action:self.model.update('coordinator',mute=True)
        self.assertEqual(c.tick(),'recovery')
        self.assertEqual([action for action,_ in self.model.calls],['SetAVTransportURI'])
        rows=[json.loads(line) for line in self.store.audit.read_text().splitlines()]
        refused=next(row for row in rows if row['event']=='post_ack_readback_refused')
        self.assertEqual(refused['changes']['coordinator']['peer'],['members','mute'])
        self.assertNotIn('RINCON',json.dumps(refused))
    def test_guard_refusal_records_actual_attempt_before_any_intent(self):
        c=self.both()
        count=len(self.model.calls)
        with self.assertRaisesRegex(BackendError,'ownership_or_selection_changed'):
            c._write('Stop',lambda snapshot:False,group=True)
        self.assertEqual(len(self.model.calls),count)
        row=json.loads(self.store.audit.read_text().splitlines()[-1])
        self.assertEqual(row['event'],'pre_intent_refused')
        self.assertEqual(row['action'],'Stop')
        self.assertTrue(row['ownership_matches'])
        self.assertFalse(row['speaker_command_sent'])
    def test_postwrite_gena_lag_is_bounded_read_only_settling(self):
        c=self.fixture();original=self.model.recheck;pending=[0]
        self.model.after=lambda action:pending.__setitem__(0,2)
        def recheck(who,f):
            if pending[0]:pending[0]-=1;raise BackendError('peer_evidence_changed')
            return original(who,f)
        self.model.recheck=recheck
        self.assertEqual(c.tick(),'both');self.assertEqual(len(self.model.calls),1)
    def test_stalled_native_transition_times_out_without_replay(self):
        c=self.fixture();self.model.after=lambda action:self.model.update('target',transport='TRANSITIONING')
        self.assertEqual(c.tick(),'recovery');self.assertEqual(len(self.model.calls),1);self.assertLess(self.now,110)
    def test_lost_ack_is_not_replayed(self):
        c=self.fixture();self.model.ambiguous='SetAVTransportURI'
        self.assertEqual(c.tick(),'recovery');count=len(self.model.calls);c.tick();c.shutdown();self.assertEqual(len(self.model.calls),count)
        self.assertIsNone(recovery.proposal(c)['fingerprint'])
    def test_scope_boot_and_gena_losses_prevent_cleanup(self):
        for kind in ('scope','boot','gena'):
            with self.subTest(kind=kind):
                c=self.both();count=len(self.model.calls)
                if kind=='scope':self.scope.close()
                elif kind=='boot':self.model.update('target',boot=18)
                else:self.model.continuous=False
                self.session=replace(self.session,state='ended',media=False);self.assertEqual(c.tick(),'recovery');self.assertEqual(len(self.model.calls),count)
    def test_service_shutdown_runs_target_cleanup_and_keeps_native_source(self):
        c=self.both();self.assertTrue(c.shutdown());self.assertTrue(self.model.peers['target'].idle());self.assertEqual(self.model.peers['coordinator'].uri,NATIVE)
    def test_standalone_skip_reconnect_requires_owned_stopped_wav(self):
        c=self.solo();self.model.update('target',transport='STOPPED');count=len(self.model.calls)
        self.assertEqual(c.tick(),'solo');self.assertEqual(self.model.calls[count:],[('Play',{'Speed':'1'})])
        self.model.update('target',uri='http://foreign.invalid/track',transport='STOPPED');count=len(self.model.calls)
        self.assertEqual(c.tick(),'recovery');self.assertEqual(len(self.model.calls),count)
    def test_restart_requires_explicit_fresh_checkpoint_recovery(self):
        c=self.both();other=Controller(self.model,lambda:self.session,self.wave,self.scope,self.store,clock=lambda:self.now,sleep=self.advance)
        count=len(self.model.calls);self.assertEqual(other.tick(),'recovery');self.assertEqual(len(self.model.calls),count)
        p=recovery.proposal(other);self.assertEqual(p['status'],'reviewable_target_cleanup')
        self.assertEqual(recovery.apply(other,p['fingerprint'])['status'],'recovered');self.assertTrue(self.model.peers['target'].idle())
    def test_recovery_after_verified_detach_does_not_detach_again(self):
        c=self.both();c._write('BecomeCoordinatorOfStandaloneGroup',lambda s:c._owned(s),group=True)
        other=Controller(self.model,lambda:self.session,self.wave,self.scope,self.store,clock=lambda:self.now,sleep=self.advance)
        p=recovery.proposal(other);self.assertEqual(recovery.apply(other,p['fingerprint'])['status'],'recovered')
        self.assertEqual(sum(a=='BecomeCoordinatorOfStandaloneGroup' for a,_ in self.model.calls),1)
        self.assertTrue(self.model.peers['target'].idle())
    def test_changed_recovery_preview_refuses_cleanup(self):
        c=self.both();p=recovery.proposal(c);self.model.update('target',volume=62);count=len(self.model.calls)
        with self.assertRaises(BackendError):recovery.apply(c,p['fingerprint'])
        self.assertEqual(len(self.model.calls),count)
    def test_coordinator_writes_are_structurally_rejected(self):
        wire=Wire(Endpoint('192.168.50.20',T),Endpoint('192.168.50.30',C))
        for action in ('Stop','Play','SetAVTransportURI','SetVolume','BecomeCoordinatorOfStandaloneGroup','AddMember','ChangeTransportSettings'):
            with self.subTest(action=action),self.assertRaisesRegex(BackendError,'target_only_write'):wire.write('coordinator',action)
        for action in ('AddMember','ChangeTransportSettings','SetMute'):
            with self.assertRaisesRegex(BackendError,'target_only_write'):wire.write('target',action)
    def test_runtime_has_no_experimental_imports_or_source_code(self):
        banned={'airplay_native','native_pcm','native_worker','silence_http','continuous_flac','provisional_policy','route_ack','roam_session','local_workers','product_workers','source_markers'}
        for path in Path('.').glob('*.py'):
            if path.name.startswith('test_'):continue
            tree=ast.parse(path.read_text())
            imports={n.module.split('.')[0] for n in ast.walk(tree) if isinstance(n,ast.ImportFrom) and n.module}
            imports|={a.name.split('.')[0] for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names}
            self.assertFalse(imports&banned,(path,imports&banned))
            self.assertNotIn('ChangeTransportSettings',path.read_text());self.assertNotIn('AddMember',path.read_text())

if __name__=='__main__':unittest.main()
