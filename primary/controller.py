"""Primary target ownership: standalone WAV and explicitly matched native grouping."""
from dataclasses import asdict,replace
import hashlib,json,secrets,time
from sonos_api import AIRPLAY,BackendError,WRITES
from failure_diagnostics import diagnostic_fields

def packed(value):
    if hasattr(value,'__dataclass_fields__'):return packed(asdict(value))
    if isinstance(value,dict):return {k:packed(v) for k,v in value.items()}
    if isinstance(value,(tuple,list,set,frozenset)):return [packed(v) for v in (sorted(value) if isinstance(value,(set,frozenset)) else value)]
    return value

def changed_fields(before,after):
    fields=('uuid','uri','transport','coordinator','members','boot','volume','mute')
    return {
        who: {
            'peer': [field for field in fields if getattr(old.peer,field)!=getattr(after[who].peer,field)],
            'uri_revision':old.revision.uri_revision!=after[who].revision.uri_revision,
            'topology_revision':old.revision.topology!=after[who].revision.topology,
        }
        for who,old in before.items() if who in after
    }

class Controller:
    def __init__(self,peers,session,wave,scope,store,*,max_volume=100,clock=time.monotonic,sleep=time.sleep):
        self.peers,self.session,self.wave,self.scope,self.store=peers,session,wave,scope,store
        self.max_volume,self.clock,self.sleep=max_volume,clock,sleep
        self.mode='recovery' if store.data.get('active') else 'idle';self.expected={}
        self.generation=None;self.uri=None;self.volume_revision=0;self.retry_at=0
        self.failed=self.mode=='recovery';self.native_hold=False;self.native_resume_at=None
        self._failure_context=None
    def _read(self,group=False,*,end=None,action=None,before=None,values=None):
        if end is not None:
            if not group:raise BackendError('owned_native_end_action')
            self.scope.assert_owned()
            out=end.read(self.expected if before is None else before,action=action,values=values)
            self.scope.assert_owned();return out
        self.scope.assert_owned();out={'target':self.peers.read('target')}
        if group:out['coordinator']=self.peers.read('coordinator')
        for who,f in out.items():
            try:self.peers.recheck(who,f)
            except BackendError as error:
                fields=getattr(error,'evidence_diagnostics',None)
                if type(fields)is dict:fields['gena_read_stage']='group_recheck' if group else 'target_recheck'
                raise
        return out
    def _has_owned_end_reader(self):
        return callable(getattr(type(self.peers),'begin_owned_native_end',None))
    def _verified_native_checkpoint(self):
        return self._verified_checkpoint('both')
    def _verified_checkpoint(self,mode):
        active=self.store.data.get('active') or {}
        return (self.mode==mode and not self.failed and self.store.data.get('status')==mode
            and active.get('phase')=='verified' and active.get('mode')==mode
            and active.get('generation')==self.generation and active.get('uri')==self.uri
            and active.get('checkpoint')==packed(self.expected))
    def _local_native_hold(self):
        local=self.session()
        return local.state=='present' and local.generation==self.generation and local.media is False
    def _try_native_hold(self):
        if (not callable(getattr(type(self.peers),'observe_owned_native_hold',None))
                or not self._verified_native_checkpoint() or not self._local_native_hold()):return False
        checkpoint=packed(self.store.data);self.scope.assert_owned()
        observed=self.peers.observe_owned_native_hold(self.expected)
        self.scope.assert_owned()
        if not self._verified_native_checkpoint() or packed(self.store.data)!=checkpoint:
            raise BackendError('owned_native_hold_checkpoint')
        if not observed or not self._local_native_hold():return False
        self._mark_native_hold();return True
    def _mark_native_hold(self):
        if not self.native_hold:
            self.store.event('owned_native_hold',speaker_writes=False,
                             coordinator_transport='PAUSED_PLAYBACK',local_state='present',local_media=False,
                             local_generation_matches=True,checkpoint_unchanged=True,peer_ownership_unchanged=True)
        self.native_hold=True;self.native_resume_at=None
    def _local_end_confirmed(self,generation=None):
        local=self.session();generation=self.generation if generation is None else generation
        return (local.state=='ended' and local.generation==generation
                and local.end_event_confirmed is True and local.media is False)
    def _begin_paused_release(self):
        if not self._verified_native_checkpoint():raise BackendError('owned_paused_release_checkpoint')
        checkpoint=packed(self.store.data);self.scope.assert_owned()
        generation=self.generation
        release=self.peers.begin_owned_paused_release(self.expected,lambda:self._local_end_confirmed(generation))
        self.scope.assert_owned()
        if not self._verified_native_checkpoint() or packed(self.store.data)!=checkpoint:
            raise BackendError('owned_paused_release_checkpoint')
        return release
    def _has_local_release(self):
        return (callable(getattr(type(self.peers),'begin_owned_paused_release',None))
                and self._local_end_confirmed())
    def _begin_owned_end(self):
        if not self._verified_native_checkpoint():raise BackendError('owned_native_end_checkpoint')
        try:return self.peers.begin_owned_native_end(self.expected)
        except Exception as error:
            raw=getattr(error,'native_end_diagnostics',None)
            fields=dict(raw) if type(raw)is dict else {}
            try:
                local=self.session()
                fields.update(end_local_snapshot_available=True,end_local_state=local.state,
                    end_local_generation_matches=local.generation==self.generation,
                    end_local_media=bool(local.media))
            except Exception:fields['end_local_snapshot_available']=False
            error.native_end_diagnostics=fields
            raise
    def _owned_read(self,*,allow_hold=False):
        end=None
        try:current=self._read(self.mode=='both')
        except BackendError as error:
            fields=diagnostic_fields(error)
            if (self.mode!='both' or set(self.expected)!={'target','coordinator'}
                    or not AIRPLAY.fullmatch(self.expected['coordinator'].peer.uri)
                    or not self._has_owned_end_reader()
                    or error.args!=('peer_evidence_changed',) or fields.get('gena_peer')!='target'
                    or fields.get('gena_transport')!='STOPPED' or fields.get('soap_transport')!='PLAYING'
                    or fields.get('gena_transport_matches') is not False
                    or not all(fields.get(key) is True for key in (
                        'gena_events_available','gena_snapshot_available','gena_subscriptions_initialized',
                        'gena_subscriptions_continuous','gena_subscriptions_unexpired','gena_uri_matches',
                        'gena_topology_matches','gena_revision_tokens_present',
                        'gena_uri_revision_matches','gena_topology_revision_matches'))
                    or fields.get('gena_subscription_lost') is not False):raise
            if allow_hold and self._try_native_hold():return None,None
            # An unchanged, still-present native source has not ended merely
            # because its target transport channels disagree during a skip.
            # This selects one read-only observation, never a write fact.
            local=self.session()
            if (allow_hold and self._verified_native_checkpoint()
                    and local.state=='present' and local.generation==self.generation):
                try:coordinator=self.peers.read('coordinator')
                except BackendError as probe_error:
                    # Keep the initiating target skew when the classifier sees
                    # another transport race; never fall into a weaker retry.
                    probe=diagnostic_fields(probe_error);age=probe.get('peer_evidence_age_ms')
                    transient=(probe_error.args==('peer_evidence_changed',)
                        and probe.get('gena_peer')=='coordinator'
                        and {probe.get('gena_transport'),probe.get('soap_transport')}=={'PLAYING','PAUSED_PLAYBACK'}
                        and type(age)is int and 0<=age<=2000
                        and probe.get('peer_evidence_age_clipped') is False
                        and probe.get('gena_subscription_lost') is False
                        and all(probe.get(k) is True for k in (
                            'gena_events_available','gena_snapshot_available','gena_subscriptions_initialized',
                            'gena_subscriptions_continuous','gena_subscriptions_unexpired','gena_uri_matches',
                            'gena_topology_matches','gena_revision_tokens_present',
                            'gena_uri_revision_matches','gena_topology_revision_matches'))
                        and ((probe.get('gena_read_stage')=='peer_read'
                              and probe.get('gena_source_revision_matches') is True
                              and probe.get('gena_revision_matches') is True)
                             or (probe.get('gena_read_stage')=='group_recheck'
                                 and probe.get('gena_source_revision_matches') is False
                                 and probe.get('gena_revision_matches') is False)))
                    self.store.event('native_transport_probe_refused',speaker_writes=False,
                                     read_only_reclassification=bool(transient),**probe)
                    local=self.session()
                    if transient and local.state=='present' and local.generation==self.generation:
                        error.native_transport_observation=True
                    raise error from None
                local=self.session()
                if (coordinator.peer.transport in ('PLAYING','PAUSED_PLAYBACK')
                        and coordinator.ownership()==self.expected['coordinator'].ownership()
                        and local.state=='present' and local.generation==self.generation):
                    error.native_transport_observation=True
                    raise error
            paused_local_end=(self._has_local_release()
                and self.peers.read('coordinator').peer.transport=='PAUSED_PLAYBACK')
            end=self._begin_paused_release() if paused_local_end else self._begin_owned_end()
            current=self._read(True,end=end)
        if (end is None and self.mode=='both' and self._has_local_release()
                and current['coordinator'].peer.transport=='PAUSED_PLAYBACK'):
            end=self._begin_paused_release();current=self._read(True,end=end)
        if (end is None and self.mode=='both' and self._has_owned_end_reader()
                and not current['coordinator'].peer.uri and current['coordinator'].peer.transport=='STOPPED'):
            end=self._begin_owned_end();current=self._read(True,end=end)
        return current,end
    def _coordinator_end_pending(self,error,fields):
        """Classify a read-only wait while the Roam's end event catches up."""
        fact=getattr(error,'rejected_fact',None)
        if fact is None or fields.get('gena_peer')!='coordinator':
            return False
        old=self.expected['coordinator']
        peer=fact.peer
        if (fields.get('gena_read_stage')=='group_recheck'
                and fields.get('gena_uri_matches') is False
                and fields.get('gena_transport')=='STOPPED' and fields.get('gena_uri_empty') is True
                and fields.get('soap_transport')=='PAUSED_PLAYBACK'
                and fields.get('gena_source_revision_matches') is False
                and fields.get('gena_uri_revision_matches') is False
                and fields.get('gena_revision_matches') is False):
            return peer.transport=='PAUSED_PLAYBACK' and fact.ownership()==old.ownership()
        # STOPPED can arrive before the old native source is cleared or its
        # event is delivered. This observation only permits a new strict read.
        source_pending=(not peer.uri and fields.get('gena_uri_matches') is False)
        if peer.uri:
            source_pending=(fields.get('gena_uri_matches') is True
                            and fact.ownership()==old.ownership())
        return (fields.get('gena_read_stage')=='peer_read'
            and fields.get('gena_transport') in ('PLAYING','PAUSED_PLAYBACK')
            and fields.get('soap_transport')=='STOPPED'
            and fields.get('gena_uri_revision_matches') is True
            and fields.get('gena_source_revision_matches') is True
            and fields.get('gena_revision_matches') is True
            and source_pending and peer.transport=='STOPPED'
            and (peer.uuid,peer.boot,peer.coordinator,peer.members)
                ==(old.peer.uuid,old.peer.boot,old.peer.coordinator,old.peer.members)
            and fact.revision.uri_revision==old.revision.uri_revision
            and fact.revision.topology==old.revision.topology)

    @staticmethod
    def _continuous_transport_conflict(error,fields):
        age=fields.get('peer_evidence_age_ms')
        return (error.args==('peer_evidence_changed',)
                and fields.get('gena_transport_matches') is False
                and fields.get('gena_subscription_lost') is False
                and fields.get('peer_evidence_age_clipped') is False
                and type(age)is int and 0<=age<=2000
                and all(fields.get(key) is True for key in (
                    'gena_events_available','gena_snapshot_available','gena_subscriptions_initialized',
                    'gena_subscriptions_continuous','gena_subscriptions_unexpired',
                    'gena_topology_matches','gena_revision_tokens_present',
                    'gena_topology_revision_matches')))

    def _solo_transport_conflict(self,error,fields,continuous):
        fact=getattr(error,'rejected_fact',None)
        return (continuous and self._verified_checkpoint('solo')
                    and fact is not None
                    and fields.get('gena_peer')=='target' and fields.get('gena_read_stage')=='peer_read'
                    and {fields.get('gena_transport'),fields.get('soap_transport')}=={'PLAYING','STOPPED'}
                    and fields.get('gena_uri_matches') is True and fields.get('gena_revision_matches') is True
                    and fields.get('gena_uri_revision_matches') is True
                    and fact.ownership()==self.expected['target'].ownership())

    def _poll_owned(self):
        """One fresh strict read for a classified native observation race."""
        self._failure_context=('poll_owned','initial_read','not_eligible')
        started=self.clock();checkpoint=packed(self.store.data)
        eligible=self.native_hold and self._verified_native_checkpoint()
        try:return self._owned_read(allow_hold=True)
        except BackendError as error:
            if (error.args==('owned_paused_release_changed',) and self._local_end_confirmed()
                    and self._verified_native_checkpoint() and packed(self.store.data)==checkpoint):
                # The Roam can stop while the paused context is being
                # constructed. No detach intent exists at this stage.
                self._failure_context=('poll_owned','paused_end_retry','read_only_retry_selected')
                self.store.event('paused_context_end_recheck',speaker_writes=False,checkpoint_unchanged=True)
                self.sleep(.5)
                if (not self._local_end_confirmed() or not self._verified_native_checkpoint()
                        or packed(self.store.data)!=checkpoint or self.clock()>=started+5):
                    raise
                confirmed=self._begin_owned_end()
                current=self._read(True,end=confirmed)
                if not self._owned(current,allow_end=True):raise BackendError('owned_source_changed')
                return current,confirmed
            fields=diagnostic_fields(error)
            continuous=self._continuous_transport_conflict(error,fields)
            # Sonos can report the old WAV stopped before the sender ends its
            # local session. Retry the read only, then let tick use fresh media.
            if (packed(self.store.data)==checkpoint
                    and self._solo_transport_conflict(error,fields,continuous)):
                self._failure_context=('poll_owned','solo_retry_read','read_only_retry_selected')
                self.store.event('fresh_evidence_retry',retry_kind='solo_target_stop_event_pending',
                                 retry_limit=1,speaker_writes=False,checkpoint_unchanged=True)
                self.sleep(.5)
                if not self._verified_checkpoint('solo') or packed(self.store.data)!=checkpoint:
                    raise BackendError('read_retry_checkpoint_changed')
                if self.clock()>=started+5:raise BackendError('read_retry_exhausted')
                current=self._read(False)
                if not self._owned(current):raise BackendError('owned_source_changed')
                return current,None
            common=(continuous and self._verified_native_checkpoint()
                and packed(self.store.data)==checkpoint and set(self.expected)=={'target','coordinator'}
                and AIRPLAY.fullmatch(self.expected['coordinator'].peer.uri))
            target_resume=(eligible and fields.get('gena_peer')=='target'
                and fields.get('gena_read_stage')=='group_recheck'
                and fields.get('gena_transport')=='PLAYING' and fields.get('soap_transport')=='STOPPED'
                and fields.get('gena_source_revision_matches') is False
                and fields.get('gena_revision_matches') is False)
            # A native skip can expose PAUSED/PLAYING in different read channels.
            # Re-read once without rebasing ownership or touching either speaker.
            coordinator_transport=(fields.get('gena_peer')=='coordinator'
                and {fields.get('gena_transport'),fields.get('soap_transport')}=={'PLAYING','PAUSED_PLAYBACK'}
                and ((fields.get('gena_read_stage')=='peer_read'
                      and fields.get('gena_source_revision_matches') is True
                      and fields.get('gena_revision_matches') is True)
                     or (fields.get('gena_read_stage')=='group_recheck'
                         and fields.get('gena_source_revision_matches') is False
                         and fields.get('gena_revision_matches') is False)))
            native_transport=(getattr(error,'native_transport_observation',False) is True
                and fields.get('gena_peer')=='target'
                and fields.get('gena_transport')=='STOPPED' and fields.get('soap_transport')=='PLAYING'
                and ((fields.get('gena_read_stage')=='peer_read'
                      and fields.get('gena_source_revision_matches') is True
                      and fields.get('gena_revision_matches') is True)
                     or (fields.get('gena_read_stage')=='group_recheck'
                         and fields.get('gena_source_revision_matches') is False
                         and fields.get('gena_revision_matches') is False)))
            fact=getattr(error,'rejected_fact',None)
            native_target_stop=(fact is not None and fields.get('gena_peer')=='target'
                and fields.get('gena_read_stage')=='peer_read'
                and fields.get('gena_transport')=='PLAYING' and fields.get('soap_transport')=='STOPPED'
                and fields.get('gena_source_revision_matches') is True
                and fields.get('gena_revision_matches') is True
                and common and fact.ownership()==self.expected['target'].ownership())
            coordinator_end=common and self._coordinator_end_pending(error,fields)
            if not coordinator_end and not (common and fields.get('gena_uri_matches') is True
                    and fields.get('gena_uri_revision_matches') is True
                    and (target_resume or coordinator_transport or native_transport or native_target_stop)):raise
        resume_retry=(native_transport and fields.get('gena_read_stage')=='peer_read'
            and fact is not None and fact.ownership()==self.expected['target'].ownership()
            and (self.native_hold or self.native_resume_at is not None and 0<=started-self.native_resume_at<=3)
            and self._local())
        retry_kind=('coordinator_end_event_pending' if coordinator_end else
                    'native_target_stop_event_pending' if native_target_stop else
                    'native_target_resume_event_pending' if resume_retry else
                    'native_target_transport_observation' if native_transport else
                    'target_transport_revision_race' if target_resume else 'coordinator_transport_read_race')
        def unchanged():
            if not self._verified_native_checkpoint() or packed(self.store.data)!=checkpoint:
                raise BackendError('read_retry_checkpoint_changed')
            if self.clock()>=started+5:raise BackendError('read_retry_exhausted')
            self.scope.assert_owned()
        unchanged()
        self._failure_context=('poll_owned','native_retry_read','read_only_retry_selected')
        self.store.event('fresh_evidence_retry',retry_kind=retry_kind,
                         retry_limit=2 if resume_retry else 1,speaker_writes=False,checkpoint_unchanged=True,
                         **(fields if native_transport or coordinator_transport or coordinator_end or native_target_stop else {}))
        self.sleep(.5);unchanged()
        if native_transport or coordinator_transport or coordinator_end or native_target_stop:
            # Reclassify using the original HOLD/END proofs. Only an active
            # resume with the identical pinned read mismatch gets one more look.
            try:current,end=self._owned_read(allow_hold=True)
            except BackendError as retry_error:
                retry_fields=diagnostic_fields(retry_error);retry_age=retry_fields.get('peer_evidence_age_ms')
                retry_fact=getattr(retry_error,'rejected_fact',None)
                if (not resume_retry or not self._local() or retry_error.args!=('peer_evidence_changed',)
                        or getattr(retry_error,'native_transport_observation',False) is not True
                        or retry_fact is None or retry_fact.ownership()!=self.expected['target'].ownership()
                        or type(retry_age)is not int or not 0<=retry_age<=2000
                        or retry_fields.get('peer_evidence_age_clipped') is not False
                        or any(retry_fields.get(key)!=value for key,value in fields.items() if key.startswith('gena_'))):
                    raise
                self._failure_context=('poll_owned','native_second_retry','read_only_retry_selected')
                unchanged();self.sleep(.5);unchanged()
                current,end=self._owned_read(allow_hold=True)
            unchanged();local=self.session()
            ended=bool(current and not current['coordinator'].peer.uri
                       and current['coordinator'].peer.transport=='STOPPED')
            cleanup_only=(coordinator_end or native_target_stop) and ended and end is not None
            if (not cleanup_only and (local.generation!=self.generation or local.state in ('lost','unproven')
                    or local.state=='ended' and not self._local_end_confirmed())):
                raise BackendError('read_retry_local_changed')
            if current is None:
                if not self.native_hold or not self._local_native_hold():raise BackendError('read_retry_local_changed')
                self.store.event('fresh_evidence_hold_proven',retry_kind=retry_kind,speaker_writes=False)
                return None,None
            if not self._owned(current,allow_end=bool(end is not None and ended)):
                raise BackendError('owned_source_changed')
            if end is not None:
                self.store.event('fresh_evidence_cleanup_proven',retry_kind=retry_kind,cleanup_only=True,
                                 native_end=ended,speaker_writes=False)
                return current,end
            if local.state=='present' and not local.media and current['coordinator'].peer.transport=='PAUSED_PLAYBACK':
                if not self._try_native_hold():raise BackendError('read_retry_local_changed')
                unchanged();self.store.event('fresh_evidence_hold_proven',retry_kind=retry_kind,speaker_writes=False)
                return None,None
            if not ((local.state=='present' and local.media or self._local_end_confirmed())
                    and current['coordinator'].peer.transport=='PLAYING'):
                raise BackendError('read_retry_local_changed')
            unchanged();self.store.event('fresh_evidence_revalidated',retry_kind=retry_kind,
                checkpoint_unchanged=True,peer_ownership_unchanged=True)
            return current,None
        # No write retry, normalization, or checkpoint rebase. A second mismatch
        # escapes; original source/topology revisions must still authorize use.
        current=self._read(True);unchanged()
        if self.session().generation!=self.generation:raise BackendError('read_retry_local_changed')
        coordinator=current['coordinator'].peer
        if (target_resume and self._has_owned_end_reader()
                and not coordinator.uri and coordinator.transport=='STOPPED'):
            # Roam removal can win the race with a paused target resuming.
            # The ordinary native-end proof still owns any subsequent cleanup.
            end=self._begin_owned_end()
            current=self._read(True,end=end);unchanged()
            if not self._owned(current,allow_end=True):raise BackendError('owned_source_changed')
            self.store.event('fresh_evidence_cleanup_proven',retry_kind=retry_kind,cleanup_only=True,
                             native_end=True,speaker_writes=False)
            return current,end
        if not self._owned(current):raise BackendError('owned_source_changed')
        self.store.event('fresh_evidence_revalidated',retry_kind=retry_kind,
                         checkpoint_unchanged=True,peer_ownership_unchanged=True)
        return current,None
    def _owned(self,current,allow_end=False):
        if set(current)!=set(self.expected):return False
        for who,f in current.items():
            old=self.expected[who]
            if f.ownership()==old.ownership():continue
            p,b=f.peer,old.peer
            if (allow_end and who=='coordinator' and not p.uri and p.transport=='STOPPED'
                    and b.uri and p.uuid==b.uuid and p.boot==b.boot and p.members==b.members
                    and p.coordinator==b.coordinator and f.revision.topology==old.revision.topology):continue
            return False
        return True
    def _local(self):
        e=self.session();return e.generation==self.generation and e.state=='present' and e.media
    def _match(self,action,before,after,values):
        b,a=before['target'].peer,after['target'].peer;want=b
        if action=='SetAVTransportURI':
            uri=values['CurrentURI']
            if uri.startswith('x-rincon:'):
                r=before['coordinator'].peer
                want=replace(b,uri=uri,coordinator=r.uuid,members=r.members|{b.uuid},transport=a.transport)
                if a.transport not in ('PLAYING','PAUSED_PLAYBACK'):return False
            else:
                want=replace(b,uri=uri,transport=a.transport)
                if a.transport not in ('STOPPED','PLAYING') or not uri and a.transport!='STOPPED':return False
        elif action=='Play':want=replace(b,transport='PLAYING')
        elif action=='BecomeCoordinatorOfStandaloneGroup':
            r=before['coordinator'].peer
            if a.uri not in ('',b.uri,r.uri) or a.transport not in ('PLAYING','STOPPED','PAUSED_PLAYBACK'):return False
            want=replace(b,uri=a.uri,transport=a.transport,coordinator=b.uuid,members=frozenset((b.uuid,)))
        elif action=='Stop':want=replace(b,transport='STOPPED')
        elif action=='SetVolume':want=replace(b,volume=values['DesiredVolume'])
        if a!=want:return False
        for who in before:
            old,new=before[who],after[who]
            expected=want if who=='target' else old.peer
            if who=='coordinator':
                if action=='SetAVTransportURI' and values.get('CurrentURI','').startswith('x-rincon:'):
                    expected=replace(expected,members=expected.members|{b.uuid},volume=new.peer.volume)
                elif action=='BecomeCoordinatorOfStandaloneGroup':expected=replace(expected,members=expected.members-{b.uuid})
                if new.peer!=expected:return False
            if old.peer.uri==expected.uri and old.revision.uri_revision!=new.revision.uri_revision:return False
            if (old.peer.coordinator,old.peer.members)==(expected.coordinator,expected.members) and old.revision.topology!=new.revision.topology:return False
        return True
    def _inflight(self,action,before,after,values):
        """Only old/expected intermediate fields may settle; foreign state stops."""
        b,a=before['target'].peer,after['target'].peer
        uris={b.uri};coordinators={b.coordinator};members={b.members}
        if action=='SetAVTransportURI':
            uri=values['CurrentURI'];uris.add(uri)
            if uri.startswith('x-rincon:'):
                r=before['coordinator'].peer;coordinators.add(r.uuid);members.add(r.members|{b.uuid})
        elif action=='BecomeCoordinatorOfStandaloneGroup':
            uris.update(('',before['coordinator'].peer.uri));coordinators.add(b.uuid);members.add(frozenset((b.uuid,)))
        volumes={b.volume}
        if action=='SetVolume':volumes.add(values['DesiredVolume'])
        if (a.uuid!=b.uuid or a.boot!=b.boot or a.uri not in uris or a.coordinator not in coordinators
                or a.members not in members or a.volume not in volumes or a.mute!=b.mute
                or a.transport not in ('PLAYING','STOPPED','PAUSED_PLAYBACK','TRANSITIONING')):return False
        for who in before:
            old,new=before[who],after[who]
            if old.peer.uri==new.peer.uri and old.revision.uri_revision!=new.revision.uri_revision:return False
            if (old.peer.coordinator,old.peer.members)==(new.peer.coordinator,new.peer.members) and old.revision.topology!=new.revision.topology:return False
        if 'coordinator' in before:
            b,a=before['coordinator'].peer,after['coordinator'].peer;allowed={b.members}
            if action=='SetAVTransportURI' and values.get('CurrentURI','').startswith('x-rincon:'):
                allowed.add(b.members|{before['target'].peer.uuid})
                a=replace(a,volume=b.volume)
            if action=='BecomeCoordinatorOfStandaloneGroup':allowed.add(b.members-{before['target'].peer.uuid})
            if a.members not in allowed or replace(a,members=b.members)!=b:return False
        return True
    def _native_volume_readback(self,before,after):
        # The volume command targets Office only. Either speaker's gain can
        # also change through Music while its acknowledgement is in flight.
        return (self._owned(after)
            and any(after[who].peer.volume!=old.peer.volume for who,old in before.items())
            and all(replace(after[who].peer,volume=old.peer.volume)==old.peer
                    for who,old in before.items()))
    def _native_volume_stop_readback(self,before,after):
        # A stopped coordinator can retain its native URI until source END.
        # Settling this acknowledgement does not authorize a detach.
        return (self.session().generation==self.generation and self._owned(after)
            and before['coordinator'].peer.transport in ('PLAYING','PAUSED_PLAYBACK')
            and after['coordinator'].peer.transport=='STOPPED'
            and all(replace(after[who].peer,transport=old.peer.transport)==old.peer
                    for who,old in before.items()))
    def _settle_volume_during_native_end(self,before,observed,operation,requested):
        if not self._has_owned_end_reader():return False
        coordinator=observed['coordinator'].peer
        if (coordinator.uri or coordinator.transport!='STOPPED'
                or not self._owned(observed,allow_end=True)):
            return False
        # Prove the direct END against the pre-command source. Keep that
        # predecessor in the checkpoint so the next tick can prove cleanup.
        self.scope.assert_owned()
        end=self.peers.begin_owned_native_end(before)
        settled=self._read(True,end=end,before=before)
        if not self._owned(settled,allow_end=True):raise BackendError('owned_source_changed')
        self.expected={**before,'target':settled['target']};self._save('verified','SetVolume')
        self.store.event('volume_acknowledged_during_native_end',operation=operation,
            requested_gain_observed=settled['target'].peer.volume==requested,
            changes=changed_fields(before,settled),speaker_command_sent=True)
        return True

    def _settle_solo_volume_timeout(self,before,operation):
        # Volume is ancillary to the owned WAV. If the acknowledged gain was
        # never observed, keep the stream only after two fresh ownership reads.
        first=self._read(False)
        self.sleep(.05)
        second=self._read(False)
        old=before['target'].peer
        observed=second['target'].peer
        local=self.session()
        if (local.generation!=self.generation or local.state=='lost'
                or not self._owned(first) or not self._owned(second)
                or first['target'].peer!=observed
                or observed.transport not in ('PLAYING','STOPPED','PAUSED_PLAYBACK')
                or replace(observed,transport=old.transport)!=old):
            return False
        self.expected=second;self._save('verified','SetVolume')
        self.store.event('solo_volume_readback_unconfirmed',operation=operation,
            requested_gain_observed=False,speaker_command_sent=True,
            changes=changed_fields(before,second))
        return True
    def _save(self,phase,action=None):
        self.store.data.update(status=self.mode,active={'mode':self.mode,'generation':self.generation,'uri':self.uri,
            'phase':phase,'action':action,'checkpoint':packed(self.expected),'volume_revision':self.volume_revision})
        self.store.persist()
    def _write(self,action,guard,*,group=False,end=None,volume_revision=None,**values):
        label=action if action in WRITES else 'unknown_action'
        self._failure_context=(label,'before_intent','not_applicable')
        if end is not None:end.allow_action(action,values)
        try:
            before=self._read(group,end=end) if end is not None else self._read(group)
        except BackendError as error:
            if action=='SetVolume' and volume_revision is not None and not group:
                fields=diagnostic_fields(error)
                if self._solo_transport_conflict(error,fields,self._continuous_transport_conflict(error,fields)):
                    decision='defer_unsent_volume'
                    self._failure_context=(label,'before_intent',decision)
                    self.store.event('volume_deferred',stage='before_intent',speaker_command_sent=False,
                        controller_operation=label,controller_phase='before_intent',retry_decision=decision,
                        checkpoint_phase='verified',**fields)
                    return False
            raise
        prior=self.store.data.get('active')
        can_finalize_detach=(action=='BecomeCoordinatorOfStandaloneGroup' and group
                             and self._verified_native_checkpoint())
        can_defer_volume=(action=='SetVolume' and volume_revision is not None and prior is not None
            and prior.get('phase')=='verified' and prior.get('mode')==self.mode
            and prior.get('generation')==self.generation and self._owned(before))
        if not guard(before):
            if can_defer_volume:
                self.store.event('volume_deferred',stage='before_intent',speaker_command_sent=False)
                return False
            local=self.session()
            self.store.event('pre_intent_refused',action=action,
                same_generation=local.generation==self.generation,
                local_present=local.state=='present',local_media=bool(local.media),
                volume_revision_changed=local.volume_revision!=self.volume_revision,
                ownership_matches=self._owned(before),changes=changed_fields(self.expected,before),
                speaker_command_sent=False)
            raise BackendError('ownership_or_selection_changed')
        operation=secrets.token_urlsafe(24)
        self._failure_context=(label,'before_send','not_applicable')
        self._save('intent',action);self.store.event('intent',action=action,operation=operation,before=packed(before))
        check=self._read(group,end=end) if end is not None else self._read(group)
        # The owned WAV can stop naturally after revoke and before Stop is sent.
        # Prove that exact postcondition twice; never fabricate an acknowledgement
        # or send/replay a command that is already unnecessary.
        if action=='Stop' and guard(check) and self._match(action,before,check,values):
            self.sleep(.05)
            final=self._read(group,end=end) if end is not None else self._read(group)
            if (guard(final) and self._match(action,before,final,values)
                    and final.keys()==check.keys()
                    and all(final[w].peer==check[w].peer and final[w].ownership()==check[w].ownership() for w in check)):
                self.store.event('not_sent_already_satisfied',action=action,operation=operation,speaker_command_sent=False)
                self.expected=final;self._save('verified',action)
                self.store.event('verified',action=action,operation=operation,speaker_command_sent=False);return
        if check.keys()!=before.keys() or any(check[w].peer!=before[w].peer or check[w].ownership()!=before[w].ownership() for w in before) or not guard(check):
            old_target=before['target'].peer
            current_target=check['target'].peer
            if (can_defer_volume and self._owned(check)
                    and replace(current_target,volume=old_target.volume,mute=old_target.mute)==old_target):
                # The volume intent has not been sent. Preserve the previous
                # verified checkpoint and process any fresh input on a new tick.
                gain_changed=current_target.volume!=old_target.volume
                mute_changed=current_target.mute!=old_target.mute
                self.store.event('volume_deferred',stage='before_send',operation=operation,
                    changes=changed_fields(before,check),speaker_command_sent=False,
                    superseded_by_observed_gain=gain_changed,superseded_by_observed_mute=mute_changed)
                if gain_changed or mute_changed:
                    # Honor the observed rendering choice. A newer sender revision can
                    # still request a different one.
                    self.volume_revision=volume_revision
                self.store.data['active']={**prior,'volume_revision':self.volume_revision}
                self.store.persist()
                return False
            self.store.event('pre_send_refused',action=action,operation=operation,
                target_transport_changed=check['target'].peer.transport!=before['target'].peer.transport,
                target_volume_changed=check['target'].peer.volume!=before['target'].peer.volume,
                target_mute_changed=check['target'].peer.mute!=before['target'].peer.mute,
                ownership_changed=any(w not in check or check[w].ownership()!=before[w].ownership() for w in before))
            raise BackendError('changed_after_intent')
        self._failure_context=(label,'send','no_command_replay')
        try:self.peers.write(action,**values)
        except BackendError as error:
            fields=diagnostic_fields(error)
            if (can_finalize_detach and error.args==('http_transport',)
                    and fields.get('speaker_http_phase')=='response_headers'
                    and fields.get('speaker_transport_reason')=='timeout'):
                error.detach_response_timeout=True
                self._failure_context=(label,'send','verify_idle_only')
            raise
        self.store.event('acknowledged',action=action,operation=operation)
        self._failure_context=(label,'readback','no_command_replay')
        until=self.clock()+8;stable=None
        while self.clock()<until:
            try:after=(self._read(group,end=end,action=action,before=check,values=values)
                       if end is not None else self._read(group))
            except BackendError as error:
                if error.args!=('peer_evidence_changed',):
                    if (action=='BecomeCoordinatorOfStandaloneGroup'
                            and getattr(end,'kind',None)=='paused_release'
                            and error.args==('owned_paused_release_changed',)):
                        error.acknowledged_paused_detach=True
                    raise
                self._failure_context=(label,'readback','read_only_retry_selected')
                stable=None;self.sleep(.05);continue
            matched=self._match(action,check,after,values)
            native_volume=not matched and action=='SetVolume' and group and self.mode=='both'
            concurrent_volume=native_volume and self._native_volume_readback(check,after)
            native_stop=native_volume and self._native_volume_stop_readback(check,after)
            if (native_volume and can_defer_volume
                    and self._settle_volume_during_native_end(check,after,operation,values['DesiredVolume'])):
                return True
            if not matched and not concurrent_volume and not native_stop:
                if not self._inflight(action,check,after,values):
                    self.store.event('post_ack_readback_refused',action=action,
                        changes=changed_fields(check,after))
                    raise BackendError('unexpected_write_readback')
                stable=None;self.sleep(.05);continue
            joining=action=='SetAVTransportURI' and values['CurrentURI'].startswith('x-rincon:')
            # Joining changes topology. A concurrent Roam gain is independent
            # of that postcondition and is never set by the controller.
            key=tuple((w,replace(f.peer,volume=0) if joining and w=='coordinator' else f.peer,
                       f.ownership()) for w,f in sorted(after.items()))
            if stable==key:
                self.expected=after;self._save('verified',action)
                if native_stop or concurrent_volume:
                    self.store.event('volume_acknowledged_during_native_stop' if native_stop
                        else 'volume_settled_with_concurrent_change',operation=operation,
                        requested_gain_observed=after['target'].peer.volume==values['DesiredVolume'],
                        changes=changed_fields(check,after),speaker_command_sent=True)
                else:
                    self.store.event('verified',action=action,operation=operation)
                return True
            stable=key;self.sleep(.05)
        if (action=='SetVolume' and volume_revision is not None and not group and self.mode=='solo'
                and self._settle_solo_volume_timeout(check,operation)):
            return True
        raise BackendError('write_readback_timeout')
    def _fail(self,error=None):
        operation,phase,retry=self._failure_context or ('runtime','outside_controller','not_applicable')
        active_phase=(self.store.data.get('active') or {}).get('phase')
        checkpoint_phase=active_phase if active_phase in ('verified','intent') else None
        self.failed=True;self.mode='recovery';self.native_hold=False;self.wave.revoke()
        self.store.data['status']='recovery';self.store.persist()
        self.store.event('failed',reason='uncertain_operation_no_replay',controller_operation=operation,
                         controller_phase=phase,retry_decision=retry,checkpoint_phase=checkpoint_phase,
                         **diagnostic_fields(error))
    def _begin_solo(self,f,e):
        self.generation=e.generation;self.uri=self.wave.activate(e.generation);self.mode='solo'
        self.expected={'target':f};self.volume_revision=0
        self._write('SetAVTransportURI',lambda s:self._local() and s['target'].peer.idle() and self._owned(s),CurrentURI=self.uri,CurrentURIMetaData='')
        self._write('Play',lambda s:self._local() and self._owned(s),Speed='1')
    def _join(self,target,coordinator,e):
        same_generation=e.generation==self.generation
        self.generation=e.generation;self.expected={'target':target,'coordinator':coordinator}
        self.mode='both';self.native_resume_at=None;self.wave.revoke()
        if not same_generation:self.volume_revision=0
        def allowed(s):
            t,r=s['target'].peer,s['coordinator'].peer
            return (self._local() and self._owned(s) and t.standalone() and r.standalone()
                and self.peers.same_group(self.session(),s['coordinator']))
        self._write('SetAVTransportURI',allowed,group=True,CurrentURI='x-rincon:'+coordinator.peer.uuid,CurrentURIMetaData='')
    def _cleanup(self,*,allow_end=False,end=None):
        paused=getattr(end,'kind',None)=='paused_release'
        checkpoint=packed(self.store.data) if paused else None
        try:
            return self._cleanup_target(allow_end=allow_end,end=end)
        except BackendError as error:
            response_timeout=getattr(error,'detach_response_timeout',False)
            if response_timeout or (paused and getattr(error,'acknowledged_paused_detach',False)):
                # Finalize only when physical idle is already proven. A lost
                # response never authorizes a replay or intermediate cleanup.
                import recovery
                self.sleep(.5)
                preview=recovery.finalize_proposal(self)
                if preview['status']!='reviewable_idle_finalize':raise
                recovery.finalize(self,preview['fingerprint'])
                self.store.event('detach_timeout_already_idle' if response_timeout
                    else 'paused_detach_already_idle',speaker_writes=False)
                return
            if (not paused or error.args!=('owned_paused_release_changed',) or not self._local_end_confirmed()
                    or not self._verified_native_checkpoint() or packed(self.store.data)!=checkpoint):
                raise
        # Office END was already authenticated. Reclassify a possible Roam
        # source end only before a detach intent exists. If a command was
        # journaled, the original recovery path keeps that intent intact.
        self.store.event('paused_release_end_recheck',speaker_writes=False,checkpoint_unchanged=True)
        self.sleep(.5)
        if (not self._local_end_confirmed() or not self._verified_native_checkpoint()
                or packed(self.store.data)!=checkpoint):
            raise BackendError('owned_paused_release_changed')
        confirmed_end=self._begin_owned_end()
        if not self._owned(self._read(True,end=confirmed_end),allow_end=True):
            raise BackendError('owned_source_changed')
        return self._cleanup(allow_end=True,end=confirmed_end)

    def _cleanup_target(self,*,allow_end=False,end=None):
        self._failure_context=('cleanup','source_proof','not_applicable')
        if end is None and self.mode=='both' and self._has_owned_end_reader():
            current,end=self._owned_read()
            allow_end=not current['coordinator'].peer.uri and current['coordinator'].peer.transport=='STOPPED'
            if not self._owned(current,allow_end):raise BackendError('owned_source_changed')
        if end is not None:
            if getattr(end,'kind',None)=='paused_release':
                self.store.event('owned_paused_release_authorized',target_only=True,
                    local_end_confirmed=True,local_generation_matches=True,checkpoint_unchanged=True,
                    coordinator_paused=True,coordinator_source_pinned=True,peer_ownership_unchanged=True,
                    target_transport_exception=end.used_exception)
            else:
                self.store.event('owned_native_end_cleanup',target_only=True,
                                 target_transport_exception=end.used_exception)
        self.wave.revoke()
        if self.mode=='both' and not self.expected['target'].peer.standalone():
            self._write('BecomeCoordinatorOfStandaloneGroup',lambda s:self._owned(s,allow_end),group=True,end=end)
            # After detach, retain coordinator proof through stop/clear so a
            # target cleanup can never conceal a changed source/transport there.
        group='coordinator' in self.expected
        if self.expected['target'].peer.transport!='STOPPED':
            self._write('Stop',lambda s:self._owned(s),group=group,end=end)
        if self.expected['target'].peer.uri:
            self._write('SetAVTransportURI',lambda s:self._owned(s) and s['target'].peer.standalone(),group=group,end=end,CurrentURI='',CurrentURIMetaData='')
        self._failure_context=('cleanup','final_read','not_applicable')
        final=self._read(group,end=end) if end is not None else self._read(group)
        if not self._owned(final) or not final['target'].peer.idle():raise BackendError('cleanup_not_idle')
        self.expected={};self.mode='idle';self.generation=self.uri=None;self.volume_revision=0;self.native_hold=False
        self.store.clean();self.store.event('complete',mode='idle')
    def _volume(self):
        e=self.session()
        if not self._local() or not e.volume_revision or e.volume_revision==self.volume_revision:return
        value=0 if e.volume_milli==-144000 else max(0,min(self.max_volume,round((e.volume_milli+30000)*self.max_volume/30000)))
        revision=e.volume_revision;group=self.mode=='both'
        if self._write('SetVolume',lambda s:self._local() and self._owned(s) and self.session().volume_revision==revision,
                       group=group,volume_revision=revision,DesiredVolume=value):
            self.volume_revision=revision;self._save('verified','SetVolume')
    def _candidate(self,e):
        if hasattr(self.peers,'candidate'):return self.peers.candidate(e)
        try:return self.peers.read('coordinator')
        except (BackendError,KeyError):return None
    def tick(self):
        if self.failed:return 'recovery'
        self._failure_context=('tick','selection','not_applicable')
        try:
            e=self.session()
            if self.mode!='idle':
                current,end=self._poll_owned()
                if current is None:return self.mode # Read-only HOLD cannot reach cleanup or volume.
                ended_native=(self.mode=='both' and not current['coordinator'].peer.uri and current['coordinator'].peer.transport=='STOPPED')
                if not self._owned(current,ended_native):raise BackendError('owned_source_changed')
                e=self.session() # Reads may span a receiver pause, END, or new generation.
                if (getattr(end,'kind',None)=='paused_release'
                        and not (e.state=='ended' and e.generation==self.generation
                                 and e.end_event_confirmed is True and e.media is False)):
                    raise BackendError('owned_paused_release_local_end')
                same_present=e.state=='present' and e.generation==self.generation
                if (self.mode=='both' and same_present and e.media is False
                        and current['coordinator'].peer.transport=='PAUSED_PLAYBACK'):
                    if not self._verified_native_checkpoint():raise BackendError('owned_native_hold_checkpoint')
                    self._mark_native_hold();return self.mode
                resumed=(self.mode=='both' and same_present and e.media is True
                         and current['coordinator'].peer.transport=='PLAYING')
                if self.native_hold and (end is not None or resumed):
                    self.store.event('owned_native_hold_finished',observation=(
                        'local_end' if getattr(end,'kind',None)=='paused_release' else
                        'native_end' if end is not None else 'strict_read'))
                    self.native_hold=False
                    self.native_resume_at=self.clock() if resumed and end is None else None
                if self.native_hold and same_present:return self.mode
                if e.generation!=self.generation or e.state in ('ended','lost'):
                    self._cleanup(allow_end=ended_native,end=end);return self.mode
                if ended_native:
                    self._cleanup(allow_end=True,end=end);return self.mode
                if self.mode=='both':
                    # HOLD is session presence, never newly synthesized media.
                    if e.state!='present':return self.mode
                    self._volume();return self.mode
                if e.state=='present' and e.media:
                    coordinator=self._candidate(e)
                    if coordinator and coordinator.peer.standalone() and self.peers.same_group(e,coordinator):
                        self._join(current['target'],coordinator,e)
                    elif current['target'].peer.transport=='STOPPED' and not self.wave.clients and self.clock()>=self.retry_at:
                        self.retry_at=self.clock()+1.5
                        self._write('Play',lambda s:self._local() and self._owned(s),Speed='1')
                    self._volume()
                return self.mode
            if e.state!='present' or not e.media:return self.mode
            target=self.peers.read('target')
            if not target.peer.idle():return 'waiting'
            coordinator=self._candidate(e)
            if coordinator and coordinator.peer.standalone() and self.peers.same_group(e,coordinator):
                self._join(target,coordinator,e)
            else:self._begin_solo(target,e)
            self._volume();return self.mode
        except Exception as error:
            self._fail(error);return 'recovery'
        finally:self._failure_context=None
    def shutdown(self):
        if self.failed:return False
        if self.mode=='idle':self.wave.revoke();return True
        try:self._cleanup();return True
        except Exception as error:self._fail(error);return False
        finally:self._failure_context=None
