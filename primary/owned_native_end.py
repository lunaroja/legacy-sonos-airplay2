"""Cleanup-only proof of one owned native source ending; no transport rewriting."""
from sonos_api import AIRPLAY,BackendError,Fact
from gena_runtime import transport_enum

class OwnedNativeEnd:
    @staticmethod
    def _unproven(stage,fields):
        error=BackendError('owned_native_end_unproven')
        error.native_end_diagnostics={'end_proof_stage':stage,**fields}
        return error
    def __init__(self,peers,expected):
        self.peers=peers;self.origin=dict(expected);self.used_exception=False
        if set(expected)!={'target','coordinator'}:
            raise self._unproven('origin_shape',{'end_origin_keys_match':False})
        t,r=expected['target'],expected['coordinator'];p,c=t.peer,r.peer
        native=AIRPLAY.fullmatch(c.uri);members=frozenset((p.uuid,c.uuid))
        origin={
            'end_origin_keys_match':True,'end_origin_native_uri_valid':bool(native),
            'end_origin_native_uuid_matches':bool(native and native[1]==c.uuid),
            'end_origin_distinct_peers':p.uuid!=c.uuid,
            'end_origin_target_uri_matches':p.uri=='x-rincon:'+c.uuid,
            'end_origin_target_coordinator_matches':p.coordinator==c.uuid,
            'end_origin_coordinator_matches':c.coordinator==c.uuid,
            'end_origin_target_members_match':p.members==members,
            'end_origin_coordinator_members_match':c.members==members,
            'end_origin_target_revision_continuous':bool(t.revision.continuous),
            'end_origin_coordinator_revision_continuous':bool(r.revision.continuous),
            'end_origin_target_tokens_present':bool(t.revision.source and t.revision.uri_revision and t.revision.topology),
            'end_origin_coordinator_tokens_present':bool(r.revision.source and r.revision.uri_revision and r.revision.topology)}
        if not all(origin.values()):raise self._unproven('origin',origin)
        try:self.ended=peers.read('coordinator')
        except Exception as error:
            error.native_end_diagnostics={'end_proof_stage':'coordinator_read',**origin}
            raise
        end=self.ended.peer
        required={
            'end_coordinator_uri_empty':not bool(end.uri),
            'end_coordinator_stopped':end.transport=='STOPPED',
            'end_coordinator_uuid_matches':end.uuid==c.uuid,
            'end_coordinator_boot_matches':end.boot==c.boot,
            'end_coordinator_group_matches':end.coordinator==c.coordinator,
            'end_coordinator_members_match':end.members==c.members,
            'end_coordinator_topology_revision_matches':self.ended.revision.topology==r.revision.topology}
        fields={**origin,**required,'end_coordinator_uri_matches_origin':end.uri==c.uri,
                'end_coordinator_uri_revision_matches_origin':self.ended.revision.uri_revision==r.revision.uri_revision,
                'end_coordinator_transport':transport_enum(end.transport),'end_direct_checked':False}
        if not all(required.values()):raise self._unproven('coordinator_state',fields)
        matched,direct=self._direct_diagnostics('coordinator',r,self.ended)
        fields.update(direct,end_direct_checked=True,end_direct_matches=matched)
        if not matched:raise self._unproven('coordinator_transition',fields)
    def _direct_diagnostics(self,who,before,after):
        with self.peers.lock:events=self.peers.events.get(who)
        if not events:return False,{'end_direct_events_available':False}
        matched,fields=events.direct_transition_with_diagnostics(who,before,after)
        return matched,{'end_direct_events_available':True,**fields}
    def _direct(self,who,before,after):
        with self.peers.lock:events=self.peers.events.get(who)
        return bool(events and events.direct_transition_matches(who,before,after))
    @staticmethod
    def allow_action(action,values):
        allowed=(action in ('BecomeCoordinatorOfStandaloneGroup','Stop') and not values
            or action=='SetAVTransportURI' and values=={'CurrentURI':'','CurrentURIMetaData':''})
        if not allowed:raise BackendError('owned_native_end_action')
    def _target(self,f):
        try:return self.peers.recheck('target',f)
        except BackendError as error:
            if error.args!=('peer_evidence_changed',):raise
            with self.peers.lock:events=self.peers.events.get('target')
            if not events:raise error
            current,fields=self.peers._evidence_snapshot(events,'target',f)
            if (not 0<=self.peers.clock()-f.observed<=2
                    or not all(fields.get(key) is True for key in (
                        'gena_subscriptions_initialized','gena_subscriptions_continuous',
                        'gena_subscriptions_unexpired','gena_uri_matches','gena_topology_matches',
                        'gena_revision_tokens_present','gena_uri_revision_matches','gena_topology_revision_matches'))
                    or fields.get('gena_subscription_lost') is not False
                    or fields.get('gena_transport')!='STOPPED' or fields.get('soap_transport')!='PLAYING'
                    or fields.get('gena_transport_matches') is not False):raise error
            self.used_exception=True
            # Preserve the actual SOAP transport and the incoherent revision.
            # This fact is usable only inside this end context, never as idle.
            return Fact(f.peer,current,f.observed)
    def read(self,anchor,*,action=None,values=None):
        if action is not None:self.allow_action(action,values or {})
        if set(anchor)!=set(self.origin):raise BackendError('owned_native_end_changed')
        # Prove the ended coordinator first; only then examine the target.
        r=self.peers.read('coordinator');t=self._target(self.peers._observe('target'))
        r=self.peers.recheck('coordinator',r);t=self._target(t)
        p,c=t.peer,r.peer;ot,oc=self.origin['target'].peer,self.origin['coordinator'].peer
        b=anchor['target'];rb=anchor['coordinator']
        if rb.ownership()==self.origin['coordinator'].ownership():rb=self.ended
        if (p.uuid!=ot.uuid or p.boot!=ot.boot or c.uuid!=oc.uuid or c.boot!=oc.boot
                or c.uri or c.transport!='STOPPED' or c.coordinator!=oc.uuid
                or r.revision.uri_revision!=self.ended.revision.uri_revision
                or r.revision.source!=self.ended.revision.source):
            raise BackendError('owned_native_end_changed')
        uris={b.peer.uri};target_groups={(b.peer.coordinator,b.peer.members)}
        coordinator_groups={(rb.peer.coordinator,rb.peer.members)}
        if action=='BecomeCoordinatorOfStandaloneGroup':
            uris.add('');target_groups.add((p.uuid,frozenset((p.uuid,))))
            coordinator_groups.add((c.uuid,frozenset((c.uuid,))))
        elif action=='SetAVTransportURI':uris.add('')
        if (p.uri not in uris or (p.coordinator,p.members) not in target_groups
                or (c.coordinator,c.members) not in coordinator_groups
                or not self._direct('target',b,t) or not self._direct('coordinator',rb,r)):
            raise BackendError('owned_native_end_changed')
        return {'target':t,'coordinator':r}
