"""Explicit local END authorizes target-only release from its owned paused group."""
from dataclasses import replace
from sonos_api import AIRPLAY,BackendError,Fact

class OwnedPausedRelease:
    kind='paused_release'
    def __init__(self,peers,expected,local_end):
        self.peers,self.origin,self.local_end=peers,dict(expected),local_end;self.used_exception=False
        self._local()
        if set(expected)!={'target','coordinator'}:raise BackendError('owned_paused_release_unproven')
        t,r=expected['target'],expected['coordinator'];p,c=t.peer,r.peer
        native=AIRPLAY.fullmatch(c.uri);members=frozenset((p.uuid,c.uuid))
        if (not native or native[1]!=c.uuid or p.uuid==c.uuid or p.uri!='x-rincon:'+c.uuid
                or p.coordinator!=c.uuid or c.coordinator!=c.uuid or p.members!=members or c.members!=members
                or not all(f.revision.continuous and f.revision.source and f.revision.uri_revision
                           and f.revision.topology for f in (t,r))):
            raise BackendError('owned_paused_release_unproven')
        self.paused=peers.read('coordinator');fresh=self.paused.peer
        if self.paused.ownership()!=r.ownership() or fresh.transport!='PAUSED_PLAYBACK':
            raise BackendError('owned_paused_release_unproven')
        # Pin the current paused source revision AND current independent gain.
        # Gain is not ownership: legitimate changes since the checkpoint must
        # be preserved. Later changes during this release still refuse.
        self.read(expected);self._local()
    def _local(self):
        if self.local_end() is not True:raise BackendError('owned_paused_release_local_end')
    @staticmethod
    def allow_action(action,values):
        allowed=(action in ('BecomeCoordinatorOfStandaloneGroup','Stop') and not values
            or action=='SetAVTransportURI' and values=={'CurrentURI':'','CurrentURIMetaData':''})
        if not allowed:raise BackendError('owned_paused_release_action')
    def _direct(self,who,before,after):
        with self.peers.lock:events=self.peers.events.get(who)
        return bool(events and events.direct_transition_matches(who,before,after))
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
            # Preserve raw PLAYING and the incoherent revision. This fact is
            # valid only inside this local-END release context, never as idle.
            return Fact(f.peer,current,f.observed)
    def read(self,anchor,*,action=None,values=None):
        self._local()
        if action is not None:self.allow_action(action,values or {})
        if set(anchor)!=set(self.origin):raise BackendError('owned_paused_release_changed')
        r=self.peers.read('coordinator');t=self._target(self.peers._observe('target'))
        r=self.peers.recheck('coordinator',r);t=self._target(t)
        p,c=t.peer,r.peer;ot,oc=self.origin['target'].peer,self.origin['coordinator'].peer
        b,rb=anchor['target'],anchor['coordinator']
        if (p.uuid!=ot.uuid or p.boot!=ot.boot
                or replace(c,members=self.paused.peer.members)!=self.paused.peer
                or r.revision.uri_revision!=self.paused.revision.uri_revision
                or r.revision.source!=self.paused.revision.source):
            raise BackendError('owned_paused_release_changed')
        uris={b.peer.uri};target_groups={(b.peer.coordinator,b.peer.members)}
        coordinator_groups={(rb.peer.coordinator,rb.peer.members)}
        if action=='BecomeCoordinatorOfStandaloneGroup':
            uris.update(('',self.paused.peer.uri))
            target_groups.add((p.uuid,frozenset((p.uuid,))))
            coordinator_groups.add((c.uuid,frozenset((c.uuid,))))
        elif action=='SetAVTransportURI':uris.add('')
        if (p.uri not in uris or (p.coordinator,p.members) not in target_groups
                or (c.coordinator,c.members) not in coordinator_groups
                or not self._direct('target',b,t) or not self._direct('coordinator',rb,r)):
            raise BackendError('owned_paused_release_changed')
        self._local()
        return {'target':t,'coordinator':r}
