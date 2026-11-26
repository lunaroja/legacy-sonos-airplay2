"""Read-only proof of the observed paused native group; never returns write facts."""
from sonos_api import AIRPLAY,BackendError,Fact

def _target(peers,f,stage):
    try:return peers.recheck('target',f,stage=stage)
    except BackendError as error:
        if error.args!=('peer_evidence_changed',):raise
        with peers.lock:events=peers.events.get('target')
        if not events:raise error
        current,fields=peers._evidence_snapshot(events,'target',f)
        if (not 0<=peers.clock()-f.observed<=2
                or not all(fields.get(key) is True for key in (
                    'gena_subscriptions_initialized','gena_subscriptions_continuous',
                    'gena_subscriptions_unexpired','gena_uri_matches','gena_topology_matches',
                    'gena_revision_tokens_present','gena_uri_revision_matches','gena_topology_revision_matches'))
                or fields.get('gena_subscription_lost') is not False
                or fields.get('gena_transport')!='STOPPED' or fields.get('soap_transport')!='PLAYING'
                or fields.get('gena_transport_matches') is not False):raise error
        # Keep both the SOAP PLAYING value and the incoherent revision. This
        # local fact never leaves observe_owned_native_hold or authorizes a write.
        return Fact(f.peer,current,f.observed)

def observe_owned_native_hold(peers,expected):
    if set(expected)!={'target','coordinator'}:raise BackendError('owned_native_hold_unproven')
    old_t,old_r=expected['target'],expected['coordinator'];t,r=old_t.peer,old_r.peer
    native=AIRPLAY.fullmatch(r.uri);members=frozenset((t.uuid,r.uuid))
    if (not native or native[1]!=r.uuid or t.uuid==r.uuid or t.uri!='x-rincon:'+r.uuid
            or t.coordinator!=r.uuid or r.coordinator!=r.uuid or t.members!=members or r.members!=members
            or not all(f.revision.continuous and f.revision.source and f.revision.uri_revision
                       and f.revision.topology for f in (old_t,old_r))):
        raise BackendError('owned_native_hold_unproven')
    coordinator=peers.read('coordinator')
    # Only the actually observed paused state selects this policy. A different
    # state returns to the existing end proof, without relaxing that proof.
    if coordinator.peer.transport!='PAUSED_PLAYBACK':return False
    if coordinator.ownership()!=old_r.ownership():raise BackendError('owned_native_hold_changed')
    target=_target(peers,peers._observe('target'),'peer_read')
    coordinator=peers.recheck('coordinator',coordinator,stage='group_recheck')
    target=_target(peers,target,'group_recheck')
    current={'target':target,'coordinator':coordinator}
    for who,f in current.items():
        if f.ownership()!=expected[who].ownership():raise BackendError('owned_native_hold_changed')
        with peers.lock:events=peers.events.get(who)
        # Unchanged URI/topology revisions also reject foreign away/back, while
        # the locked current-snapshot check catches a late gap or source event.
        if not events or not events.direct_transition_matches(who,expected[who],f):
            raise BackendError('owned_native_hold_changed')
    return True
