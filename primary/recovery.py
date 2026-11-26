"""Explicit recovery of a fully verified checkpoint; uncertain writes are never replayed."""
import hashlib,json
from controller import Controller,packed
from sonos_api import BackendError

def proposal(controller):
    active=controller.store.data.get('active')
    if not active:return {'status':'already_clean','fingerprint':None}
    if active.get('phase')!='verified' or active.get('mode') not in ('solo','both'):
        return {'status':'uncertain_write_requires_manual_review','fingerprint':None}
    current=controller._read('coordinator' in active.get('checkpoint',{}))
    old=active['checkpoint']
    if set(current)!=set(old):return {'status':'checkpoint_changed','fingerprint':None}
    for who,f in current.items():
        # Subscription revisions are per process. Exact durable peer identity,
        # source, topology and boot bind explicit recovery after restart.
        prior=old[who]['peer'];now=packed(f.peer)
        if any(now[k]!=prior[k] for k in ('uuid','uri','coordinator','members','boot')):
            return {'status':'checkpoint_changed','fingerprint':None}
    key={'active':active,'current':packed(current)}
    # Only the user's proposal state, not transient timestamps/revision tokens,
    # participates in its stable fingerprint.
    key['current']={who:packed(f.peer) for who,f in current.items()}
    fingerprint=hashlib.sha256(json.dumps(key,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return {'status':'reviewable_target_cleanup','fingerprint':fingerprint,'target_only':True}

def apply(controller,expected):
    p=proposal(controller)
    if p['status']=='already_clean':return p
    if not expected or p.get('fingerprint')!=expected:raise BackendError('recovery_fingerprint_changed')
    active=controller.store.data['active'];current=controller._read('coordinator' in active['checkpoint'])
    if proposal(controller).get('fingerprint')!=expected:raise BackendError('recovery_fingerprint_changed')
    controller.mode=active['mode'];controller.generation=active['generation'];controller.uri=active['uri']
    controller.expected=current;controller.failed=False
    # This is a new, explicitly requested cleanup plan. It never replays the
    # pending action or starts audio. The first fresh guard can still refuse it.
    try:controller._cleanup()
    except Exception as error:controller._fail(error);raise
    return {'status':'recovered','target_only':True}


def _idle_finalize_snapshot(controller):
    """Two coherent target reads under the process-held scope; no coordinator use."""
    first=controller._read(False)['target']
    if not first.peer.idle():return None
    controller.sleep(.1)
    second=controller._read(False)['target']
    if (not second.peer.idle() or second.peer!=first.peer
            or second.ownership()!=first.ownership()):
        raise BackendError('idle_finalize_target_changed')
    return second


def _idle_finalize_fingerprint(controller,target):
    # Bind every durable journal/config field and target field. Per-process
    # subscription revisions and observation timestamps cannot survive restart.
    key={'operation':'finalize_verified_idle_v1','journal':controller.store.data,
         'target':packed(target.peer)}
    return hashlib.sha256(json.dumps(key,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def finalize_proposal(controller):
    """Explicit local-only reconciliation after independently completed cleanup.

    This is intentionally separate from proposal/apply: the ordinary recovery
    path retains its intent/checkpoint/no-replay rules for every non-idle target.
    """
    controller.scope.assert_owned()
    if not controller.store.data.get('active'):
        return {'status':'already_clean','fingerprint':None,'speaker_writes':False}
    current=_idle_finalize_snapshot(controller)
    if current is None:
        return {'status':'idle_finalize_target_not_idle','fingerprint':None,
                'target_only':True,'speaker_writes':False}
    return {'status':'reviewable_idle_finalize',
            'fingerprint':_idle_finalize_fingerprint(controller,current),
            'target_only':True,'speaker_writes':False}


def finalize(controller,expected):
    """Acknowledge stable physical idle state; never replay or issue speaker writes."""
    before=finalize_proposal(controller)
    if before['status']=='already_clean':return before
    if (before['status']!='reviewable_idle_finalize' or not expected
            or before.get('fingerprint')!=expected):
        raise BackendError('idle_finalize_fingerprint_changed')
    controller.store.event('idle_finalize_intent',fingerprint=expected,
                           target_only=True,speaker_writes=False)
    # Re-read after the durable audit event, refusing changes to either the
    # journal or the physical target. A new preview is required on mismatch.
    current=finalize_proposal(controller)
    if current.get('fingerprint')!=expected:
        raise BackendError('idle_finalize_fingerprint_changed')
    controller.scope.assert_owned()
    controller.wave.revoke()
    controller.store.clean()
    controller.expected={};controller.mode='idle';controller.generation=None
    controller.uri=None;controller.volume_revision=0;controller.failed=False
    controller.store.event('idle_finalized',fingerprint=expected,
                           target_only=True,speaker_writes=False)
    return {'status':'finalized_idle','target_only':True,'speaker_writes':False}
