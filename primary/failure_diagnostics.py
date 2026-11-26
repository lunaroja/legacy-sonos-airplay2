"""Terminal failure metadata: fixed classes/codes and existing safe diagnostics."""
from gena_runtime import EventError,PEER_FAILURE_CODES,TRANSPORT_ENUMS,diagnostic_fields as gena_fields
from runtime_state import StateError
from sonos_api import BackendError
from speaker_diagnostics import REASONS,safe_record,diagnostic_fields as speaker_fields

BACKEND_REASONS=PEER_FAILURE_CODES|REASONS|frozenset((
    'transport_state','topology_identity','target_only_write','write_arguments',
    'owned_native_end_checkpoint','owned_native_end_unproven','owned_native_end_changed','owned_native_end_action',
    'read_retry_checkpoint_changed','read_retry_exhausted','read_retry_local_changed',
    'owned_paused_release_unproven','owned_paused_release_changed','owned_paused_release_action',
    'owned_paused_release_checkpoint','owned_paused_release_local_end',
    'owned_native_hold_unproven','owned_native_hold_changed','owned_native_hold_checkpoint',
    'recovery_fingerprint_changed','idle_finalize_target_changed','idle_finalize_fingerprint_changed'))
# These labels are constants, never type(error).__name__ or a raw exception message.
EXCEPTION_CLASSES=((EventError,'EventError'),(BackendError,'BackendError'),
    (StateError,'StateError'),(TimeoutError,'TimeoutError'),
    (ConnectionRefusedError,'ConnectionRefusedError'),(ConnectionResetError,'ConnectionResetError'),
    (ConnectionAbortedError,'ConnectionAbortedError'),(BrokenPipeError,'BrokenPipeError'),
    (OSError,'OSError'),(KeyError,'KeyError'),(IndexError,'IndexError'),
    (AttributeError,'AttributeError'),(TypeError,'TypeError'),(ValueError,'ValueError'),
    (AssertionError,'AssertionError'),(NotImplementedError,'NotImplementedError'),
    (RecursionError,'RecursionError'),(OverflowError,'OverflowError'),
    (MemoryError,'MemoryError'),(RuntimeError,'RuntimeError'))

NATIVE_END_FLAGS=frozenset((
    'end_coordinator_boot_matches',
    'end_coordinator_group_matches',
    'end_coordinator_members_match',
    'end_coordinator_stopped',
    'end_coordinator_topology_revision_matches',
    'end_coordinator_uri_empty',
    'end_coordinator_uri_matches_origin',
    'end_coordinator_uri_revision_matches_origin',
    'end_coordinator_uuid_matches',
    'end_direct_checked',
    'end_direct_events_available',
    'end_direct_matches',
    'end_direct_previous_topology_matches',
    'end_direct_previous_topology_revision_matches',
    'end_direct_previous_uri_matches',
    'end_direct_previous_uri_revision_matches',
    'end_direct_snapshot_source_revision_matches',
    'end_direct_snapshot_topology_matches',
    'end_direct_snapshot_topology_revision_matches',
    'end_direct_snapshot_uri_matches',
    'end_direct_snapshot_uri_revision_matches',
    'end_direct_subscriptions_continuous',
    'end_direct_tokens_present',
    'end_direct_topology_revision_unchanged',
    'end_direct_topology_transition_matches',
    'end_direct_topology_unchanged',
    'end_direct_uri_revision_unchanged',
    'end_direct_uri_transition_matches',
    'end_direct_uri_unchanged',
    'end_local_generation_matches',
    'end_local_media',
    'end_local_snapshot_available',
    'end_origin_coordinator_matches',
    'end_origin_coordinator_members_match',
    'end_origin_coordinator_revision_continuous',
    'end_origin_coordinator_tokens_present',
    'end_origin_distinct_peers',
    'end_origin_keys_match',
    'end_origin_native_uri_valid',
    'end_origin_native_uuid_matches',
    'end_origin_target_coordinator_matches',
    'end_origin_target_members_match',
    'end_origin_target_revision_continuous',
    'end_origin_target_tokens_present',
    'end_origin_target_uri_matches',
))
NATIVE_END_STAGES=frozenset(('origin_shape','origin','coordinator_read','coordinator_state','coordinator_transition'))

def diagnostic_fields(error):
    # HTTP metadata may exist even when Wire did not attach a read-operation
    # record. Preserve it, then preserve the more contextual safe read record.
    result=safe_record(getattr(error,'http_diagnostics',None))
    result.update(speaker_fields(error))
    result.update(gena_fields(error))
    raw=getattr(error,'native_end_diagnostics',None)
    if type(raw)is dict:
        for key in NATIVE_END_FLAGS:
            if type(raw.get(key))is bool:result[key]=raw[key]
        for key,choices in (('end_proof_stage',NATIVE_END_STAGES),('end_coordinator_transport',TRANSPORT_ENUMS),
                            ('end_local_state',('present','ended','lost','unproven'))):
            value=raw.get(key)
            if type(value)is str and value in choices:result[key]=value
    result['controller_exception_class']=('unspecified' if error is None else
        next((label for cls,label in EXCEPTION_CLASSES if isinstance(error,cls)),'other'))
    if isinstance(error,BackendError):
        code=error.args[0] if len(error.args)==1 else None
        result['controller_failure_reason']=(code if type(code)is str and code in BACKEND_REASONS
                                             else 'unknown_backend_error')
    return result
