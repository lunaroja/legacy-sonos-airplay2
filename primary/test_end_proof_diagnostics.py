"""Constructor refusal diagnostics preserve decisions and never export values."""
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock
import ast,copy,json,unittest
from owned_native_end import OwnedNativeEnd
from sonos_api import BackendError
from failure_diagnostics import diagnostic_fields,NATIVE_END_FLAGS
import test_owned_native_end as N
from test_primary import T,C,NATIVE

SECRET='private-end-proof-value-not-for-output'

def prior_direct(events,who,before,after):
    """Frozen pre-diagnostic predicate, for decision parity only."""
    with events.lock:
        state=events.states[who];b,a=before.peer,after.peer;br,ar=before.revision,after.revision
        subs=[s for s in events.subscriptions if s.who==who];now=events.clock()
        if not subs or any(not s.sid or s.sequence is None or s.lost or s.deadline<=now for s in subs):return False
        bt=(b.coordinator,b.members,b.boot);at=(a.coordinator,a.members,a.boot)
        if (not br.uri_revision or not br.topology or not ar.uri_revision or not ar.topology
                or state['source'].get('AVTransportURI')!=a.uri or state['topology']!=at
                or state['uri_revision']!=ar.uri_revision or state['topology_revision']!=ar.topology
                or state['source_revision']!=ar.source):return False
        uri=(br.uri_revision==ar.uri_revision if b.uri==a.uri else
            state.get('previous_uri')==b.uri and state.get('previous_uri_revision')==br.uri_revision
            and br.uri_revision!=ar.uri_revision)
        topology=(br.topology==ar.topology if bt==at else
            state.get('previous_topology')==bt and state.get('previous_topology_revision')==br.topology
            and br.topology!=ar.topology)
        return bool(uri and topology)

class EndProofDiagnostics(unittest.TestCase):
    fixture=N.OwnedNativeEndTests.fixture
    end=N.OwnedNativeEndTests.end
    rows=N.OwnedNativeEndTests.rows
    writes=N.OwnedNativeEndTests.writes
    assert_no_cleanup=N.OwnedNativeEndTests.assert_no_cleanup
    def refusal(self,expected=None):
        with self.assertRaises(BackendError) as caught:
            OwnedNativeEnd(self.peers,self.c.expected if expected is None else expected)
        self.assertEqual(caught.exception.args,('owned_native_end_unproven',))
        fields=diagnostic_fields(caught.exception);self.assertNotIn(SECRET,json.dumps(fields))
        self.assert_no_cleanup();return fields
    def test_origin_shape_refusal_has_no_peer_read(self):
        self.fixture();self.peers.read=Mock(side_effect=AssertionError('No read expected'))
        d=self.refusal({'target':self.c.expected['target']});self.assertEqual(d['end_proof_stage'],'origin_shape')
        self.assertFalse(d['end_origin_keys_match']);self.peers.read.assert_not_called()
    def test_origin_native_uri_member_uri_and_revision_flags(self):
        self.fixture()
        for who,changes,key in (
            ('coordinator',{'peer':replace(self.c.expected['coordinator'].peer,uri=SECRET)},'end_origin_native_uri_valid'),
            ('target',{'peer':replace(self.c.expected['target'].peer,uri=SECRET)},'end_origin_target_uri_matches'),
            ('target',{'revision':replace(self.c.expected['target'].revision,continuous=False)},'end_origin_target_revision_continuous')):
            with self.subTest(key=key):
                expected=dict(self.c.expected);expected[who]=replace(expected[who],**changes)
                d=self.refusal(expected);self.assertEqual(d['end_proof_stage'],'origin');self.assertFalse(d[key])
    def test_coordinator_not_ended_distinguishes_owned_nonempty_uri_and_transport(self):
        self.fixture()
        for transport in ('PLAYING','STOPPED','PAUSED_PLAYBACK'):
            with self.subTest(transport=transport):
                self.model.update('coordinator',transport=transport);self.model.emit_all();d=self.refusal()
                self.assertEqual(d['end_proof_stage'],'coordinator_state');self.assertFalse(d['end_coordinator_uri_empty'])
                self.assertTrue(d['end_coordinator_uri_matches_origin']);self.assertTrue(d['end_coordinator_uri_revision_matches_origin']);self.assertEqual(d['end_coordinator_transport'],transport)
                self.assertEqual(d['end_coordinator_stopped'],transport=='STOPPED');self.assertFalse(d['end_direct_checked'])
    def test_coordinator_boot_or_group_refusal_is_precise(self):
        for kind,key in (('boot','end_coordinator_boot_matches'),('group','end_coordinator_members_match')):
            with self.subTest(kind=kind):
                self.doCleanups();self.fixture()
                if kind=='boot':self.model.update('coordinator',boot=99)
                else:
                    self.model.update('target',coordinator=T,members=frozenset((T,)))
                    self.model.update('coordinator',members=frozenset((C,)))
                self.end();d=self.refusal();self.assertEqual(d['end_proof_stage'],'coordinator_state')
                self.assertFalse(d[key]);self.assertTrue(d['end_coordinator_uri_empty']);self.assertFalse(d['end_direct_checked'])
    def test_foreign_and_aba_predecessor_refusals_are_distinct(self):
        for sequence,previous_matches in (((SECRET,''),False),((SECRET,NATIVE,''),True)):
            with self.subTest(previous_matches=previous_matches):
                self.doCleanups();self.fixture();self.model.end_mismatch=True
                for uri in sequence:self.model.update('coordinator',uri=uri,transport='PLAYING' if uri else 'STOPPED');self.model.emit_all()
                d=self.refusal();self.assertEqual(d['end_proof_stage'],'coordinator_transition')
                self.assertTrue(d['end_direct_checked']);self.assertTrue(d['end_direct_subscriptions_continuous'])
                self.assertEqual(d['end_direct_previous_uri_matches'],previous_matches)
                self.assertFalse(d['end_direct_previous_uri_revision_matches']);self.assertFalse(d['end_direct_uri_transition_matches'])
                self.assertFalse(d['end_direct_matches'])
    def after_read(self,change):
        original=self.peers.read
        def read(who):
            fact=original(who)
            if who=='coordinator':change()
            return fact
        self.peers.read=read
    def test_missing_events_at_transition_is_explicit(self):
        self.fixture();self.end();self.after_read(lambda:self.peers.events.pop('coordinator'))
        d=self.refusal();self.assertEqual(d['end_proof_stage'],'coordinator_transition')
        self.assertTrue(d['end_direct_checked']);self.assertFalse(d['end_direct_events_available'])
    def test_source_revision_race_is_distinct_from_bad_uri_predecessor(self):
        self.fixture();self.end()
        def change():
            for transport in ('PLAYING','STOPPED'):self.model.update('coordinator',transport=transport);self.model.emit_all()
        self.after_read(change);d=self.refusal();self.assertEqual(d['end_proof_stage'],'coordinator_transition')
        self.assertTrue(d['end_direct_snapshot_uri_matches']);self.assertTrue(d['end_direct_snapshot_uri_revision_matches'])
        self.assertFalse(d['end_direct_snapshot_source_revision_matches']);self.assertNotIn('end_direct_previous_uri_matches',d)
    def test_continuity_loss_between_read_and_transition_remains_refusal(self):
        self.fixture();self.end();self.after_read(lambda:setattr(self.peers.events['coordinator'].subscriptions[0],'lost',True))
        d=self.refusal();self.assertEqual(d['end_proof_stage'],'coordinator_transition')
        self.assertFalse(d['end_direct_subscriptions_continuous']);self.assertFalse(d['end_direct_matches'])
    def test_coordinator_read_error_keeps_original_class_reason_and_http_fields(self):
        self.fixture();error=BackendError('http_transport');error.http_diagnostics={'speaker_transport_reason':'timeout','speaker_http_phase':'response_headers'}
        self.peers.read=Mock(side_effect=error)
        with self.assertRaises(BackendError) as caught:OwnedNativeEnd(self.peers,self.c.expected)
        self.assertIs(caught.exception,error);d=diagnostic_fields(error)
        self.assertEqual(d['end_proof_stage'],'coordinator_read');self.assertEqual(d['controller_failure_reason'],'http_transport')
        self.assertEqual(d['speaker_transport_reason'],'timeout');self.assertEqual(d['speaker_http_phase'],'response_headers');self.assert_no_cleanup()
    def test_persistent_transport_skew_refuses_without_inventing_end_proof(self):
        c=self.fixture();self.model.end_mismatch=True;self.model.emit_all()
        self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup();row=self.rows()[-1]
        self.assertEqual(row['controller_failure_reason'],'peer_evidence_changed')
        self.assertEqual(row['gena_peer'],'target');self.assertEqual(row['gena_transport'],'STOPPED')
        self.assertEqual(row['soap_transport'],'PLAYING');self.assertNotIn('end_proof_stage',row)
        retries=[r for r in self.rows() if r['event']=='fresh_evidence_retry']
        self.assertEqual(len(retries),1);self.assertEqual(retries[0]['retry_kind'],'native_target_transport_observation')
    def test_local_end_hold_and_generation_are_fixed_refusal_metadata(self):
        for state,generation in (('ended','one'),('unproven','one'),('present','different-private-generation')):
            with self.subTest(state=state):
                self.doCleanups();c=self.fixture();self.model.end_mismatch=True;self.model.emit_all()
                self.session=replace(self.session,state=state,generation=generation,media=False)
                self.assertEqual(c.tick(),'recovery');self.assert_no_cleanup();d=self.rows()[-1]
                self.assertEqual(d['end_local_state'],state);self.assertTrue(d['end_local_snapshot_available'])
                self.assertEqual(d['end_local_generation_matches'],generation=='one');self.assertFalse(d['end_local_media'])
                self.assertNotIn('different-private-generation',json.dumps(d))
    def test_failed_local_diagnostic_read_cannot_replace_constructor_refusal(self):
        c=self.fixture();c.session=Mock(side_effect=RuntimeError(SECRET))
        with self.assertRaises(BackendError) as caught:c._begin_owned_end()
        self.assertEqual(caught.exception.args,('owned_native_end_unproven',))
        d=diagnostic_fields(caught.exception);self.assertEqual(d['end_proof_stage'],'coordinator_state')
        self.assertFalse(d['end_local_snapshot_available']);self.assertNotIn(SECRET,json.dumps(d));self.assert_no_cleanup()
    def test_retained_native_uri_with_changed_uri_revision_is_visible(self):
        self.fixture()
        for uri in (SECRET,NATIVE):self.model.update('coordinator',uri=uri);self.model.emit_all()
        d=self.refusal();self.assertEqual(d['end_proof_stage'],'coordinator_state')
        self.assertTrue(d['end_coordinator_uri_matches_origin']);self.assertFalse(d['end_coordinator_uri_revision_matches_origin'])
    def test_injected_values_are_rejected_at_output_boundary(self):
        error=BackendError('owned_native_end_unproven')
        error.native_end_diagnostics={key:SECRET for key in NATIVE_END_FLAGS}
        error.native_end_diagnostics.update(end_proof_stage=SECRET,end_coordinator_transport=SECRET,end_local_state=SECRET,raw_uri=SECRET,
            end_coordinator_uri_empty=1,end_direct_checked=True)
        d=diagnostic_fields(error);self.assertNotIn(SECRET,json.dumps(d))
        self.assertEqual({key:value for key,value in d.items() if key.startswith('end_')},{'end_direct_checked':True})
    def test_direct_decision_matches_frozen_predicate_across_refusal_cases(self):
        self.fixture();self.end();events=self.peers.events['coordinator'];before=self.c.expected['coordinator'];after=self.peers.read('coordinator')
        state=copy.deepcopy(events.states['coordinator']);subs=[copy.copy(s) for s in events.subscriptions]
        mutations=[lambda:None,
            lambda:events.states['coordinator']['source'].update(AVTransportURI=SECRET),
            lambda:events.states['coordinator'].update(topology=None),
            lambda:events.states['coordinator'].update(uri_revision=SECRET),
            lambda:events.states['coordinator'].update(topology_revision=SECRET),
            lambda:events.states['coordinator'].update(source_revision=SECRET),
            lambda:events.states['coordinator'].update(previous_uri=SECRET),
            lambda:events.states['coordinator'].update(previous_uri_revision=SECRET),
            lambda:setattr(events.subscriptions[0],'lost',True),
            lambda:setattr(events.subscriptions[0],'deadline',0),
            lambda:setattr(events.subscriptions[0],'sequence',None)]
        for mutate in mutations:
            events.states['coordinator']=copy.deepcopy(state);events.subscriptions=[copy.copy(s) for s in subs];mutate()
            want=prior_direct(events,'coordinator',before,after)
            result,fields=events.direct_transition_with_diagnostics('coordinator',before,after)
            self.assertIs(result,want);self.assertIs(events.direct_transition_matches('coordinator',before,after),want)
            self.assertTrue(all(type(v)is bool for v in fields.values()));self.assertNotIn(SECRET,json.dumps(fields))
    def test_diagnostic_snapshot_stays_bound_to_the_refusal(self):
        self.fixture();self.model.update('coordinator',uri=SECRET);self.model.emit_all();self.end()
        events=self.peers.events['coordinator'];original=events.direct_transition_with_diagnostics
        def change_after_snapshot(*args):
            answer=original(*args);events.states['coordinator']['previous_uri']=NATIVE
            events.states['coordinator']['previous_uri_revision']=self.c.expected['coordinator'].revision.uri_revision
            return answer
        events.direct_transition_with_diagnostics=change_after_snapshot;d=self.refusal()
        self.assertFalse(d['end_direct_previous_uri_matches']);self.assertFalse(d['end_direct_matches'])
    def test_all_emitted_fixed_boolean_fields_are_allowlisted(self):
        emitted=set()
        for name in ('owned_native_end.py','gena_runtime.py','controller.py'):
            for node in ast.walk(ast.parse((Path(__file__).parent/name).read_text())):
                if isinstance(node,ast.Constant) and isinstance(node.value,str) and node.value.startswith('end_'):emitted.add(node.value)
                if isinstance(node,ast.keyword) and node.arg and node.arg.startswith('end_'):emitted.add(node.arg)
        self.assertEqual(emitted-{'end_proof_stage','end_coordinator_transport','end_local_state'},set(NATIVE_END_FLAGS))

if __name__=='__main__':unittest.main()
