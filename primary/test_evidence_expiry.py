"""Fixed expiry age/availability/stage proof, without changing the two-second gate."""
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
import json,unittest
import test_peer_diagnostics as P
from controller import Controller
from failure_diagnostics import diagnostic_fields
from sonos_api import BackendError

class ExpiryDiagnostics(unittest.TestCase):
    setUp=P.EvidenceDiagnostics.setUp
    fact=P.EvidenceDiagnostics.fact
    failure=P.EvidenceDiagnostics.failure
    def test_old_but_continuous_view_records_exact_bounded_age(self):
        f=self.fact();self.now+=2.2;d=diagnostic_fields(self.failure(f))
        self.assertEqual(d['controller_failure_reason'],'peer_evidence_expired');self.assertEqual(d['peer_evidence_age_ms'],2200);self.assertEqual(d['peer_evidence_expiry_cause'],'too_old');self.assertFalse(d['peer_evidence_age_clipped']);self.assertTrue(d['gena_events_available']);self.assertTrue(d['gena_snapshot_available']);self.assertTrue(d['gena_subscriptions_continuous']);self.assertTrue(d['gena_revision_matches']);self.assertEqual(d['gena_read_stage'],'caller_recheck')
    def test_missing_events_does_not_masquerade_as_age(self):
        f=self.fact();self.backend.events.clear();d=diagnostic_fields(self.failure(f));self.assertEqual(d['peer_evidence_expiry_cause'],'events_unavailable');self.assertEqual(d['peer_evidence_age_ms'],0);self.assertFalse(d['gena_events_available']);self.assertFalse(d['gena_snapshot_available']);self.assertNotIn('gena_subscriptions_continuous',d)
    def test_two_seconds_still_passes_and_any_older_read_fails(self):
        f=self.fact();self.now+=2;self.assertIs(self.backend.recheck('target',f),f);self.now+=.001
        self.assertEqual(diagnostic_fields(self.failure(f))['peer_evidence_age_ms'],2001)
    def test_negative_nonfinite_and_large_ages_are_bounded(self):
        for age,cause,expected in ((-.25,'clock_reversed',-250),(120,'too_old',60000),(float('nan'),'age_nonfinite',None),(float('inf'),'age_nonfinite',None)):
            with self.subTest(cause=cause,expected=expected):
                f=self.fact();f=replace(f,observed=self.now-age);d=diagnostic_fields(self.failure(f));self.assertEqual(d['peer_evidence_expiry_cause'],cause)
                if expected is None:self.assertNotIn('peer_evidence_age_ms',d)
                else:self.assertEqual(d['peer_evidence_age_ms'],expected)
                if age==120:self.assertTrue(d['peer_evidence_age_clipped'])
    def test_expired_transport_mismatch_is_not_labelled_coherent(self):
        f=self.fact();self.now+=2.2;self.events.states['target']['source']['TransportState']='STOPPED';d=diagnostic_fields(self.failure(f));self.assertFalse(d['gena_transport_matches']);self.assertTrue(d['gena_subscriptions_continuous']);self.assertFalse(d['gena_revision_matches'])
    def test_diagnostic_collection_failure_cannot_replace_original_refusal(self):
        f=self.fact();self.now+=2.2;self.events.revision_with_diagnostics=Mock(side_effect=RuntimeError(P.SECRET));error=self.failure(f);self.assertEqual(str(error),'peer_evidence_expired');d=diagnostic_fields(error);self.assertFalse(d['gena_snapshot_available']);self.assertNotIn(P.SECRET,json.dumps(d))
    def test_individual_peer_read_expiry_is_labelled(self):
        original=self.backend.wire.read
        def slow(who):self.now+=2.2;return self.peer
        self.backend.wire.read.side_effect=slow
        with self.assertRaises(BackendError) as e:self.backend.read('target')
        d=diagnostic_fields(e.exception);self.assertEqual(d['gena_read_stage'],'peer_read');self.assertEqual(d['peer_evidence_age_ms'],2200)
    def test_controller_group_and_single_rechecks_get_distinct_stages(self):
        for group,stage in ((True,'group_recheck'),(False,'target_recheck')):
            f=self.fact();peers=Mock();peers.read.return_value=f;error=BackendError('peer_evidence_expired');error.evidence_diagnostics={'gena_peer':'target','gena_read_stage':'caller_recheck'};peers.recheck.side_effect=error
            c=Controller(peers,Mock(),Mock(),SimpleNamespace(assert_owned=Mock()),SimpleNamespace(data={}))
            with self.assertRaises(BackendError) as e:c._read(group)
            self.assertIs(e.exception,error);self.assertEqual(diagnostic_fields(error)['gena_read_stage'],stage);peers.write.assert_not_called()
    def test_numeric_and_enum_output_injection_is_rejected(self):
        error=BackendError('peer_evidence_expired')
        for age in (True,60001,-60001,'2 seconds'):
            error.evidence_diagnostics={'peer_evidence_age_ms':age,'peer_evidence_expiry_cause':P.SECRET,'gena_read_stage':P.SECRET,'gena_events_available':1}
            d=diagnostic_fields(error);self.assertNotIn('peer_evidence_age_ms',d);self.assertNotIn('peer_evidence_expiry_cause',d);self.assertNotIn('gena_read_stage',d);self.assertNotIn('gena_events_available',d);self.assertNotIn(P.SECRET,json.dumps(d))

if __name__=='__main__':unittest.main()
