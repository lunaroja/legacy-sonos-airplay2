"""Fixed evidence diagnostics preserve failure semantics and exclude capabilities."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import json,tempfile,threading,unittest
from controller import Controller
from gena_runtime import Events,EventError,diagnostic_fields
from peer_runtime import Peers
from runtime_state import ProductScope,Store
from sonos_api import Endpoint,Peer,Fact,BackendError
from session import Session
T='RINCON_00000000000101400';C='RINCON_00000000000201400'
SECRET='private-marker-not-permitted-in-output'

class EvidenceDiagnostics(unittest.TestCase):
    def setUp(self):
        self.now=100.;self.peer=Peer(T,'x-rincon:'+C,'PLAYING',C,frozenset((T,C)),17,33,False)
        self.events=Events(SimpleNamespace(target=Endpoint('127.0.0.2',T),coordinator=None),{'target':1400},'127.0.0.1',0,clock=lambda:self.now)
        for sub in self.events.subscriptions:sub.sid='uuid:'+SECRET;sub.sequence=0;sub.deadline=300.;sub.renew_at=200.
        self.events.states['target'].update(source={'AVTransportURI':self.peer.uri,'TransportState':'PLAYING'},topology=(C,frozenset((T,C)),17),source_revision='private-source-revision',topology_revision='private-topology-revision',uri_revision='private-uri-revision')
        self.backend=object.__new__(Peers);self.backend.events={'target':self.events};self.backend.clock=lambda:self.now;self.backend.lock=threading.RLock()
        self.backend.wire=Mock();self.backend.wire.read.return_value=self.peer;self.backend.write=Mock(side_effect=AssertionError('speaker write forbidden'))
    def fact(self):return Fact(self.peer,self.events.revision('target',self.peer,self.now),self.now)
    def failure(self,f=None):
        try:self.backend.recheck('target',f or self.fact())
        except BackendError as error:return error
        self.fail('Existing read refusal must remain in effect')
    def test_native_member_transport_disagreement_is_reported_and_still_refused(self):
        self.events.states['target']['source']['TransportState']='STOPPED'
        for _ in range(3):
            error=self.failure();self.assertEqual(str(error),'peer_evidence_changed');d=diagnostic_fields(error)
            self.assertTrue(d['gena_subscriptions_continuous']);self.assertTrue(d['gena_uri_matches']);self.assertTrue(d['gena_topology_matches']);self.assertFalse(d['gena_transport_matches'])
            self.assertEqual((d['gena_transport'],d['soap_transport']),('STOPPED','PLAYING'))
            self.assertEqual(d['controller_failure_reason'],'peer_evidence_changed')
        self.backend.write.assert_not_called()
    def test_correctly_coherent_read_still_passes_without_error(self):
        f=self.fact();self.assertIs(self.backend.recheck('target',f),f)
    def test_lost_or_expired_subscriptions_remain_distinct_refusals(self):
        for kind in ('lost','expired'):
            with self.subTest(kind=kind):
                sub=self.events.subscriptions[0];sub.lost=kind=='lost';sub.deadline=90. if kind=='expired' else 300.
                d=diagnostic_fields(self.failure());self.assertFalse(d['gena_subscriptions_continuous']);self.assertEqual(d['gena_subscription_lost'],kind=='lost');self.assertEqual(d['gena_subscriptions_unexpired'],kind!='expired');self.assertTrue(d['gena_uri_matches']);self.assertTrue(d['gena_transport_matches'])
    def test_missing_initial_event_is_not_mislabeled_as_transport_exception(self):
        self.events.subscriptions[0].sequence=None;d=diagnostic_fields(self.failure())
        self.assertFalse(d['gena_subscriptions_initialized']);self.assertFalse(d['gena_subscriptions_continuous']);self.assertTrue(d['gena_transport_matches'])
    def test_foreign_uri_and_topology_disagreements_still_refuse(self):
        self.events.states['target']['source']['AVTransportURI']='http://'+SECRET+'/capability'
        self.events.states['target']['topology']=(C,frozenset((C,)),18)
        d=diagnostic_fields(self.failure());self.assertFalse(d['gena_uri_matches']);self.assertFalse(d['gena_topology_matches']);self.assertNotIn(SECRET,json.dumps(d))
    def test_revision_change_records_only_boolean_comparisons(self):
        f=self.fact();self.events.states['target']['uri_revision']='new-'+SECRET
        d=diagnostic_fields(self.failure(f));self.assertFalse(d['gena_revision_matches']);self.assertFalse(d['gena_uri_revision_matches']);self.assertTrue(d['gena_topology_revision_matches']);self.assertNotIn(SECRET,json.dumps(d));self.assertNotIn('private-',json.dumps(d))
    def test_diagnostics_stay_bound_to_the_failed_locked_snapshot(self):
        self.events.states['target']['source']['TransportState']='STOPPED';f=self.fact();original=self.events.revision_with_diagnostics
        def changed_after_snapshot(*args):
            result=original(*args);self.events.states['target']['source']['TransportState']='PLAYING';return result
        self.events.revision_with_diagnostics=changed_after_snapshot
        d=diagnostic_fields(self.failure(f));self.assertEqual(d['gena_transport'],'STOPPED');self.assertFalse(d['gena_transport_matches']);self.assertEqual(self.events.states['target']['source']['TransportState'],'PLAYING')
    def test_unknown_or_missing_transport_is_bounded_without_raw_content(self):
        for value,expected in ((SECRET,'other'),(None,'missing')):
            self.events.states['target']['source']['TransportState']=value;d=diagnostic_fields(self.failure())
            self.assertEqual(d['gena_transport'],expected);self.assertNotIn(SECRET,json.dumps(d))
    def test_output_boundary_rejects_injected_fields_and_wrong_types(self):
        error=BackendError(SECRET);error.evidence_diagnostics={'uri':SECRET,'sid':SECRET,'callback':SECRET,'gena_uri_matches':1,'gena_transport_matches':'false','gena_transport':SECRET,'soap_transport':SECRET,'gena_peer':SECRET,'gena_topology_matches':True}
        d=diagnostic_fields(error);self.assertEqual(d,{'gena_topology_matches':True});self.assertNotIn(SECRET,json.dumps(d))
    def test_existing_event_error_diagnostics_remain_allowlisted(self):
        error=EventError('event_sequence_gap');error.peer='target';error.service='AVTransport';error.phase='notify';error.raw=SECRET
        self.assertEqual(diagnostic_fields(error),dict(gena_reason='event_sequence_gap',gena_peer='target',gena_service='AVTransport',gena_phase='notify'))
    def controller(self):
        tmp=tempfile.TemporaryDirectory(dir=Path(__file__).parent);self.addCleanup(tmp.cleanup);root=Path(tmp.name)
        scope=ProductScope(root/'scope',(T,C));self.addCleanup(scope.close);store=Store(root/'state','synthetic-diagnostics');wave=Mock()
        session=Session('one','ended',False,None,0,0,self.now,1)
        c=Controller(self.backend,lambda:session,wave,scope,store);c.mode='both';c.generation='one'
        target=self.fact();coordinator=replace(self.peer,uuid=C,uri='native-owned-source',coordinator=C)
        c.expected={'target':target,'coordinator':Fact(coordinator,target.revision,self.now)};c._save('verified','SetVolume')
        self.events.states['target']['source']['TransportState']='STOPPED'
        return c,store
    def test_terminal_tick_records_fixed_fields_without_cleanup_intent(self):
        c,store=self.controller();self.assertEqual(c.tick(),'recovery');self.backend.write.assert_not_called()
        rows=[json.loads(x) for x in store.audit.read_text().splitlines()];self.assertEqual(len(rows),1);row=rows[0]
        self.assertEqual((row['event'],row['reason']),('failed','uncertain_operation_no_replay'));self.assertEqual(row['gena_transport'],'STOPPED');self.assertEqual(row['soap_transport'],'PLAYING');self.assertFalse(row['gena_transport_matches']);self.assertNotIn(SECRET,json.dumps(rows));self.assertNotIn('x-rincon',json.dumps(rows));self.assertEqual(store.data['active']['phase'],'verified');self.assertEqual(store.data['active']['action'],'SetVolume')
    def test_shutdown_failure_records_same_fields_and_retains_journal(self):
        c,store=self.controller();self.assertFalse(c.shutdown());self.backend.write.assert_not_called()
        row=json.loads(store.audit.read_text().splitlines()[-1]);self.assertEqual(row['controller_failure_reason'],'peer_evidence_changed');self.assertFalse(row['gena_transport_matches']);self.assertIsNotNone(store.data['active'])
    def test_non_gena_owned_source_failure_gets_fixed_reason_only(self):
        c,store=self.controller();c._fail(BackendError('owned_source_changed'));row=json.loads(store.audit.read_text().splitlines()[-1])
        self.assertEqual(row['controller_failure_reason'],'owned_source_changed');self.assertNotIn('gena_transport',row)

if __name__=='__main__':unittest.main()
