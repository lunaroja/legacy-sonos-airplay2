"""Diagnostic coverage for previously blank failures; no LAN or speaker writes."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
import ast,json,tempfile,time,unittest
import failure_diagnostics as D,gena_runtime as G,speaker_diagnostics as S,recovery
from controller import Controller
from runtime_state import ProductScope,Store,StateError
from sonos_api import BackendError,HTTP
from session import Session
SECRET='private-do-not-emit-capability-token'
T='RINCON_00000000000101400'

class FailureDiagnostics(unittest.TestCase):
    def test_all_current_literal_backend_codes_have_bounded_output(self):
        codes=set();dynamic=[]
        for path in Path(__file__).parent.glob('*.py'):
            if path.name.startswith('test_'):continue
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='BackendError':
                    if len(node.args)==1 and isinstance(node.args[0],ast.Constant) and isinstance(node.args[0].value,str):codes.add(node.args[0].value)
                    else:dynamic.append(path.name)
        self.assertEqual(dynamic,[]);self.assertTrue(codes);self.assertLessEqual(codes,D.BACKEND_REASONS)
        for code in codes:self.assertEqual(D.diagnostic_fields(BackendError(code))['controller_failure_reason'],code)
    def test_real_http_timeout_keeps_phase_and_safe_cause(self):
        conn=Mock();conn.request.side_effect=TimeoutError(SECRET)
        with patch('sonos_api.HTTPConnection',return_value=conn):
            try:HTTP().request('127.0.0.2',1400,'GET','/not-sent',None,{},time.monotonic()+2)
            except BackendError as error:failure=error
            else:self.fail('timeout should remain a failure')
        # This class/code was previously entirely absent from the GENA-only output.
        self.assertEqual(G.diagnostic_fields(failure),{})
        d=D.diagnostic_fields(failure);self.assertEqual(d['controller_exception_class'],'BackendError');self.assertEqual(d['controller_failure_reason'],'http_transport');self.assertEqual(d['speaker_transport_reason'],'timeout');self.assertEqual(d['speaker_http_phase'],'request');self.assertNotIn(SECRET,json.dumps(d))
    def test_deadline_before_http_try_block_still_has_class_and_reason(self):
        with patch('sonos_api.HTTPConnection') as conn:
            try:HTTP().request('127.0.0.2',1400,'GET','/not-sent',None,{},time.monotonic()-1)
            except BackendError as error:d=D.diagnostic_fields(error)
            else:self.fail('expired read should fail')
        conn.assert_not_called();self.assertEqual(d,dict(controller_exception_class='BackendError',controller_failure_reason='http_deadline'))
    def test_existing_speaker_read_and_readback_diagnostics_survive(self):
        error=BackendError('http_transport');error.read_diagnostics=dict(speaker_peer='coordinator',speaker_service='AVTransport',speaker_read_action='GetTransportInfo',speaker_http_status=503,raw=SECRET)
        error.http_diagnostics=dict(speaker_transport_reason='connection_reset',speaker_http_phase='response_body',url=SECRET)
        error.readback_diagnostics=dict(readback_failed_reads=2,readback_read_failures=[dict(speaker_peer='target',speaker_read_reason='http_deadline',raw=SECRET),dict(speaker_peer='coordinator',speaker_read_reason='soap_rejected')])
        expected=S.diagnostic_fields(error);result=D.diagnostic_fields(error)
        for key,value in expected.items():self.assertEqual(result[key],value)
        self.assertEqual(result['speaker_transport_reason'],'connection_reset');self.assertNotIn(SECRET,json.dumps(result))
    def test_event_error_is_distinct_and_remains_event_error(self):
        self.assertFalse(issubclass(G.EventError,BackendError));error=G.EventError('event_sequence_gap');error.peer='target';error.service='AVTransport';error.phase='notify'
        d=D.diagnostic_fields(error);self.assertEqual(d['controller_exception_class'],'EventError');self.assertEqual(d['gena_reason'],'event_sequence_gap');self.assertNotIn('controller_failure_reason',d)
    def test_peer_mismatch_flags_preserved_unchanged(self):
        error=BackendError('peer_evidence_changed');error.evidence_diagnostics=dict(gena_peer='target',gena_transport='STOPPED',soap_transport='PLAYING',gena_transport_matches=False,gena_uri_matches=True)
        expected=G.diagnostic_fields(error);d=D.diagnostic_fields(error)
        for key,value in expected.items():self.assertEqual(d[key],value)
        self.assertEqual(d['controller_exception_class'],'BackendError')
    def test_fixed_classes_never_include_messages_or_arbitrary_type_names(self):
        Custom=type(SECRET,(Exception,),{})
        for error,label in ((StateError(SECRET),'StateError'),(TimeoutError(SECRET),'TimeoutError'),(ValueError(SECRET),'ValueError'),(KeyError(SECRET),'KeyError'),(AttributeError(SECRET),'AttributeError'),(Custom(SECRET),'other')):
            d=D.diagnostic_fields(error);self.assertEqual(d,{'controller_exception_class':label});self.assertNotIn(SECRET,json.dumps(d))
    def test_unknown_or_multiargument_backend_reason_is_not_serialized(self):
        for error in (BackendError(SECRET),BackendError('http_transport',SECRET),BackendError()):
            self.assertEqual(D.diagnostic_fields(error),dict(controller_exception_class='BackendError',controller_failure_reason='unknown_backend_error'))
    def test_missing_error_is_visible_as_unspecified(self):
        self.assertEqual(D.diagnostic_fields(None),{'controller_exception_class':'unspecified'})
    def test_injected_http_and_speaker_fields_remain_rejected(self):
        error=BackendError(SECRET);error.http_diagnostics=dict(speaker_http_status=True,speaker_http_phase=SECRET,uri=SECRET);error.read_diagnostics=dict(speaker_peer=SECRET,speaker_read_reason=SECRET);error.readback_diagnostics=dict(readback_failed_reads=True,readback_read_failures=[{'raw':SECRET}]*3)
        d=D.diagnostic_fields(error);self.assertEqual(d,dict(controller_exception_class='BackendError',controller_failure_reason='unknown_backend_error'));self.assertNotIn(SECRET,json.dumps(d))
    def controller(self,error):
        temp=tempfile.TemporaryDirectory(dir=Path(__file__).parent);self.addCleanup(temp.cleanup);root=Path(temp.name)
        scope=ProductScope(root/'scope',(T,));self.addCleanup(scope.close);store=Store(root/'state','synthetic-diagnostics')
        peers=Mock();peers.read.side_effect=error;wave=Mock();session=Session('owned','present',True,None,0,0,100.,1)
        c=Controller(peers,lambda:session,wave,scope,store);c.mode='both';c.generation='owned';c._save('verified','SetVolume')
        return c,store,peers
    def test_midstream_http_deadline_records_reason_without_new_intent_or_write(self):
        c,store,peers=self.controller(BackendError('http_deadline'));self.assertEqual(c.tick(),'recovery');peers.write.assert_not_called()
        rows=[json.loads(x) for x in store.audit.read_text().splitlines()];self.assertEqual(len(rows),1);self.assertEqual(rows[0]['event'],'failed');self.assertEqual(rows[0]['controller_exception_class'],'BackendError');self.assertEqual(rows[0]['controller_failure_reason'],'http_deadline');self.assertEqual(store.data['active']['phase'],'verified');self.assertEqual(store.data['active']['action'],'SetVolume')
    def test_non_backend_midstream_exception_gets_safe_class(self):
        c,store,peers=self.controller(KeyError(SECRET));self.assertEqual(c.tick(),'recovery');peers.write.assert_not_called();row=json.loads(store.audit.read_text().splitlines()[-1]);self.assertEqual(row['controller_exception_class'],'KeyError');self.assertNotIn(SECRET,json.dumps(row))
    def test_recovery_cleanup_passes_the_original_exception_to_failure_recorder(self):
        error=BackendError('http_transport');error.http_diagnostics=dict(speaker_transport_reason='timeout');controller=Mock();controller.store.data={'active':dict(checkpoint={},mode='both',generation='owned',uri=None)};controller._read.return_value={};controller._cleanup.side_effect=error
        with patch.object(recovery,'proposal',return_value=dict(status='reviewable_target_cleanup',fingerprint='a'*64)):
            with self.assertRaises(BackendError) as raised:recovery.apply(controller,'a'*64)
        self.assertIs(raised.exception,error);controller._fail.assert_called_once_with(error)

if __name__=='__main__':unittest.main()
