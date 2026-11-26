"""Recovery waits for real GENA snapshots from simulated speakers. No listener or receiver."""
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
import io,json,tempfile,unittest

import office_primary,peer_runtime,recovery
from configuration import digest
from failure_diagnostics import diagnostic_fields
from gena_runtime import EventError
from peer_runtime import Peers
from runtime_state import ProductScope,StateError,Store
from sonos_api import BackendError
from test_wire import SOAPModel,event_body

class RecoveryReadiness(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        root=Path(temp.name)
        self.config=json.loads((Path(__file__).parent/'config.group.example.json').read_text())
        self.config.update(state_dir=str(root/'state'),scope_dir=str(root/'scope'))
        self.store=Store(self.config['state_dir'],digest(self.config))
        self.active={'phase':'intent','mode':'both','checkpoint':{'coordinator':{}}}
        self.store.data['active']=self.active;self.store.persist()
        self.now=100.;self.model=SOAPModel();self.created=[];self.on_wait=None
        self.runtime=office_primary.Runtime(self.config)
        self.addCleanup(self.runtime.close,False)
        self.held=set();self.real_emit=self.model.emit
        def emit(sub):
            if (sub.who,sub.service) not in self.held:self.real_emit(sub)
        self.model.emit=emit
        self.wire_roles=[]
        request=self.model.request
        def record(ip,port,method,*args,**kwargs):
            self.wire_roles.append(('target' if ip==self.config['target']['ip'] else 'coordinator',method))
            return request(ip,port,method,*args,**kwargs)
        self.model.request=record
        def make_peers(config,**kwargs):
            peers=Peers(config,clock=lambda:self.now,**kwargs)
            peers.wire.http=self.model
            create=peers._events
            def make_events(who):
                self.created.append(who)
                events=create(who);events.clock=lambda:self.now
                events.http=self.model;self.model.events[who]=events
                def start():
                    for sub in events.subscriptions:events.subscribe(sub)
                events.start=start
                return events
            peers._events=make_events
            return peers
        self.patch(office_primary,'Peers',side_effect=make_peers)
        self.patch(office_primary,'Receiver',side_effect=AssertionError('recovery started receiver'))
        self.patch(office_primary.Wave,'start',side_effect=AssertionError('recovery opened WAV listener'))
        self.patch(peer_runtime.time,'sleep',side_effect=self.advance)
        self.thread=self.patch(peer_runtime.threading,'Thread')
    def patch(self,obj,key,**kwargs):
        p=patch.object(obj,key,**kwargs);instance=p.start();self.addCleanup(p.stop);return instance
    def advance(self,n):
        self.now+=n
        if self.on_wait:self.on_wait()
    def deliver(self,who,service):
        self.held.discard((who,service))
        event=self.model.events[who]
        sub=next(s for s in event.subscriptions if s.service==service)
        self.real_emit(sub)
        return sub
    def start(self):
        self.runtime.start(with_receiver=False,target_only=True)
        self.runtime.controller.sleep=self.advance
        return self.runtime.controller
    def test_delayed_initial_topology_then_two_fresh_reads_scope_digest_and_target_only_apply(self):
        saved_digest=digest(self.config);both={self.config[w]['uuid'] for w in ('target','coordinator')}
        self.model.update('coordinator',uri='http://synthetic.invalid/foreign',transport='PLAYING')
        coordinator_before=self.model.peers['coordinator'];self.held.add(('target','ZoneGroupTopology'))
        def after_delay():
            self.assertFalse(self.model.all_calls) # readiness does no SOAP polling
            if self.now<100.19:return
            self.assertEqual(self.runtime.scope.peers,both)
            with self.assertRaisesRegex(StateError,'another_product_instance'):
                ProductScope(self.config['scope_dir'],[self.config['coordinator']['uuid']])
            self.assertEqual(self.model.events['target'].pending_initial('target'),'ZoneGroupTopology')
            sub=self.deliver('target','ZoneGroupTopology');self.assertEqual(sub.sequence,0)
            self.on_wait=None
        self.on_wait=after_delay
        c=self.start();self.assertGreaterEqual(self.now,100.19)
        self.assertEqual(self.runtime.store.config_digest,saved_digest)
        self.assertEqual(self.runtime.store.data['config'],digest(self.config))
        self.assertEqual(self.model.events['target'].states['target']['source']['AVTransportURI'],'')
        self.assertEqual(self.model.events['target'].subscriptions[0].sequence,0)
        result=recovery.finalize_proposal(c);self.assertEqual(result['status'],'reviewable_idle_finalize')
        self.assertEqual(sum(action=='GetMediaInfo' for _,action,_ in self.model.all_calls),2)
        self.assertEqual(recovery.finalize(c,result['fingerprint'])['status'],'finalized_idle')
        self.assertTrue(self.runtime.close(cleanup=False))
        self.assertEqual({who for who,_ in self.wire_roles},{'target'})
        self.assertTrue(all(action.startswith('Get') for _,action,_ in self.model.all_calls))
        self.assertEqual(self.model.peers['coordinator'],coordinator_before)
        self.assertEqual(self.created,['target']);self.thread.assert_not_called()
    def test_delayed_source_can_follow_topology_sequence_zero(self):
        self.held.add(('target','AVTransport'))
        def after_delay():
            self.assertEqual(self.model.events['target'].pending_initial('target'),'AVTransport')
            self.deliver('target','AVTransport');self.on_wait=None
        self.on_wait=after_delay
        self.start();self.assertGreater(self.now,100);self.assertEqual(self.created,['target'])
    def test_sequence_zero_without_semantic_fields_waits_for_followup(self):
        self.held.add(('target','AVTransport'))
        def initialize():
            e=self.model.events['target'];sub=e.subscriptions[0]
            e.notify(sub.path,sub.ip,{'nt':'upnp:event','nts':'upnp:propchange','seq':'0','sid':sub.sid},event_body('LastChange','<Event><InstanceID val="0"><TransportState val="STOPPED"/></InstanceID></Event>'))
            self.model.subs['target','AVTransport']=(sub.sid,1)
            self.assertEqual(sub.sequence,0);self.assertEqual(e.pending_initial('target'),'AVTransport')
            def complete():
                self.deliver('target','AVTransport');self.on_wait=None
            self.on_wait=complete
        self.on_wait=initialize
        self.start();self.assertEqual(self.model.events['target'].subscriptions[0].sequence,1)
    def test_final_snapshot_mismatch_after_readiness_immediately_refused(self):
        c=self.start()
        self.model.update('target',uri='http://synthetic.invalid/foreign')
        with self.assertRaises(BackendError) as exc:recovery.finalize_proposal(c)
        fields=diagnostic_fields(exc.exception)
        self.assertEqual(fields['controller_failure_reason'],'peer_evidence_changed')
        self.assertFalse(fields['gena_uri_matches'])
        self.assertIsNotNone(self.runtime.store.data['active'])
        self.assertTrue(all(action.startswith('Get') for _,action,_ in self.model.all_calls))
    def test_initial_loss_and_expiry_are_terminal_before_five_seconds(self):
        for mode,code in (('lost','event_sequence_gap'),('expired','event_subscription_expired')):
            with self.subTest(mode=mode):
                self.held={('target','ZoneGroupTopology')}
                def damage():
                    event=self.model.events['target'];sub=event.subscriptions[0]
                    if mode=='lost':
                        sid,seq=self.model.subs['target','AVTransport']
                        self.model.subs['target','AVTransport']=(sid,seq+1)
                        with self.assertRaises(EventError):self.real_emit(sub)
                    else:sub.deadline=self.now
                    self.on_wait=None
                self.on_wait=damage;before=self.now
                with self.assertRaises(EventError) as exc:self.start()
                self.assertEqual(diagnostic_fields(exc.exception)['gena_reason'],code)
                self.assertLess(self.now-before,.1);self.assertFalse(self.model.all_calls)
                self.assertEqual(self.runtime.store.data['active'],self.active)
                self.runtime.close(False)
                self.runtime=office_primary.Runtime(self.config);self.addCleanup(self.runtime.close,False)
    def test_full_cli_timeout_names_only_expected_peer_service_and_preserves_journal(self):
        self.held.add(('target','ZoneGroupTopology'))
        with patch.object(office_primary,'load',return_value=self.config),patch.object(office_primary,'Runtime',return_value=self.runtime),patch.object(office_primary.signal,'signal'),redirect_stdout(io.StringIO()) as output:
            code=office_primary.main(['--config','unused','recover','--finalize-idle','--apply','0'*64])
        report=json.loads(output.getvalue())
        self.assertEqual(code,2);self.assertEqual(self.now,105)
        self.assertEqual((report['gena_reason'],report['gena_peer'],report['gena_service']),('event_initial_timeout','target','ZoneGroupTopology'))
        self.assertEqual((report['error_type'],report['controller_exception_class']),('EventError','EventError'))
        self.assertEqual(self.runtime.store.data['active'],self.active)
        self.assertFalse(self.model.all_calls);self.thread.assert_not_called()
        for value in ('uuid:',self.config['target']['ip'],self.config['target']['uuid'],'/event/'):
            self.assertNotIn(value,output.getvalue())
    def test_required_coordinator_shares_deadline_and_absence_never_triggers_reads(self):
        self.store.data['active']={**self.active,'phase':'verified'};self.store.persist()
        self.held.add(('target','ZoneGroupTopology'))
        def late_target():
            if self.now>104.5:
                self.deliver('target','ZoneGroupTopology');self.on_wait=None
        self.on_wait=late_target
        with self.assertRaises(EventError) as exc:self.runtime.start(with_receiver=False)
        self.assertEqual(self.now,105);self.assertEqual(diagnostic_fields(exc.exception)['gena_peer'],'coordinator')
        self.thread.return_value.start.assert_called_once();self.assertFalse(self.model.all_calls)
    def test_normal_group_recovery_waits_for_coordinator_semantics_as_well_as_registration(self):
        self.store.data['active']={**self.active,'phase':'verified'};self.store.persist()
        self.held.add(('coordinator','AVTransport'))
        def register():
            peers=self.runtime.peers
            if 'coordinator' not in peers.events:
                event=peers._events('coordinator');event.start();peers.events['coordinator']=event
            elif self.now>100.12:
                self.deliver('coordinator','AVTransport');self.on_wait=None
        self.on_wait=register
        self.runtime.start(with_receiver=False)
        self.assertGreater(self.now,100.12);self.assertFalse(self.model.all_calls)
        self.assertEqual(self.created,['target','coordinator'])
    def test_independent_clean_and_manual_review_need_no_notifications(self):
        for active in (None,self.active):
            with self.subTest(active=bool(active)):
                self.held={('target','AVTransport'),('target','ZoneGroupTopology')}
                self.store.data['active']=active;self.store.persist()
                self.runtime.start(with_receiver=False)
                result=recovery.proposal(self.runtime.controller)
                self.assertEqual(result['status'],'uncertain_write_requires_manual_review' if active else 'already_clean')
                self.assertEqual(self.now,100);self.assertFalse(self.model.all_calls)
                self.runtime.close(False)
                self.runtime=office_primary.Runtime(self.config);self.addCleanup(self.runtime.close,False)
    def test_unknown_cli_exception_never_discloses_message(self):
        with patch.object(office_primary,'load',side_effect=RuntimeError('synthetic-private-contents')),redirect_stdout(io.StringIO()) as output:
            self.assertEqual(office_primary.main(['--config','unused','recover']),2)
        self.assertEqual(json.loads(output.getvalue())['controller_exception_class'],'RuntimeError')
        self.assertNotIn('synthetic-private-contents',output.getvalue())

    def assert_signalled_apply_refused(self,*,finalize):
        active={**self.active,'phase':'verified','mode':'solo','checkpoint':{'target':{}}}
        self.store.data['active']=active;self.store.persist()
        before=self.store.path.read_bytes()
        self.held.add(('target','ZoneGroupTopology'))
        handlers={}
        def interrupt_then_finish():
            if not self.runtime.done.is_set():
                handlers[office_primary.signal.SIGTERM](office_primary.signal.SIGTERM,None)
            elif self.now>=100.19:
                self.deliver('target','ZoneGroupTopology');self.on_wait=None
        self.on_wait=interrupt_then_finish
        action='finalize' if finalize else 'apply'
        argv=['--config','unused','recover','--apply','0'*64]+(['--finalize-idle'] if finalize else [])
        with patch.object(office_primary,'load',return_value=self.config),patch.object(office_primary,'Runtime',return_value=self.runtime),patch.object(office_primary.signal,'signal',side_effect=handlers.__setitem__),patch.object(recovery,action,wraps=getattr(recovery,action)) as apply,redirect_stdout(io.StringIO()) as output:
            self.assertEqual(office_primary.main(argv),2)
        self.assertEqual(json.loads(output.getvalue())['reason'],'recovery_start_cancelled')
        apply.assert_not_called()
        self.assertTrue(self.runtime.done.is_set());self.assertFalse(self.model.all_calls)
        self.assertTrue(all(method in ('SUBSCRIBE','UNSUBSCRIBE') for _,method in self.wire_roles))
        self.assertEqual(self.store.path.read_bytes(),before);self.assertFalse(self.store.audit.exists())
        self.assertTrue(self.runtime.scope.closed)
        other=ProductScope(self.config['scope_dir'],self.runtime.scope.peers);other.close()
    def test_sigterm_during_initial_wait_refuses_ordinary_recovery_apply(self):
        self.assert_signalled_apply_refused(finalize=False)
    def test_sigterm_during_initial_wait_refuses_idle_finalization_apply(self):
        self.assert_signalled_apply_refused(finalize=True)
