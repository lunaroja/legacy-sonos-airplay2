"""Regression: idle for two days, renew continuity, fail closed on owned restart."""
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
import io,json,tempfile,time,unittest
import office_primary
from office_primary import Runtime
from runtime_state import Store
from configuration import digest,load
from gena_runtime import EventError
import test_wire as wire
class IdleRenewal(unittest.TestCase):
 fixture=wire.WireIntegration.fixture
 def clock(self):
  self.fixture();self.now=time.monotonic()
  for e in self.peers.events.values():e.clock=lambda:self.now
 def test_without_maintenance_exact_observed_expiry(self):
  self.clock();self.now+=301
  with self.assertRaises(EventError)as e:self.peers.maintain_target()
  self.assertEqual(e.exception.code,'event_subscription_expired');self.assertEqual(self.model.all_calls,[])
 def test_runtime_renews_two_days_without_media_reads_or_writes(self):
  self.clock();r=Runtime({});r.peers=self.peers;r.receiver_failed=lambda:False;calls=[]
  r.wake=SimpleNamespace(wait=lambda n:setattr(self,'now',self.now+100),clear=lambda:None)
  def tick():
   calls.append(True)
   if len(calls)==1728:r.done.set()
   return 'idle'
  r.controller=SimpleNamespace(tick=tick,failed=False,_fail=Mock());old=[s.sid for s in self.peers.events['target'].subscriptions]
  with redirect_stdout(io.StringIO()):self.assertEqual(r.run(),0)
  r.controller._fail.assert_not_called()
  for sub,sid in zip(self.peers.events['target'].subscriptions,old):
   self.assertEqual(sub.sid,sid);self.assertFalse(sub.lost);self.assertGreater(sub.deadline,self.now)
  self.assertEqual(self.model.all_calls,[])
 def test_loss_prevents_controller_entry_or_write(self):
  self.clock();r=Runtime({});r.peers=self.peers;r.receiver_failed=lambda:False;r.wake=SimpleNamespace(wait=lambda n:None,clear=lambda:None)
  self.peers.events['target'].subscriptions[0].lost=True;r.controller=SimpleNamespace(tick=Mock(),_fail=Mock())
  self.assertEqual(r.run(),2);r.controller.tick.assert_not_called();r.controller._fail.assert_called_once();self.assertEqual(self.model.all_calls,[])
class RestartGate(unittest.TestCase):
 def setUp(self):
  t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);p=Path(t.name);self.cfg=json.loads(Path('config.group.example.json').read_text());self.cfg.update(state_dir=str(p/'state'),scope_dir=str(p/'scope'))
  self.path=p/'config.json';self.path.write_text(json.dumps(self.cfg));self.path.chmod(0o600);self.store=Store(self.cfg['state_dir'],digest(load(self.path)))
 def gate(self):
  with patch.object(office_primary,'Runtime')as r,redirect_stdout(io.StringIO()):
   result=office_primary.main(['--config',str(self.path),'can-start']);r.assert_not_called();return result
 def test_idle_or_failed_without_any_command_can_restart(self):
  for status in('idle','recovery'):
   self.store.data.update(status=status,active=None);self.store.persist();self.assertEqual(self.gate(),0)
 def test_incomplete_active_is_refused(self):
  for active in({'phase':'verified','mode':'both'},{'phase':'intent','mode':'solo'},{}):
   self.store.data.update(status='recovery',active=active);self.store.persist();old=self.store.path.read_bytes();self.assertEqual(self.gate(),1);self.assertEqual(self.store.path.read_bytes(),old)
 def test_recognized_pending_join_can_start_read_only_recovery(self):
  active={'phase':'intent','mode':'both','action':'SetAVTransportURI',
          'generation':'test-session','checkpoint':{'target':{'peer':{}},'coordinator':{'peer':{}}}}
  for status in ('both','recovery'):
   self.store.data.update(status=status,active=active);self.store.persist()
   old=self.store.path.read_bytes();self.assertEqual(self.gate(),0);self.assertEqual(self.store.path.read_bytes(),old)
 def test_unknown_status_is_not_authorized(self):
  self.store.data['status']='unknown';self.store.persist();self.assertEqual(self.gate(),1)
 def test_configuration_mismatch_still_refuses(self):
  self.cfg['max_volume']=50;self.path.write_text(json.dumps(self.cfg));self.assertEqual(self.gate(),2)
 def test_systemd_restart_is_bounded_and_conditioned(self):
  text=Path('office-primary.service').read_text()
  for expected in('Restart=on-failure','RestartSec=5','StartLimitBurst=3','StartLimitIntervalSec=60','ExecCondition=','config.json can-start'):self.assertIn(expected,text)
