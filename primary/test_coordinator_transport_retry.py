"""Reproduce a real track-skip GENA/SOAP transport disagreement without LAN calls."""
import copy,unittest
from dataclasses import replace
from unittest.mock import Mock
import test_transport_race_retry as R
from controller import packed
from sonos_api import BackendError

class CoordinatorTransportRetry(unittest.TestCase):
 fixture=R.TransportRaceRetry.fixture;fake_clock=R.TransportRaceRetry.fake_clock
 rows=R.TransportRaceRetry.rows;writes=R.TransportRaceRetry.writes
 def error(self,**changes):
  fields=dict(gena_peer='coordinator',gena_read_stage='peer_read',gena_transport='PAUSED_PLAYBACK',soap_transport='PLAYING',gena_source_revision_matches=True,gena_revision_matches=True)
  fields.update(changes);return R.race_error(**fields)
 def setup_race(self,persistent=False):
  c=self.fixture();self.advance=self.fake_clock();original=self.peers.wire.read;seen=[False]
  self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
  def read(who):
   fact=original(who)
   if who=='coordinator' and (persistent or not seen[0]):
    seen[0]=True;return replace(fact,transport='PLAYING')
   return fact
  self.peers.wire.read=read
  def settle(n):
   self.assertEqual(self.writes(),[]);self.advance(n)
   if not persistent:self.model.update('coordinator',transport='PLAYING');self.model.emit_all()
  c.sleep=settle;return c
 def test_real_serializers_reproduce_skip_race_and_strict_recheck_preserves_checkpoint(self):
  c=self.setup_race();old=copy.deepcopy(self.store.data);expected=packed(c.expected)
  self.assertEqual(c.tick(),'both');self.assertEqual(self.store.data,old);self.assertEqual(packed(c.expected),expected)
  self.assertEqual(self.writes(),[]);self.assertEqual(sum(r['event']=='fresh_evidence_revalidated'for r in self.rows()),1)
 def test_skip_event_advances_between_coordinator_read_and_group_recheck(self):
  c=self.fixture();self.advance=self.fake_clock();original=self.peers.read;changed=[False]
  self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
  def read(who):
   fact=original(who)
   if who=='coordinator' and not changed[0]:
    changed[0]=True;self.model.update('coordinator',transport='PLAYING');self.model.emit_all()
   return fact
  self.peers.read=read;old=copy.deepcopy(self.store.data);expected=packed(c.expected)
  self.assertEqual(c.tick(),'both');self.assertEqual(self.store.data,old);self.assertEqual(packed(c.expected),expected)
  self.assertEqual(self.writes(),[]);self.assertEqual(sum(r['event']=='fresh_evidence_revalidated'for r in self.rows()),1)
 def test_persistent_disagreement_does_not_loop_or_send(self):
  c=self.setup_race(True);self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
  self.assertEqual(sum(r['event']=='fresh_evidence_retry'for r in self.rows()),1)
 def test_foreign_or_away_back_source_during_wait_refuses(self):
  for who in ('target','coordinator'):
   for back in (False,True):
    with self.subTest(who=who,back=back):
     self.doCleanups();c=self.setup_race();settle=c.sleep;old=self.model.peers[who].uri
     def change(n):
      settle(n);self.model.update(who,uri='http://foreign.invalid/audio');self.model.emit_all()
      if back:self.model.update(who,uri=old);self.model.emit_all()
     c.sleep=change;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
 def test_new_generation_or_command_intent_never_gains_authorization(self):
  for kind in ('generation','intent'):
   with self.subTest(kind=kind):
    self.doCleanups();c=self.setup_race();settle=c.sleep
    def change(n):
     settle(n)
     if kind=='generation':self.session=replace(self.session,generation='another')
     else:self.store.data['active']['phase']='intent'
    c.sleep=change;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
 def test_subscription_loss_during_wait_refuses(self):
  c=self.setup_race();settle=c.sleep
  def lose(n):settle(n);self.peers.events['coordinator'].subscriptions[0].lost=True
  c.sleep=lose;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
 def test_transport_exception_requires_unchanged_evidence_and_verified_native_mode(self):
  cases=[{'gena_uri_matches':False},{'gena_topology_matches':False},{'gena_subscription_lost':True},{'peer_evidence_age_ms':2001},{'soap_transport':'STOPPED'},{'gena_source_revision_matches':False}]
  for changes in cases:
   with self.subTest(changes=changes):
    self.doCleanups();c=self.fixture();c._owned_read=Mock(side_effect=self.error(**changes));c._read=Mock()
    self.assertEqual(c.tick(),'recovery');c._read.assert_not_called();self.assertEqual(self.writes(),[])
 def test_pre_write_and_post_intent_reads_do_not_use_poll_retry(self):
  for stage in (1,2):
   with self.subTest(stage=stage):
    self.doCleanups();c=self.fixture();original=c._read;calls=[0]
    def read(*a,**kw):
     calls[0]+=1
     if calls[0]==stage:raise self.error()
     return original(*a,**kw)
    c._read=read
    with self.assertRaises(BackendError):c._write('SetVolume',lambda _:True,group=True,DesiredVolume=50)
    self.assertEqual(self.writes(),[]);self.assertFalse(any(r['event']=='fresh_evidence_retry'for r in self.rows()))
