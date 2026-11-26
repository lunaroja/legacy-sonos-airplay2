"""Native source lifetime must not be inferred from target transport skew."""
import copy,unittest
from dataclasses import replace
from unittest.mock import Mock
from controller import packed
from sonos_api import BackendError
import test_owned_native_hold as H

class NativeTransportObservation(unittest.TestCase):
 fixture=H.NativeHold.fixture;rows=H.NativeHold.rows;writes=H.NativeHold.writes
 fake_clock=H.NativeHold.fake_clock;end=H.NativeHold.end
 def race(self,stage='peer_read',persistent=False):
  c=self.fixture();advance=self.fake_clock();old=self.peers.read;armed=[False]
  if stage=='peer_read':self.model.end_mismatch=True;self.model.emit_all()
  else:
   def read(who):
    f=old(who)
    if who=='coordinator'and not armed[0]:armed[0]=True;self.model.end_mismatch=True;self.model.emit_all()
    return f
   self.peers.read=read
  def settle(n):
   self.assertEqual(self.writes(),[]);advance(n);c.sleep=advance
   if not persistent:self.model.end_mismatch=False;self.model.emit_all()
  c.sleep=settle;return c
 def test_playing_native_source_rechecks_target_skew_in_both_read_orders(self):
  for stage in ('peer_read','group_recheck'):
   with self.subTest(stage=stage):
    self.doCleanups();c=self.race(stage);before=copy.deepcopy(self.store.data);expected=packed(c.expected)
    self.assertEqual(c.tick(),'both');self.assertEqual(self.store.data,before);self.assertEqual(packed(c.expected),expected)
    self.assertEqual(self.writes(),[]);r=[r for r in self.rows()if r['event']=='fresh_evidence_retry'];self.assertEqual(len(r),1)
    self.assertEqual(r[0]['retry_kind'],'native_target_transport_observation');self.assertEqual(r[0]['gena_read_stage'],stage)
    self.assertTrue(any(r['event']=='fresh_evidence_revalidated'for r in self.rows()))
 def test_persistent_skew_spends_only_one_observation_and_never_writes(self):
  c=self.race(persistent=True);self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
  self.assertEqual(sum(r['event']=='fresh_evidence_retry'for r in self.rows()),1)
 def test_pause_during_wait_requires_real_hold_and_preserves_pending_volume(self):
  c=self.race(persistent=True);settle=c.sleep;self.session=replace(self.session,volume_revision=1,volume_milli=-12000)
  def pause(n):
   settle(n);self.session=replace(self.session,media=False);self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
  c.sleep=pause;self.assertEqual(c.tick(),'both');self.assertTrue(c.native_hold);self.assertEqual(self.writes(),[])
  self.assertTrue(any(r['event']=='fresh_evidence_hold_proven'for r in self.rows()))
 def test_coordinator_pause_without_local_pause_cannot_authorize_volume(self):
  c=self.race();settle=c.sleep;self.session=replace(self.session,volume_revision=1,volume_milli=-12000)
  def pause(n):settle(n);self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
  c.sleep=pause;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
 def test_real_native_end_during_wait_still_requires_original_cleanup_proof(self):
  c=self.race(persistent=True);settle=c.sleep
  def end(n):settle(n);self.end()
  c.sleep=end;self.assertEqual(c.tick(),'idle',self.store.public());self.assertTrue(self.model.peers['target'].idle())
  self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
  self.assertTrue(any(r['event']=='fresh_evidence_cleanup_proven'for r in self.rows()))
 def test_confirmed_paused_end_during_wait_uses_existing_release_context(self):
  c=self.race(persistent=True);settle=c.sleep
  def end(n):
   settle(n);self.session=replace(self.session,state='ended',media=False,end_event_confirmed=True)
   self.model.update('coordinator',transport='PAUSED_PLAYBACK');self.model.emit_all()
  c.sleep=end;self.assertEqual(c.tick(),'idle',self.store.public());self.assertTrue(self.model.peers['target'].idle())
  self.assertEqual(self.writes(),['BecomeCoordinatorOfStandaloneGroup','Stop','SetAVTransportURI'])
  self.assertTrue(any(r['event']=='owned_paused_release_authorized'for r in self.rows()))
 def test_foreign_and_away_back_sources_during_observation_never_adopt(self):
  for who in ('target','coordinator'):
   for back in (False,True):
    with self.subTest(who=who,back=back):
     self.doCleanups();c=self.race();settle=c.sleep;old=self.model.peers[who].uri
     def foreign(n):
      settle(n);self.model.update(who,uri='http://foreign.invalid/track');self.model.emit_all()
      if back:self.model.update(who,uri=old);self.model.emit_all()
     c.sleep=foreign;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
 def test_lost_unproven_unconfirmed_end_or_replaced_generation_cannot_finish(self):
  for change in ({'state':'lost'},{'state':'unproven'},{'state':'ended','media':False},{'generation':'new'}):
   with self.subTest(change=change):
    self.doCleanups();c=self.race();settle=c.sleep
    def mutate(n):settle(n);self.session=replace(self.session,**change)
    c.sleep=mutate;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
 def test_subscription_loss_and_budget_exhaustion_never_authorize_writes(self):
  for kind in ('subscription','deadline'):
   with self.subTest(kind=kind):
    self.doCleanups();c=self.race();settle=c.sleep
    def mutate(n):
     settle(6 if kind=='deadline'else n)
     if kind=='subscription':self.peers.events['target'].subscriptions[0].lost=True
    c.sleep=mutate;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
 def test_combined_target_and_coordinator_skew_uses_stricter_session_guards(self):
  import test_transport_race_retry as R
  for lost in (False,True):
   with self.subTest(lost=lost):
    self.doCleanups();c=self.race();original=self.peers.read;once=[False];settle=c.sleep
    def read(who):
     if who=='coordinator'and not once[0]:
      once[0]=True;raise R.race_error(gena_peer='coordinator',gena_read_stage='peer_read',gena_transport='PAUSED_PLAYBACK',soap_transport='PLAYING',gena_source_revision_matches=True,gena_revision_matches=True)
     return original(who)
    def change(n):
     settle(n)
     if lost:self.session=replace(self.session,state='lost')
    self.peers.read=read;c.sleep=change
    self.assertEqual(c.tick(),'recovery'if lost else'both');self.assertEqual(self.writes(),[])
    retries=[r for r in self.rows()if r['event']=='fresh_evidence_retry'];self.assertEqual(len(retries),1)
    self.assertEqual(retries[0]['retry_kind'],'native_target_transport_observation')
 def test_hold_classifier_coordinator_skew_cannot_clean_after_lineage_loss(self):
  import test_transport_race_retry as R
  c=self.race();self.session=replace(self.session,media=False);original=self.peers.read;once=[False];settle=c.sleep
  def read(who):
   if who=='coordinator'and not once[0]:
    once[0]=True;raise R.race_error(gena_peer='coordinator',gena_read_stage='peer_read',gena_transport='PAUSED_PLAYBACK',soap_transport='PLAYING',gena_source_revision_matches=True,gena_revision_matches=True)
   return original(who)
  def lose(n):settle(n);self.session=replace(self.session,state='lost')
  self.peers.read=read;c.sleep=lose;self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[])
  self.assertEqual(sum(r['event']=='fresh_evidence_retry'for r in self.rows()),1)
 def test_http_probe_failure_never_becomes_a_read_retry(self):
  c=self.race();original=self.peers.read
  def read(who):
   if who=='coordinator':raise BackendError('http_deadline')
   return original(who)
  self.peers.read=read;c.sleep=Mock(side_effect=AssertionError('HTTP retry forbidden'))
  self.assertEqual(c.tick(),'recovery');self.assertEqual(self.writes(),[]);c.sleep.assert_not_called()
 def test_cleanup_and_post_intent_paths_never_select_observation(self):
  for kind in ('cleanup','write'):
   with self.subTest(kind=kind):
    self.doCleanups();c=self.race();c.sleep=Mock(side_effect=AssertionError('no retry here'))
    with self.assertRaises(BackendError):
     if kind=='cleanup':c._cleanup()
     else:c._write('SetVolume',lambda _:True,group=True,DesiredVolume=40)
    self.assertEqual(self.writes(),[]);c.sleep.assert_not_called()
