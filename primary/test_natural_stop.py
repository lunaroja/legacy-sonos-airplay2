"""Reproduce the real cleanup race using SOAP and GENA, without LAN calls."""
from dataclasses import replace
import time,unittest,json
import test_wire as W
from peer_runtime import Advertisement
class NaturalStop(unittest.TestCase):
 fixture=W.WireIntegration.fixture
 def setup_race(self,kind='stop'):
  c=self.fixture();self.peers.ad=Advertisement(None,time.monotonic(),False);self.assertEqual(c.tick(),'solo')
  self.session=replace(self.session,state='ended',media=False);original=self.peers.read;once=[False]
  def read(who):
   active=self.store.data.get('active')or{}
   if who=='target'and active.get('phase')=='intent'and active.get('action')=='Stop'and not once[0]:
    once[0]=True;old=self.model.peers['target'];self.model.update('target',transport='STOPPED')
    if kind=='foreign':self.model.update('target',uri='http://foreign.invalid/audio')
    if kind=='aba':
     self.model.update('target',uri='http://foreign.invalid/audio');self.model.emit_all();self.model.update('target',uri=old.uri)
    if kind=='volume':self.model.update('target',volume=old.volume+1)
    self.model.emit_all()
   return original(who)
  self.peers.read=read;return c
 def actions(self):return[a for who,a,args in self.model.all_calls if not a.startswith('Get')]
 def test_natural_stop_between_intent_and_send_is_verified_without_sending_stop(self):
  c=self.setup_race();self.assertEqual(c.tick(),'idle',self.store.public());self.assertTrue(self.model.peers['target'].idle())
  self.assertEqual(self.actions(),['SetAVTransportURI','Play','SetAVTransportURI'])
  rows=[json.loads(l)for l in self.store.audit.read_text().splitlines()];self.assertEqual(sum(r['event']=='not_sent_already_satisfied'for r in rows),1)
 def test_foreign_source_aba_and_manual_gain_are_not_adopted(self):
  for kind in('foreign','aba','volume'):
   with self.subTest(kind=kind):
    self.doCleanups();c=self.setup_race(kind);self.assertEqual(c.tick(),'recovery');self.assertNotIn('Stop',self.actions());self.assertEqual(self.store.data['active']['phase'],'intent')
