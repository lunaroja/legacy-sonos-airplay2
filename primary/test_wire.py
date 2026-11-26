"""Real SOAP serializers, GENA parsers and controller against synthetic speakers."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as X
from unittest.mock import patch
import tempfile,threading,time,unittest
from controller import Controller
from gena_runtime import Events,EventError,SID,diagnostic_fields
from sonos_api import Wire,Endpoint,BackendError,SOAP,Fact,xml
from peer_runtime import Peers,Advertisement
from test_primary import Model,T,C,G,NATIVE
from session import Session
from runtime_state import ProductScope,Store
from wav_http import Wave

def event_body(name,value):
    root=X.Element('{urn:schemas-upnp-org:event-1-0}propertyset')
    X.SubElement(X.SubElement(root,'{urn:schemas-upnp-org:event-1-0}property'),name).text=value
    return X.tostring(root)

class SOAPModel(Model):
    def __init__(self):
        super().__init__(time.monotonic);self.events={};self.subs={};self.all_calls=[];self.unsubscribe=[]
    def topology(self):
        root=X.Element('ZoneGroups');seen=set()
        for p in self.peers.values():
            if p.coordinator in seen:continue
            seen.add(p.coordinator);g=X.SubElement(root,'ZoneGroup',Coordinator=p.coordinator)
            for uid in sorted(p.members):
                peer=next(x for x in self.peers.values() if x.uuid==uid)
                X.SubElement(g,'ZoneGroupMember',UUID=uid,BootSeq=str(peer.boot))
        return X.tostring(root,encoding='unicode')
    def body(self,sub):
        if sub.service=='ZoneGroupTopology':return event_body('ZoneGroupState',self.topology())
        p=self.peers[sub.who];root=X.Element('Event');i=X.SubElement(root,'InstanceID',val='0')
        X.SubElement(i,'AVTransportURI',val=p.uri);X.SubElement(i,'TransportState',val=p.transport)
        return event_body('LastChange',X.tostring(root,encoding='unicode'))
    def emit(self,sub):
        key=(sub.who,sub.service);sid,seq=self.subs[key];self.subs[key]=(sid,seq+1)
        self.events[sub.who].notify(sub.path,sub.ip,{'nt':'upnp:event','nts':'upnp:propchange','sid':sid,'seq':str(seq)},self.body(sub))
    def emit_all(self):
        for events in self.events.values():
            for sub in events.subscriptions:self.emit(sub)
    def request(self,ip,port,method,path,body,headers,deadline,include_headers=False):
        who='target' if ip=='192.168.50.20' else 'coordinator';p=self.peers[who]
        if method=='GET':return 200,f'<root><device><UDN>uuid:{p.uuid}</UDN></device></root>'.encode()
        if method in ('SUBSCRIBE','UNSUBSCRIBE'):
            service='AVTransport' if 'AVTransport' in path else 'ZoneGroupTopology'
            sub=next(s for s in self.events[who].subscriptions if s.service==service)
            if method=='UNSUBSCRIBE':self.unsubscribe.append((who,service,headers['SID']));return 200,b''
            if 'SID' not in headers:
                sid='uuid:'+p.uuid+'_sub0000000042';self.subs[(who,service)]=(sid,0);self.emit(sub)
            sid=self.subs[(who,service)][0]
            return 200,{'SID':sid,'TIMEOUT':'Second-300'},b''
        request=X.fromstring(body);node=request.find('./{'+SOAP+'}Body')[0];service=node.tag.split('}')[0].split(':')[-2];action=node.tag.split('}')[1]
        args={x.tag:x.text or '' for x in node};self.all_calls.append((who,action,args))
        urn=f'urn:schemas-upnp-org:service:{service}:1';envelope=X.Element('{'+SOAP+'}Envelope');out=X.SubElement(X.SubElement(envelope,'{'+SOAP+'}Body'),'{'+urn+'}'+action+'Response')
        values={}
        if action=='GetMediaInfo':values={'CurrentURI':p.uri}
        elif action=='GetTransportInfo':values={'CurrentTransportState':p.transport}
        elif action=='GetVolume':values={'CurrentVolume':str(p.volume)}
        elif action=='GetMute':values={'CurrentMute':str(int(p.mute))}
        elif action=='GetZoneGroupState':values={'ZoneGroupState':self.topology()}
        else:
            assert who=='target','coordinator mutation forbidden'
            args.pop('InstanceID');args.pop('Channel',None)
            if action=='SetVolume':args['DesiredVolume']=int(args['DesiredVolume'])
            self.write(action,**args);self.emit_all()
        for key,value in values.items():X.SubElement(out,key).text=value
        return 200,X.tostring(envelope)

class WireIntegration(unittest.TestCase):
    def fixture(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);root=Path(tmp.name)
        self.model=SOAPModel();self.peers=object.__new__(Peers);self.peers.clock=time.monotonic;self.peers.lock=threading.RLock()
        target=Endpoint('192.168.50.20',T);coordinator=Endpoint('192.168.50.30',C)
        self.peers.events={};self.peers.wire=Wire(target,coordinator,http=self.model)
        self.peers.ad=Advertisement(G,time.monotonic(),True)
        for who,endpoint in (('target',target),('coordinator',coordinator)):
            config=SimpleNamespace(target=endpoint if who=='target' else None,coordinator=endpoint if who=='coordinator' else None)
            event=Events(config,{who:1400},'127.0.0.1',0,http=self.model);self.model.events[who]=event;self.peers.events[who]=event
            self.addCleanup(event.close)
            for sub in event.subscriptions:event.subscribe(sub)
        self.scope=ProductScope(root/'scope',(T,C));self.addCleanup(self.scope.close)
        self.store=Store(root/'state','wire');self.session=Session('one','present',True,G,0,0,time.monotonic(),1)
        self.wave=Wave('127.0.0.1',19091,'192.168.50.20');self.addCleanup(self.wave.close)
        self.c=Controller(self.peers,lambda:self.session,self.wave,self.scope,self.store)
        return self.c
    def test_real_wire_mid_song_deselection_preserves_coordinator(self):
        c=self.fixture();self.assertEqual(c.tick(),'both');before=self.model.peers['coordinator']
        self.session=replace(self.session,state='ended',media=False)
        self.assertEqual(c.tick(),'idle',self.store.public())
        self.assertEqual(self.model.peers['coordinator'],replace(before,members=frozenset((C,))))
        writes=[(who,a) for who,a,_ in self.model.all_calls if not a.startswith('Get')]
        self.assertEqual(writes,[('target','SetAVTransportURI'),('target','BecomeCoordinatorOfStandaloneGroup'),('target','Stop'),('target','SetAVTransportURI')])
        self.assertTrue(self.model.peers['target'].idle());self.assertIsNone(self.store.data['active'])
    def test_real_wire_wav_volume_and_native_promotion(self):
        c=self.fixture();self.peers.ad=Advertisement(None,time.monotonic(),True)
        self.assertEqual(c.tick(),'solo');self.session=replace(self.session,volume_revision=1,volume_milli=-12000)
        self.assertEqual(c.tick(),'solo');self.assertEqual(self.model.peers['target'].volume,60)
        self.peers.ad=Advertisement(G,time.monotonic(),True);self.assertEqual(c.tick(),'both')
        self.assertEqual(self.model.peers['coordinator'].volume,31)
    def test_same_group_needs_uri_advertisement_session_and_fresh_continuity(self):
        self.fixture();f=self.peers.read('coordinator');self.assertTrue(self.peers.same_group(self.session,f))
        for change in ({'group':None},{'state':'ended'},{'media':False}):self.assertFalse(self.peers.same_group(replace(self.session,**change),f))
        self.peers.ad=Advertisement(G,time.monotonic()-3,True);self.assertFalse(self.peers.same_group(self.session,f))
        self.peers.ad=Advertisement(G,time.monotonic()-1.5,True,1);self.assertFalse(self.peers.same_group(self.session,f))
        self.peers.ad=Advertisement(G,time.monotonic(),True)
        self.model.update('coordinator',uri='http://foreign.invalid/track');self.model.emit_all()
        with self.assertRaises(BackendError):self.peers.same_group(self.session,f)
    def test_foreign_away_back_and_subscription_gap_fail_closed(self):
        for case in ('source','gap'):
            with self.subTest(case=case):
                self.doCleanups();c=self.fixture();self.assertEqual(c.tick(),'both');count=len(self.model.calls)
                if case=='source':
                    self.model.update('coordinator',uri='http://foreign.invalid/track');self.model.emit_all();self.model.update('coordinator',uri=NATIVE);self.model.emit_all()
                else:
                    sub=self.peers.events['coordinator'].subscriptions[0];sid,seq=self.model.subs[('coordinator',sub.service)];self.model.subs[('coordinator',sub.service)]=(sid,seq+1)
                    with self.assertRaises(EventError):self.model.emit(sub)
                self.assertEqual(c.tick(),'recovery');self.assertEqual(len(self.model.calls),count)
    def test_solo_never_queries_unselected_coordinator(self):
        c=self.fixture();self.peers.ad=Advertisement(None,0,False)
        self.assertEqual(c.tick(),'solo')
        self.assertFalse(any(who=='coordinator' for who,_,_ in self.model.all_calls))
    def test_optional_coordinator_event_failure_does_not_fail_owned_solo(self):
        c=self.fixture();self.peers.ad=Advertisement(None,0,False);self.assertEqual(c.tick(),'solo')
        self.peers.ad=Advertisement(G,time.monotonic(),True);self.peers.events['coordinator'].subscriptions[0].lost=True
        self.assertEqual(c.tick(),'solo');self.assertFalse(c.failed)
    def test_only_target_events_are_required_when_coordinator_absent(self):
        self.fixture();events=self.peers.events['target'];self.assertEqual(len(events.subscriptions),2)
        self.assertTrue(events.revision('target',self.model.peers['target'],time.monotonic()).continuous)
    def test_opaque_sid_bounds_exact_duplicate_gap_and_single_cleanup(self):
        self.fixture();events=self.peers.events['target'];sub=events.subscriptions[0]
        self.assertIn('_sub',sub.sid);headers={'nt':'upnp:event','nts':'upnp:propchange','sid':sub.sid,'seq':'0'}
        events.notify(sub.path,sub.ip,headers,self.model.body(sub));self.assertFalse(sub.lost)
        with self.assertRaises(EventError):events.notify(sub.path,sub.ip,{**headers,'seq':'2'},self.model.body(sub))
        self.assertTrue(sub.lost);events.close();events.close()
        self.assertEqual(sum(x[0]=='target' for x in self.model.unsubscribe),2)
        for sid in ('uuid:x\r\nFoo:bar','uuid:x y','uuid:'+'x'*124):self.assertIsNone(SID.fullmatch(sid))
    def test_xml_rejects_entities_deep_and_large_inputs(self):
        for data in (b'<!DOCTYPE r [<!ENTITY x "hi">]><r>&x;</r>',b'<r>'+b'x'*262144+b'</r>',b'<r>'*26+b'</r>'*26):
            with self.assertRaises(BackendError):xml(data)

if __name__=='__main__':unittest.main()

@unittest.skipUnless(__import__('sys').platform.startswith('linux'),'Linux loopback GENA')
class EventSocket(unittest.TestCase):
    def test_callback_status_exact_retry_and_source_scope(self):
        import socket
        class HTTP:
            def request(self,ip,port,method,path,body,headers,deadline,include_headers=False):
                if method=='UNSUBSCRIBE':return 200,b''
                return 200,{'SID':'uuid:opaque_sonos_subscription','TIMEOUT':'Second-300'},b''
        config=SimpleNamespace(target=Endpoint('127.0.0.1',T),coordinator=None)
        events=Events(config,{'target':1400},'127.0.0.1',0,http=HTTP());events.start();self.addCleanup(events.close)
        sub=events.subscriptions[0];body=event_body('LastChange','<Event><InstanceID val="0"><AVTransportURI val=""/><TransportState val="STOPPED"/></InstanceID></Event>')
        def request(path,sequence,body=body):
            headers=f'NOTIFY {path} HTTP/1.1\r\nNT: upnp:event\r\nNTS: upnp:propchange\r\nSID: {sub.sid}\r\nSEQ: {sequence}\r\nContent-Length: {len(body)}\r\n\r\n'.encode()
            with socket.create_connection(('127.0.0.1',events.port),timeout=2) as client:
                client.sendall(headers+body);return client.recv(1024).split(b'\r\n')[0]
        self.assertEqual(request(sub.path,0),b'HTTP/1.1 200 OK')
        self.assertEqual(request(sub.path,0),b'HTTP/1.1 200 OK')
        self.assertEqual(request('/event/wrong',1),b'HTTP/1.1 412 Precondition Failed')
        self.assertFalse(sub.lost)
        self.assertEqual(request(sub.path,2),b'HTTP/1.1 412 Precondition Failed');self.assertTrue(sub.lost)
