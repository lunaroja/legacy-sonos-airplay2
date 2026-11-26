"""Bounded pinned-peer reads and target-only writes; coordinator writes are impossible."""
from __future__ import annotations
from contextlib import closing
from dataclasses import dataclass,field
from http.client import HTTPConnection,HTTPException,RemoteDisconnected
from ipaddress import IPv4Address
import re,time
from xml.etree import ElementTree as X
from speaker_diagnostics import safe_record
SOAP='http://schemas.xmlsoap.org/soap/envelope/'
MAX_XML=262144
UID=re.compile(r'RINCON_[0-9A-F]{12}01400\Z')
AIRPLAY=re.compile(r'x-sonos-vli:(RINCON_[0-9A-F]{12}01400):([0-9]{1,10}),airplay:([0-9a-fA-F]{32})\Z')
PATHS={'AVTransport':'/MediaRenderer/AVTransport/Control','RenderingControl':'/MediaRenderer/RenderingControl/Control','ZoneGroupTopology':'/ZoneGroupTopology/Control'}
READS={'GetMediaInfo':('AVTransport',{'InstanceID':'0'}),'GetTransportInfo':('AVTransport',{'InstanceID':'0'}),'GetVolume':('RenderingControl',{'InstanceID':'0','Channel':'Master'}),'GetMute':('RenderingControl',{'InstanceID':'0','Channel':'Master'}),'GetZoneGroupState':('ZoneGroupTopology',{})}
WRITES={'SetAVTransportURI':('AVTransport',{'InstanceID','CurrentURI','CurrentURIMetaData'}),'Play':('AVTransport',{'InstanceID','Speed'}),'Stop':('AVTransport',{'InstanceID'}),'BecomeCoordinatorOfStandaloneGroup':('AVTransport',{'InstanceID'}),'SetVolume':('RenderingControl',{'InstanceID','Channel','DesiredVolume'})}
class BackendError(RuntimeError):pass

def xml(data):
    if not isinstance(data,bytes) or len(data)>MAX_XML or b'\0' in data:
        raise BackendError('xml_size_or_encoding')
    try:
        text=data.decode('utf-8-sig')
        if '<!DOCTYPE' in text.upper() or '<!ENTITY' in text.upper(): raise ValueError()
        root=X.fromstring(text)
    except (ValueError, X.ParseError): raise BackendError('invalid_xml') from None
    stack=[(root,0)]; count=0
    while stack:
        node,depth=stack.pop(); count+=1
        if depth>24 or count>4096: raise BackendError('xml_complexity')
        stack.extend((child,depth+1) for child in node)
    return root


def field_text(node,name,limit=65536):
    found=node.findall('.//{*}'+name)
    if len(found)!=1 or len(found[0].text or '')>limit: raise BackendError('response_field')
    return found[0].text or ''

class HTTP:
    def __init__(self, timeout=2.0): self.timeout=timeout
    def request(self, ip, port, method, path, body, headers, deadline, include_headers=False):
        remaining=deadline-time.monotonic()
        if remaining<=0: raise BackendError('http_deadline')
        phase='request';status=None
        try:
            with closing(HTTPConnection(ip,port,timeout=min(self.timeout,remaining))) as conn:
                conn.request(method,path,body=body,headers=headers)
                phase='response_headers'
                response=conn.getresponse();status=response.status;phase='response_body'
                if response.getheader('Content-Encoding','identity')!='identity': raise BackendError('http_encoding')
                pieces=[]; total=0
                while True:
                    remaining=deadline-time.monotonic()
                    if remaining<=0: raise BackendError('http_deadline')
                    if conn.sock: conn.sock.settimeout(min(self.timeout,remaining))
                    chunk=response.read1(min(16384,MAX_XML+1-total))
                    if not chunk: break
                    total+=len(chunk)
                    if total>MAX_XML: raise BackendError('http_body_limit')
                    pieces.append(chunk)
                return (response.status,dict(response.getheaders()),b''.join(pieces)) if include_headers else (response.status,b''.join(pieces))
        except BackendError as error:
            error.http_diagnostics=safe_record({'speaker_http_phase':phase,'speaker_http_status':status})
            raise
        except (OSError,HTTPException) as error:
            reason=next((code for cls,code in ((TimeoutError,'timeout'),
                (ConnectionRefusedError,'connection_refused'),(RemoteDisconnected,'remote_disconnected'),
                (ConnectionResetError,'connection_reset'),(HTTPException,'http_protocol'))
                if isinstance(error,cls)),'os_error')
            failure=BackendError('http_transport')
            failure.http_diagnostics=safe_record({'speaker_transport_reason':reason,
                'speaker_http_phase':phase,'speaker_http_status':status})
            raise failure from None

@dataclass(frozen=True)
class Revision:
    topology: str
    source: str
    uri: str = field(repr=False)
    coordinator: str
    members: frozenset[str]
    observed: float
    continuous: bool
    uri_revision: str = ''

@dataclass(frozen=True)
class Endpoint:
    ip:str
    uuid:str
    port:int=1400
    def __post_init__(self):
        address=IPv4Address(self.ip)
        if (str(address)!=self.ip or address.is_multicast or address.is_unspecified
                or not (address.is_private or address.is_loopback) or not UID.fullmatch(self.uuid)
                or type(self.port)is not int or not 1<=self.port<=65535):raise ValueError('configured_peer')

@dataclass(frozen=True)
class Peer:
    uuid:str
    uri:str=field(repr=False)
    transport:str
    coordinator:str
    members:frozenset[str]
    boot:int
    volume:int
    mute:bool
    def standalone(self):return self.coordinator==self.uuid and self.members==frozenset((self.uuid,))
    def idle(self):return self.standalone() and not self.uri and self.transport=='STOPPED'

@dataclass(frozen=True)
class Fact:
    peer:Peer
    revision:Revision
    observed:float
    def ownership(self):
        p,r=self.peer,self.revision
        return (p.uuid,p.uri,p.coordinator,tuple(sorted(p.members)),p.boot,r.uri_revision,r.topology)

class Wire:
    def __init__(self,target,coordinator=None,*,http=None,clock=time.monotonic):
        self.peers={'target':target}
        if coordinator:self.peers['coordinator']=coordinator
        self.http=http or HTTP();self.clock=clock
    def _soap(self,who,service,action,args,deadline):
        endpoint=self.peers[who];urn=f'urn:schemas-upnp-org:service:{service}:1'
        envelope=X.Element('{'+SOAP+'}Envelope');body=X.SubElement(envelope,'{'+SOAP+'}Body')
        request=X.SubElement(body,'{'+urn+'}'+action)
        for key,value in args.items():X.SubElement(request,key).text=str(value)
        status,data=self.http.request(endpoint.ip,endpoint.port,'POST',PATHS[service],X.tostring(envelope),
            {'SOAPAction':f'"{urn}#{action}"','Content-Type':'text/xml; charset="utf-8"'},deadline)
        if status!=200:raise BackendError('soap_rejected')
        response=xml(data)
        bodies=response.findall('./{'+SOAP+'}Body')
        if len(bodies)!=1 or len(bodies[0])!=1 or bodies[0][0].tag!='{'+urn+'}'+action+'Response':
            raise BackendError('soap_response_shape')
        return bodies[0][0]
    def read(self,who):
        endpoint=self.peers[who];deadline=self.clock()+2
        status,data=self.http.request(endpoint.ip,endpoint.port,'GET','/xml/device_description.xml',None,{},deadline)
        if status!=200:raise BackendError('peer_identity')
        device=xml(data).find('./{*}device')
        if device is None or device.findtext('./{*}UDN')!='uuid:'+endpoint.uuid:raise BackendError('peer_identity')
        values={name:self._soap(who,service,name,args,deadline) for name,(service,args) in READS.items()}
        uri=field_text(values['GetMediaInfo'],'CurrentURI');transport=field_text(values['GetTransportInfo'],'CurrentTransportState',32)
        if transport=='NO_MEDIA_PRESENT':transport='STOPPED'
        if transport not in ('PLAYING','STOPPED','PAUSED_PLAYBACK','TRANSITIONING'):raise BackendError('transport_state')
        groups=xml(field_text(values['GetZoneGroupState'],'ZoneGroupState',MAX_XML).encode())
        found=[g for g in groups.findall('.//{*}ZoneGroup') if any(x.get('UUID')==endpoint.uuid for x in g.findall('./{*}ZoneGroupMember'))]
        if len(found)!=1:raise BackendError('topology_identity')
        group=found[0];members=frozenset(x.get('UUID','') for x in group.findall('./{*}ZoneGroupMember'));coordinator=group.get('Coordinator','')
        own=[x for x in group.findall('./{*}ZoneGroupMember') if x.get('UUID')==endpoint.uuid]
        boot=own[0].get('BootSeq','')
        if (coordinator not in members or any(not UID.fullmatch(x) for x in members)
                or not boot.isdigit() or not 0<int(boot)<2**32):raise BackendError('topology_identity')
        volume=field_text(values['GetVolume'],'CurrentVolume',3);mute=field_text(values['GetMute'],'CurrentMute',1)
        if not volume.isdigit() or not 0<=int(volume)<=100 or mute not in ('0','1'):raise BackendError('rendering_state')
        return Peer(endpoint.uuid,uri,transport,coordinator,members,int(boot),int(volume),mute=='1')
    def write(self,who,action,**values):
        # Structural restriction, independent of controller decisions.
        if who!='target' or action not in WRITES:raise BackendError('target_only_write')
        service,keys=WRITES[action];args={'InstanceID':'0',**values}
        if service=='RenderingControl':args['Channel']='Master'
        if set(args)!=keys:raise BackendError('write_arguments')
        if action=='Play' and args['Speed']!='1':raise BackendError('write_arguments')
        if action=='SetAVTransportURI' and (len(args['CurrentURI'])>2048 or args['CurrentURIMetaData']!=''):raise BackendError('write_arguments')
        if action=='SetVolume' and (type(args['DesiredVolume'])is not int or not 0<=args['DesiredVolume']<=100):raise BackendError('write_arguments')
        return self._soap(who,service,action,args,self.clock()+2)
