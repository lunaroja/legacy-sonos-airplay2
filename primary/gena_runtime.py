"""Bounded native GENA subscriptions and per-peer semantic revision continuity."""
from __future__ import annotations
from dataclasses import dataclass,field
from socketserver import ThreadingTCPServer,BaseRequestHandler
import hashlib,hmac,re,secrets,socket,threading,time
from sonos_api import HTTP,Revision,xml,UID,BackendError

EVENT_PATHS={'AVTransport':'/MediaRenderer/AVTransport/Event','ZoneGroupTopology':'/ZoneGroupTopology/Event'}
# Sonos subscription IDs are opaque (often RINCON_..._sub...), not RFC4122
# UUIDs. Bound the complete value, require printable ASCII with no whitespace,
# and still compare the exact response SID on every callback/renewal.
SID=re.compile(r'uuid:[\x21-\x7e]{1,123}\Z',re.ASCII)

EVENT_CODES=frozenset(('event_root','event_property','duplicate_event_property','event_instance',
 'event_transport_field','event_value_bound','event_topology_group','event_topology_identity',
 'event_subscription','event_sequence_gap','event_destination','event_type','event_headers',
 'event_initial_race','event_initial_timeout','subscription_response','subscription_already_lost','event_request_deadline',
 'event_header_bound','event_request','event_header_count','event_duplicate_header','event_body_bound',
 'event_short_body','event_extra_body','event_continuity_lost','event_subscription_expired',
 'event_payload_invalid','event_processing_failed','unknown_event_error'))
EVENT_DETAILS=frozenset(('http_status','sid_format','sequence_format','timeout_format','timeout_range',
 'renewal_sid_changed','initial_duplicate_or_nonzero','xml_size_or_encoding','invalid_xml','xml_complexity'))

def allowed(value,choices):return value if type(value)is str and value in choices else None

class EventError(RuntimeError):
    """Fixed codes only, even when an unexpected caller supplies a raw message."""
    def __init__(self,code,detail=None):
        self.code=allowed(code,EVENT_CODES) or 'unknown_event_error'
        self.detail=allowed(detail,EVENT_DETAILS)
        self.peer=self.service=self.phase=None
        super().__init__(self.code)

def event_error(error,sub=None,phase=None):
    if not isinstance(error,EventError):
        detail=error.args[0] if isinstance(error,BackendError) and len(error.args)==1 else None
        if allowed(detail,('xml_size_or_encoding','invalid_xml','xml_complexity')):
            error=EventError('event_payload_invalid',detail)
        else:error=EventError('event_processing_failed')
    if sub is not None:
        error.peer=allowed(sub.who,('target','coordinator'))
        error.service=allowed(sub.service,EVENT_PATHS)
    if error.phase is None:error.phase=allowed(phase,('subscribe','renew','notify','maintain'))
    return error

PEER_FAILURE_CODES=frozenset(('peer_events_unavailable','peer_evidence_expired',
 'peer_evidence_changed','owned_source_changed','ownership_or_selection_changed',
 'changed_after_intent','unexpected_write_readback','write_readback_timeout','cleanup_not_idle'))
EVIDENCE_FLAGS=frozenset(('gena_events_available','gena_snapshot_available','peer_evidence_age_clipped','gena_subscriptions_initialized','gena_subscriptions_continuous',
 'gena_subscription_lost','gena_subscriptions_unexpired','gena_uri_matches','gena_uri_empty',
 'gena_transport_matches','gena_topology_matches','gena_revision_tokens_present',
 'gena_revision_matches','gena_uri_revision_matches','gena_topology_revision_matches',
 'gena_source_revision_matches'))
TRANSPORT_ENUMS=frozenset(('STOPPED','PLAYING','PAUSED_PLAYBACK','TRANSITIONING',
 'NO_MEDIA_PRESENT','missing','other'))

def transport_enum(value):
    if value is None:return 'missing'
    return value if type(value)is str and value in TRANSPORT_ENUMS else 'other'

def diagnostic_fields(error):
    if isinstance(error,BackendError):
        fields={};code=error.args[0] if len(error.args)==1 else None
        safe=allowed(code,PEER_FAILURE_CODES)
        if safe is not None:fields['controller_failure_reason']=safe
        raw=getattr(error,'evidence_diagnostics',None)
        if type(raw)is dict:
            for key in EVIDENCE_FLAGS:
                if type(raw.get(key))is bool:fields[key]=raw[key]
            for key,choices in (('gena_peer',('target','coordinator')),
                                ('gena_transport',TRANSPORT_ENUMS),
                                ('soap_transport',TRANSPORT_ENUMS),
                                ('gena_read_stage',('peer_read','caller_recheck','group_recheck','target_recheck','same_group')),
                                ('peer_evidence_expiry_cause',('events_unavailable','too_old','clock_reversed','age_nonfinite'))):
                safe=allowed(raw.get(key),choices)
                if safe is not None:fields[key]=safe
        age=raw.get('peer_evidence_age_ms') if type(raw)is dict else None
        if type(age)is int and -60000<=age<=60000:fields['peer_evidence_age_ms']=age
        return fields
    if not isinstance(error,EventError):return {}
    # Re-allowlist at output too; no str(error), raw args, headers or XML.
    fields={'gena_reason':allowed(error.code,EVENT_CODES) or 'unknown_event_error'}
    for key,value,choices in (('gena_detail',error.detail,EVENT_DETAILS),
                             ('gena_peer',error.peer,('target','coordinator')),
                             ('gena_service',error.service,EVENT_PATHS),
                             ('gena_phase',error.phase,('subscribe','renew','notify','maintain'))):
        safe=allowed(value,choices)
        if safe is not None:fields[key]=safe
    return fields

def properties(data):
    root=xml(data)
    if root.tag!='{urn:schemas-upnp-org:event-1-0}propertyset':raise EventError('event_root')
    values={}
    for prop in root:
        if prop.tag!='{urn:schemas-upnp-org:event-1-0}property' or len(prop)!=1:raise EventError('event_property')
        node=prop[0];name=node.tag.split('}')[-1]
        if name in values:raise EventError('duplicate_event_property')
        values[name]=node.text or ''
    return values

def transport_update(data):
    values=properties(data)
    if 'LastChange' not in values:return {}
    root=xml(values['LastChange'].encode())
    instances=[x for x in root if x.tag.split('}')[-1]=='InstanceID' and x.get('val')=='0']
    if len(instances)!=1:raise EventError('event_instance')
    result={}
    for node in instances[0]:
        name=node.tag.split('}')[-1]
        if name not in ('AVTransportURI','TransportState'):continue
        if name in result or 'val' not in node.attrib:raise EventError('event_transport_field')
        value=node.get('val')
        if len(value)>65536:raise EventError('event_value_bound')
        result[name]=('STOPPED' if value=='NO_MEDIA_PRESENT' else value)
    return result

def topology_for(data,uuid):
    values=properties(data)
    if 'ZoneGroupState' not in values:return None
    root=xml(values['ZoneGroupState'].encode())
    groups=[g for g in root.findall('.//{*}ZoneGroup') if any(m.get('UUID')==uuid for m in g.findall('./{*}ZoneGroupMember'))]
    if len(groups)!=1:raise EventError('event_topology_group')
    g=groups[0];members=frozenset(x.get('UUID','') for x in g.findall('./{*}ZoneGroupMember'))
    coord=g.get('Coordinator','')
    own=next(x for x in g.findall('./{*}ZoneGroupMember') if x.get('UUID')==uuid)
    boot=own.get('BootSeq','')
    if coord not in members or any(not UID.fullmatch(x) for x in members) or not boot.isdigit() or len(boot)>20:
        raise EventError('event_topology_identity')
    return coord,members,int(boot)

@dataclass
class Subscription:
    who:str
    uuid:str
    ip:str
    port:int
    service:str
    path:str=field(default_factory=lambda:'/event/'+secrets.token_urlsafe(32))
    sid:str|None=None
    sequence:int|None=None
    deadline:float=0
    renew_at:float=0
    lost:bool=False
    early:tuple|None=None
    last_digest:str|None=None
    failure:EventError|None=field(default=None,repr=False)
    cleanup_sid:str|None=field(default=None,repr=False)
    cleanup_done:bool=False

class Events:
    def __init__(self,config,ports,bind,port,*,changed=lambda *_:None,http=None,clock=time.monotonic):
        self.config,self.ports,self.bind,self.port=config,ports,bind,port
        self.changed,self.http,self.clock=changed,http or HTTP(),clock
        self.lock=threading.RLock();self.done=threading.Event();self.server=self.thread=None
        self.incarnation=secrets.token_hex(12);self.number=0
        self.peers=tuple(w for w in ('target','coordinator') if getattr(config,w,None) is not None)
        self.subscriptions=[Subscription(w,getattr(config,w).uuid,getattr(config,w).ip,ports[w],service)
                            for w in self.peers for service in EVENT_PATHS]
        self.states={w:{'source':{},'topology':None,'source_revision':'','topology_revision':'','uri_revision':''} for w in self.peers}
    def _revision(self):self.number+=1;return self.incarnation+':'+str(self.number)
    def _lose(self,sub,error=None):
        first=not sub.lost;sub.lost=True
        if sub.failure is None:
            sub.failure=event_error(error or EventError('event_continuity_lost'),sub)
        if first:self.changed(sub.who,'continuity_lost')
    def _apply(self,sub,sid,seq,data):
        if sub.lost or sid!=sub.sid:raise EventError('event_subscription')
        digest=hashlib.sha256(data).hexdigest()
        if sub.sequence==seq and sub.last_digest==digest:return
        expected=0 if sub.sequence is None else 1 if sub.sequence==0xffffffff else sub.sequence+1
        if seq!=expected:raise EventError('event_sequence_gap')
        state=self.states[sub.who];changed=False
        if sub.service=='AVTransport':
            update=transport_update(data);source={**state['source'],**update}
            if source!=state['source']:
                if source.get('AVTransportURI')!=state['source'].get('AVTransportURI'):
                    state['previous_uri']=state['source'].get('AVTransportURI')
                    state['previous_uri_revision']=state['uri_revision']
                    state['uri_revision']=self._revision()
                state['source']=source;state['source_revision']=self._revision();changed=True
        else:
            topology=topology_for(data,sub.uuid)
            if topology is not None and topology!=state['topology']:
                state['previous_topology']=state['topology']
                state['previous_topology_revision']=state['topology_revision']
                state['topology']=topology;state['topology_revision']=self._revision();changed=True
        sub.sequence=seq;sub.last_digest=digest
        if changed:self.changed(sub.who,'source' if sub.service=='AVTransport' else 'topology')
    def notify(self,path,ip,headers,data):
        with self.lock:
            found=[s for s in self.subscriptions if hmac.compare_digest(s.path,path) and s.ip==ip]
            if len(found)!=1:raise EventError('event_destination')
            sub=found[0]
            try:
                if sub.lost:raise sub.failure or EventError('event_subscription')
                if headers.get('nt')!='upnp:event' or headers.get('nts')!='upnp:propchange':raise EventError('event_type')
                sid=headers.get('sid','');seq=headers.get('seq','')
                if not SID.fullmatch(sid):raise EventError('event_headers','sid_format')
                if not seq.isdigit() or len(seq)>10 or int(seq)>0xffffffff:raise EventError('event_headers','sequence_format')
                if sub.sid is None:
                    if sub.early==(sid,int(seq),data):return  # exact initial retry; one bounded pending event
                    if sub.early is not None or int(seq)!=0:raise EventError('event_initial_race','initial_duplicate_or_nonzero')
                    # Validate bounded XML now, accept only when response SID matches.
                    properties(data);sub.early=(sid,int(seq),data);return
                self._apply(sub,sid,int(seq),data)
            except Exception as error:
                failure=event_error(error,sub,'notify');self._lose(sub,failure)
                raise failure from None
    def subscribe(self,sub,renew=False):
        try:return self._subscribe(sub,renew)
        except EventError as error:
            failure=event_error(error,sub,'renew' if renew else 'subscribe')
            with self.lock:self._lose(sub,failure)
            raise failure from None
    def _subscribe(self,sub,renew=False):
        headers={'SID':sub.sid,'TIMEOUT':'Second-300'} if renew else {
            'CALLBACK':f'<http://{self.bind}:{self.port}{sub.path}>','NT':'upnp:event','TIMEOUT':'Second-300'}
        status,response,_=self.http.request(sub.ip,sub.port,'SUBSCRIBE',EVENT_PATHS[sub.service],None,headers,
                                           self.clock()+2,include_headers=True)
        response={k.lower():v for k,v in response.items()}
        sid=response.get('sid','');match=re.fullmatch(r'Second-([0-9]{1,6})',response.get('timeout',''),re.I)
        with self.lock:
            if status!=200:raise EventError('subscription_response','http_status')
            if not SID.fullmatch(sid):raise EventError('subscription_response','sid_format')
            # A successful response with a bounded header-safe SID identifies
            # our newly created subscription even if later timeout/initial-event
            # validation fails. Save it only for one cleanup attempt, not event
            # authority. A changed renewal SID cannot replace the original.
            if not renew and sub.cleanup_sid is None:
                sub.cleanup_sid=sid;sub.cleanup_done=False
            if not match:raise EventError('subscription_response','timeout_format')
            if not 10<=int(match[1])<=86400:raise EventError('subscription_response','timeout_range')
            if renew and sid!=sub.sid:raise EventError('subscription_response','renewal_sid_changed')
            if sub.lost:raise sub.failure or EventError('subscription_already_lost')
            sub.sid=sid;seconds=int(match[1]);sub.deadline=self.clock()+seconds;sub.renew_at=self.clock()+seconds/2
            if sub.early:
                early=sub.early;sub.early=None
                try:self._apply(sub,*early)
                except Exception as error:raise event_error(error,sub,'notify') from None
    def pending_initial(self,who):
        """First service without a complete initial snapshot, even after a SUBSCRIBE ACK."""
        with self.lock:
            ready=set();state=self.states[who];now=self.clock()
            for sub in self.subscriptions:
                if sub.who!=who:continue
                if sub.lost:raise sub.failure or event_error(EventError('event_continuity_lost'),sub,'maintain')
                if sub.sid and not sub.deadline>now:
                    raise event_error(EventError('event_subscription_expired'),sub,'maintain')
                if sub.sid and sub.sequence is not None:ready.add(sub.service)
            if ('AVTransport' not in ready or 'AVTransportURI' not in state['source']
                    or 'TransportState' not in state['source'] or not state['uri_revision'] or not state['source_revision']):
                return 'AVTransport'
            if 'ZoneGroupTopology' not in ready or state['topology'] is None or not state['topology_revision']:
                return 'ZoneGroupTopology'
            return None
    def revision(self,who,peer,observed):
        return self.revision_with_diagnostics(who,peer,observed)[0]
    def revision_with_diagnostics(self,who,peer,observed):
        # The decision and its fixed diagnostic flags must describe the same
        # locked snapshot, rather than a later event observed after failure.
        with self.lock:
            now=self.clock();state=self.states[who]
            subs=[s for s in self.subscriptions if s.who==who]
            initialized=bool(subs and all(s.sid and s.sequence is not None for s in subs))
            unexpired=bool(subs and all(s.deadline>now for s in subs))
            lost=any(s.lost for s in subs)
            continuous=bool(initialized and not lost and unexpired)
            uri_matches=state['source'].get('AVTransportURI')==peer.uri
            transport_matches=state['source'].get('TransportState')==peer.transport
            topology_matches=state['topology']==(peer.coordinator,peer.members,peer.boot)
            good=bool(continuous and uri_matches and transport_matches and topology_matches)
            revision=Revision(state['topology_revision'],state['source_revision'],peer.uri,
                peer.coordinator,peer.members,observed,good,state['uri_revision'])
            fields={'gena_peer':allowed(who,('target','coordinator')),
                'gena_subscriptions_initialized':initialized,
                'gena_subscriptions_continuous':continuous,
                'gena_subscription_lost':lost,'gena_subscriptions_unexpired':unexpired,
                'gena_uri_matches':uri_matches,'gena_uri_empty':state['source'].get('AVTransportURI')=='',
                'gena_transport_matches':transport_matches,
                'gena_topology_matches':topology_matches,
                'gena_transport':transport_enum(state['source'].get('TransportState')),
                'soap_transport':transport_enum(peer.transport)}
            return revision,fields
    def direct_transition_matches(self,who,before,after):
        return self.direct_transition_with_diagnostics(who,before,after)[0]
    def direct_transition_with_diagnostics(self,who,before,after):
        """The same predecessor decision and bounded flags from its locked snapshot."""
        with self.lock:
            state=self.states[who];b,a=before.peer,after.peer;br,ar=before.revision,after.revision
            subs=[s for s in self.subscriptions if s.who==who];now=self.clock()
            continuous=bool(subs) and not any(not s.sid or s.sequence is None or s.lost or s.deadline<=now for s in subs)
            fields={'end_direct_subscriptions_continuous':continuous}
            if not continuous:return False,fields
            tokens=bool(br.uri_revision and br.topology and ar.uri_revision and ar.topology)
            fields['end_direct_tokens_present']=tokens
            if not tokens:return False,fields
            bt=(b.coordinator,b.members,b.boot);at=(a.coordinator,a.members,a.boot)
            snapshot={
                'end_direct_snapshot_uri_matches':state['source'].get('AVTransportURI')==a.uri,
                'end_direct_snapshot_topology_matches':state['topology']==at,
                'end_direct_snapshot_uri_revision_matches':state['uri_revision']==ar.uri_revision,
                'end_direct_snapshot_topology_revision_matches':state['topology_revision']==ar.topology,
                'end_direct_snapshot_source_revision_matches':state['source_revision']==ar.source}
            fields.update(snapshot)
            if not all(snapshot.values()):return False,fields
            uri_unchanged=b.uri==a.uri;uri_revision_unchanged=br.uri_revision==ar.uri_revision
            previous_uri=state.get('previous_uri')==b.uri
            previous_uri_revision=state.get('previous_uri_revision')==br.uri_revision
            topology_unchanged=bt==at;topology_revision_unchanged=br.topology==ar.topology
            previous_topology=state.get('previous_topology')==bt
            previous_topology_revision=state.get('previous_topology_revision')==br.topology
            uri=uri_revision_unchanged if uri_unchanged else previous_uri and previous_uri_revision and not uri_revision_unchanged
            topology=topology_revision_unchanged if topology_unchanged else previous_topology and previous_topology_revision and not topology_revision_unchanged
            fields.update(end_direct_uri_unchanged=uri_unchanged,
                end_direct_uri_revision_unchanged=uri_revision_unchanged,
                end_direct_previous_uri_matches=previous_uri,
                end_direct_previous_uri_revision_matches=previous_uri_revision,
                end_direct_topology_unchanged=topology_unchanged,
                end_direct_topology_revision_unchanged=topology_revision_unchanged,
                end_direct_previous_topology_matches=previous_topology,
                end_direct_previous_topology_revision_matches=previous_topology_revision,
                end_direct_uri_transition_matches=bool(uri),end_direct_topology_transition_matches=bool(topology))
            return bool(uri and topology),fields
    def start(self):
        owner=self
        class Handler(BaseRequestHandler):
            def handle(self):
                sock=self.request;deadline=time.monotonic()+2;status=412
                def receive(count):
                    remaining=deadline-time.monotonic()
                    if remaining<=0:raise EventError('event_request_deadline')
                    sock.settimeout(remaining);return sock.recv(count)
                try:
                    data=bytearray()
                    while b'\r\n\r\n' not in data:
                        chunk=receive(1024)
                        if not chunk or len(data)+len(chunk)>8192:raise EventError('event_header_bound')
                        data.extend(chunk)
                    header,body=bytes(data).split(b'\r\n\r\n',1);lines=header.decode('ascii').split('\r\n')
                    parts=lines[0].split(' ')
                    if len(parts)!=3 or parts[0]!='NOTIFY' or parts[2] not in ('HTTP/1.0','HTTP/1.1'):raise EventError('event_request')
                    headers={}
                    if len(lines)>33:raise EventError('event_header_count')
                    for line in lines[1:]:
                        key,value=line.split(':',1);key=key.lower()
                        if key in headers:raise EventError('event_duplicate_header')
                        headers[key]=value.strip()
                    length=headers.get('content-length','')
                    if 'transfer-encoding' in headers or not length.isdigit() or not 0<int(length)<=262144:raise EventError('event_body_bound')
                    while len(body)<int(length):
                        chunk=receive(min(16384,int(length)-len(body)))
                        if not chunk:raise EventError('event_short_body')
                        body+=chunk
                    if len(body)!=int(length):raise EventError('event_extra_body')
                    owner.notify(parts[1],self.client_address[0],headers,body);status=200
                except Exception:pass
                # Sonos retries initial NOTIFYs when the success reason is not OK.
                reason='OK' if status==200 else 'Precondition Failed'
                try:sock.sendall(f'HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'.encode())
                except OSError:pass
        class Server(ThreadingTCPServer):
            allow_reuse_address=True;daemon_threads=True;request_queue_size=8
            slots=threading.BoundedSemaphore(8)
            def process_request(self,request,address):
                if not self.slots.acquire(False):request.close();return
                try:super().process_request(request,address)
                except BaseException:self.slots.release();raise
            def process_request_thread(self,request,address):
                try:super().process_request_thread(request,address)
                finally:self.slots.release()
        self.server=Server((self.bind,self.port),Handler);self.port=self.server.server_address[1]
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.1},daemon=True);self.thread.start()
        try:
            for sub in self.subscriptions:self.subscribe(sub)
        except BaseException:self.close();raise
    def maintain(self):
        for sub in self.subscriptions:
            if sub.lost:raise sub.failure or event_error(EventError('event_continuity_lost'),sub,'maintain')
            if self.clock()>=sub.deadline:
                failure=event_error(EventError('event_subscription_expired'),sub,'maintain')
                with self.lock:self._lose(sub,failure)
                raise failure
            if self.clock()>=sub.renew_at:
                try:self.subscribe(sub,renew=True)
                except Exception:
                    with self.lock:self._lose(sub)
                    raise
    def close(self):
        self.done.set()
        for sub in self.subscriptions:
            with self.lock:
                sid=None
                if not sub.cleanup_done:
                    sub.cleanup_done=True
                    sid=sub.cleanup_sid or sub.sid
                    sub.cleanup_sid=None
            if sid and SID.fullmatch(sid):
                try:self.http.request(sub.ip,sub.port,'UNSUBSCRIBE',EVENT_PATHS[sub.service],None,{'SID':sid},self.clock()+1)
                except Exception:pass
        if self.server:self.server.shutdown();self.server.server_close();self.server=None
        if self.thread:self.thread.join(2);self.thread=None
