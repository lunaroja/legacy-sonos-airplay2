"""Independent target/coordinator subscriptions; absent coordinator cannot block solo."""
from __future__ import annotations
from dataclasses import dataclass
from types import SimpleNamespace
import math,threading,time
from configuration import endpoint
from fresh_airplay_gid import Target,read_gid,canonical_group_uuid
from gena_runtime import Events,EventError
from sonos_api import Wire,Fact,BackendError,AIRPLAY

@dataclass(frozen=True)
class Advertisement:
    group:str|None
    observed:float
    valid:bool
    ttl:float=2.0

class Peers:
    def __init__(self,config,*,wake=lambda:None,clock=time.monotonic):
        self.config=config;self.clock=clock;self.wake=wake;self.events={};self.lock=threading.RLock()
        self.wire=Wire(endpoint(config,'target'),endpoint(config,'coordinator'),clock=clock)
        self.done=threading.Event();self.thread=None;self.ad=Advertisement(None,0,False)
    def _events(self,who):
        peer=endpoint(self.config,who)
        cfg=SimpleNamespace(target=peer if who=='target' else None,coordinator=peer if who=='coordinator' else None)
        return Events(cfg,{who:peer.port},self.config['local_ip'],self.config['event_port']+(who=='coordinator'),changed=lambda *_:self.wake())
    def start(self,*,target_only=False):
        events=self._events('target');events.start();self.events['target']=events
        if not target_only:self.start_coordinator()
    def start_coordinator(self):
        if self.config['coordinator']:
            self.thread=threading.Thread(target=self._coordinator,daemon=True);self.thread.start()
    def wait_initial(self,*,with_coordinator):
        """Wait only for initial notifications. No SOAP, renewal or additional subscription."""
        roles=('target','coordinator') if with_coordinator else ('target',)
        deadline=self.clock()+5
        missing,service='target',None
        while self.clock()<deadline:
            pending=[]
            for who in roles:
                with self.lock:events=self.events.get(who)
                if events is None:pending.append((who,None));continue
                initial=events.pending_initial(who)
                if initial is not None:pending.append((who,initial))
            if not pending:
                if self.clock()<deadline:return
                break
            missing,service=pending[0]
            time.sleep(min(.05,max(0,deadline-self.clock())))
        error=EventError('event_initial_timeout')
        error.peer=missing;error.service=service
        raise error
    def _coordinator(self):
        c=self.config['coordinator'];target=Target(c['ip'],c['airplay_instance'])
        while not self.done.is_set():
            events=None
            try:
                events=self._events('coordinator');events.start()
                with self.lock:self.events['coordinator']=events
                while not self.done.is_set():
                    events.maintain();result=read_gid(target,timeout=.6)
                    group=None;valid=False;ttl=result.get('ttl_seconds',0)
                    if (result.get('status')=='ok' and result.get('reason')=='matched_fresh_response'
                            and result.get('source_ip')==target.ip and result.get('instance')==target.instance
                            and type(ttl)is int and ttl>0):
                        group=canonical_group_uuid(result.get('fields',{}).get('gid'));valid=True
                    with self.lock:self.ad=Advertisement(group,self.clock(),valid,min(2,ttl) if valid else 0)
                    self.wake();self.done.wait(.3)
            except Exception:
                with self.lock:self.ad=Advertisement(None,0,False);self.events.pop('coordinator',None)
                self.wake()
            finally:
                if events:events.close()
            self.done.wait(3)
    def maintain_target(self):
        # Renewal belongs to the service lifetime, including hours without media.
        # Never replace a lost subscription inside an owned session.
        with self.lock:events=self.events.get('target')
        if events is None:raise BackendError('peer_events_unavailable')
        events.maintain()

    def _observe(self,who):
        with self.lock:events=self.events.get(who)
        if not events:raise BackendError('peer_events_unavailable')
        events.maintain();observed=self.clock();peer=self.wire.read(who);revision=events.revision(who,peer,observed)
        return Fact(peer,revision,observed)
    def read(self,who):
        f=self._observe(who);self.recheck(who,f,stage='peer_read');return f
    def observe_owned_native_hold(self,expected):
        from owned_native_hold import observe_owned_native_hold
        return observe_owned_native_hold(self,expected)
    def begin_owned_paused_release(self,expected,local_end):
        from owned_paused_release import OwnedPausedRelease
        return OwnedPausedRelease(self,expected,local_end)
    def begin_owned_native_end(self,expected):
        from owned_native_end import OwnedNativeEnd
        return OwnedNativeEnd(self,expected)
    def _evidence_snapshot(self,events,who,f):
        current,fields=events.revision_with_diagnostics(who,f.peer,f.observed)
        fields.update(gena_revision_tokens_present=bool(current.source and current.topology and current.uri_revision),
            gena_revision_matches=current==f.revision,
            gena_uri_revision_matches=current.uri_revision==f.revision.uri_revision,
            gena_topology_revision_matches=current.topology==f.revision.topology,
            gena_source_revision_matches=current.source==f.revision.source)
        return current,fields
    def recheck(self,who,f,*,stage='caller_recheck'):
        with self.lock:events=self.events.get(who)
        age=self.clock()-f.observed
        if not events or not 0<=age<=2:
            fields={'gena_peer':who,'gena_events_available':bool(events),
                'gena_snapshot_available':False,'gena_read_stage':stage,
                'peer_evidence_expiry_cause':('events_unavailable' if not events else
                    'age_nonfinite' if not math.isfinite(age) else
                    'clock_reversed' if age<0 else 'too_old')}
            if math.isfinite(age):
                clipped=max(-60.,min(60.,age));fields.update(peer_evidence_age_ms=round(clipped*1000),
                    peer_evidence_age_clipped=clipped!=age)
            if events:
                try:
                    _,snapshot=self._evidence_snapshot(events,who,f)
                    fields.update(snapshot);fields['gena_snapshot_available']=True
                except Exception:pass # Diagnostics cannot replace the original refusal.
            failure=BackendError('peer_evidence_expired');failure.evidence_diagnostics=fields
            raise failure
        current,fields=self._evidence_snapshot(events,who,f)
        if not current.continuous or not current.source or not current.topology or not current.uri_revision or current!=f.revision:
            fields.update(gena_events_available=True,gena_snapshot_available=True,gena_read_stage=stage,
                peer_evidence_age_ms=round(age*1000),peer_evidence_age_clipped=False)
            failure=BackendError('peer_evidence_changed');failure.evidence_diagnostics=fields
            failure.rejected_fact=f
            raise failure
        return f
    def candidate(self,session):
        # A solo tick never probes an absent/unselected/stale coordinator.
        with self.lock:ad=self.ad
        if ('coordinator' not in self.wire.peers or not session.media or not session.group
                or not ad.valid or ad.group!=session.group or not 0<=self.clock()-ad.observed<=ad.ttl):return None
        try:return self.read('coordinator')
        except (BackendError,EventError):return None
    def same_group(self,session,f):
        self.recheck('coordinator',f,stage='same_group')
        with self.lock:ad=self.ad
        native=AIRPLAY.fullmatch(f.peer.uri)
        return bool(session.media and session.state=='present' and session.group and ad.valid
            and 0<=self.clock()-ad.observed<=ad.ttl and ad.group==session.group and native
            and native[1]==f.peer.uuid and native[3].lower()==session.group.replace('-','').lower()
            and f.peer.transport in ('PLAYING','PAUSED_PLAYBACK'))
    def write(self,action,**values):return self.wire.write('target',action,**values)
    def close(self):
        self.done.set()
        if self.thread:self.thread.join(8)
        with self.lock:events=list(self.events.values());self.events.clear()
        for event in events:event.close()
