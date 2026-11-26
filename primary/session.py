"""Generic authenticated receiver session and framed PCM continuity; no remote policy."""
from __future__ import annotations
from collections import OrderedDict
from dataclasses import dataclass,field
import math,secrets,struct,threading,time,uuid
EVENT=struct.Struct('!4sBBHQQQIIIIQII16siI')
MAX_AGE=2.0

@dataclass(frozen=True)
class Event:
    event: int
    flags: int
    serial: int
    generation: int = field(repr=False)
    observed: float
    epoch: int
    first: int
    sequence: int
    kind: int
    pts: int = field(repr=False)
    frames: int
    rtp: int = field(repr=False)
    group: str | None = field(repr=False)

    volume_milli: int = 0
    volume_revision: int = 0

    @classmethod
    def parse(cls, packet):
        if not isinstance(packet, bytes) or len(packet) != EVENT.size:
            raise ValueError('event_size')
        magic, version, event, flags, serial, gen, ns, epoch, first, seq, kind, pts, frames, rtp, gid, volume_milli, volume_revision = EVENT.unpack(packet)
        if (magic != b'SPSE' or version != 2 or not 1 <= event <= 7 or flags & ~31
                or not serial or not ns or kind > 4 or frames > 8192):
            raise ValueError('event_shape')
        if (volume_revision and not (volume_milli == -144000 or -30000 <= volume_milli <= 0)) or (not volume_revision and volume_milli):
            raise ValueError('volume_shape')
        if not gen and volume_revision:
            raise ValueError('volume_without_generation')
        group = str(uuid.UUID(bytes=gid)) if gid != bytes(16) else None
        if bool(flags & 2) != bool(group) or first > seq or (bool(epoch) != bool(first)):
            raise ValueError('event_context')
        if ((not gen and (event != 1 or flags or seq or epoch or kind))
                or (gen and event == 1) or (seq == 0 and (kind or epoch or first or pts or frames or rtp))):
            raise ValueError('event_generation')
        if kind in (3, 4) and (pts or frames or rtp):
            raise ValueError('control_payload')
        if kind in (1, 2) and (not frames or bool(pts) != (kind == 1)):
            raise ValueError('audio_header')
        return cls(event, flags, serial, gen, ns / 1e9, epoch, first, seq, kind, pts, frames, rtp, group, volume_milli, volume_revision)


class Observer:
    def __init__(self,*,clock=time.monotonic,changed=lambda:None):
        self.clock,self.changed=clock,changed;self.lock=threading.RLock()
        self.incarnation=secrets.token_hex(16);self.failed=False;self.lineage_at=None
        self.failure=None
        self.serial=self.revision=self.record_sequence=self.record_epoch=0
        self.current=None;self.records=OrderedDict();self.bound=None
        self.fifo_stopped=True;self.last_timed=None
    def lineage_checked(self):
        # Only SessionChannel calls this after checking its pinned Popen/FIFO
        # attachment and draining credential-checked IPC. No event is re-dated.
        with self.lock:
            if not self.failed:self.lineage_at=self.clock()

    def invalidate(self, failure=None):
        with self.lock:
            if not self.failed:
                self.failure = failure
            self.failed = True
            self.lineage_at = None
            self.bound = self.last_timed = None
            self.revision += 1
        self.changed()

    def feed(self, packet):
        try:
            event = Event.parse(packet)
            with self.lock:
                if self.failed:
                    return False
                prior = self.current
                if (event.serial != self.serial+1 or not 0 <= self.clock()-event.observed <= MAX_AGE
                        or (prior and event.observed < prior.observed)
                        or (prior and event.generation < prior.generation)):
                    raise ValueError('event_continuity')
                if prior is None and event.event != 1:
                    raise ValueError('hello_required')
                if prior and event.generation != prior.generation:
                    if event.event != 2 or event.generation != prior.generation+1 or prior.flags & 1:
                        raise ValueError('generation_transition')
                    self.bound = self.last_timed = None
                elif prior and event.generation and (event.group != prior.group or event.sequence < prior.sequence):
                    raise ValueError('generation_context_changed')
                if prior and event.generation == prior.generation and (event.volume_revision < prior.volume_revision or (event.volume_revision == prior.volume_revision and event.volume_milli != prior.volume_milli)):
                    raise ValueError('volume_revision_continuity')
                if event.flags & 16:
                    raise ValueError('receiver_observer_failed')
                self.current, self.serial = event, event.serial
                self.revision += 1
                self._bind()
            self.changed()
            return True
        except (ValueError, TypeError, OverflowError) as error:
            self.invalidate(error.args[0] if error.args in (
                ('event_size',), ('event_shape',), ('volume_shape',), ('volume_without_generation',),
                ('event_context',), ('event_generation',), ('control_payload',), ('audio_header',),
                ('event_continuity',), ('hello_required',), ('generation_transition',),
                ('generation_context_changed',), ('volume_revision_continuity',),
                ('receiver_observer_failed',), ('fifo_event_mismatch',)) else 'event_invalid')
            return False

    def observe_record(self, record):
        """Call after the worker validates SPTS framing; never consume FIFO twice."""
        try:
            k, e, s = record.kind, record.epoch, record.sequence
            p, f, r = record.pts_ns, record.frames, record.rtp
            with self.lock:
                if self.failed:
                    return False
                if (any(type(x) is not int for x in (k,e,s,p,f,r)) or k not in (1,2,3,4)
                        or not 0 < s <= 0xffffffff or s != self.record_sequence+1
                        or not 0 < e <= 0xffffffff or not 0 <= r <= 0xffffffff
                        or not 0 <= p <= 0xffffffffffffffff or not 0 <= f <= 8192):
                    raise ValueError('fifo_sequence')
                if k == 3:
                    if e <= self.record_epoch or p or f or r:
                        raise ValueError('fifo_flush')
                    self.record_epoch, self.fifo_stopped = e, False
                    self.bound = self.last_timed = None
                elif e != self.record_epoch or self.fifo_stopped:
                    raise ValueError('fifo_boundary_required')
                elif k == 4:
                    if p or f or r:
                        raise ValueError('fifo_stop')
                    self.fifo_stopped = True
                    self.last_timed = None
                elif not f or len(record.pcm) != f*4 or bool(p) != (k == 1):
                    raise ValueError('fifo_pcm')
                at = self.clock()
                self.record_sequence = s
                self.records[(e,s)] = ((k,p,f,r),at)
                while len(self.records) > 128:
                    self.records.popitem(last=False)
                self._bind()
                if k == 1 and self.current and self.bound == (self.current.generation,e,self.current.first):
                    self.last_timed = (self.current.generation,e,at)
                if k >= 3:
                    self.revision += 1
            if k >= 3:
                self.changed()
            return True
        except (AttributeError, ValueError, TypeError, OverflowError):
            self.invalidate()
            return False

    def _bind(self):
        e = self.current
        if not e or not e.generation or not e.first or self.fifo_stopped or e.epoch != self.record_epoch:
            return
        boundary = self.records.get((e.epoch,e.first))
        tip = self.records.get((e.epoch,e.sequence))
        if self.bound != (e.generation,e.epoch,e.first):
            if not boundary or boundary[0] != (3,0,0,0):
                return
        if not tip:
            return  # IPC can precede the separately buffered FIFO reader.
        if tip[0] != (e.kind,e.pts,e.frames,e.rtp):
            raise ValueError('fifo_event_mismatch')
        self.bound = (e.generation,e.epoch,e.first)
        # A TIMED frame may have arrived before its sideband snapshot.
        timed = [at for (epoch,seq),((kind,_,_,_),at) in self.records.items()
                 if epoch==e.epoch and seq>=e.first and kind==1]
        if timed:
            self.last_timed = (e.generation,e.epoch,max(timed))


    def evidence(self):
        with self.lock:
            e=self.current;now=self.clock()
            fresh=lambda t: t is not None and 0<=now-t<=MAX_AGE
            alive=not self.failed and fresh(self.lineage_at)
            generation=f'{self.incarnation}:{e.generation}' if e and e.generation else ''
            state='lost' if self.failed else 'unproven'
            if alive and e and e.generation:
                if not e.flags&1 and e.event==5:state='ended'
                elif e.flags&15==15:state='present'
            media=bool(state=='present' and fresh(e.observed) and not self.fifo_stopped
                and self.bound==(e.generation,e.epoch,e.first) and self.last_timed
                and self.last_timed[:2]==(e.generation,e.epoch) and fresh(self.last_timed[2]))
            return Session(generation,state,media,e.group if e else None,
                e.volume_revision if e else 0,e.volume_milli if e else 0,
                e.observed if e else 0,self.revision,end_event_confirmed=state=='ended')

@dataclass(frozen=True)
class Session:
    generation:str
    state:str
    media:bool
    group:str|None
    volume_revision:int
    volume_milli:int
    observed:float
    revision:int
    end_event_confirmed:bool=False
