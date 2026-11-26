"""Bounded SPTS v2 framing only; no native sender or clock conversion."""
from dataclasses import dataclass
import struct
HEADER=struct.Struct('>4sBBHIIQQII')
TIMED,UNTIMED,FLUSH,STOP=1,2,3,4
class Fault(ValueError):pass

@dataclass(frozen=True)
class Record:
    kind: int
    epoch: int
    sequence: int
    pts_ns: int
    observed_ns: int
    frames: int
    rtp: int
    pcm: bytes


class Records:
    """Incremental equivalent of timing prototype v2 read_packet; no scanning."""
    def __init__(self):
        self.buffer = bytearray()
        self.partial_since = None

    def feed(self, data, now):
        if len(self.buffer) + len(data) > 100_000:
            raise Fault('input_buffer_limit')
        self.buffer.extend(data)
        result = []
        while len(self.buffer) >= HEADER.size:
            magic, version, kind, bpf, epoch, seq, pts, observed, frames, rtp = HEADER.unpack_from(self.buffer)
            if (magic != b'SPTS' or version != 2 or bpf != 4 or kind not in range(1, 5)
                    or frames > 8192 or (kind == TIMED and (not frames or not pts))
                    or (kind >= FLUSH and frames)):
                raise Fault('invalid_source_record')
            size = HEADER.size + (frames * 4 if kind <= UNTIMED else 0)
            if len(self.buffer) < size:
                break
            result.append(Record(kind, epoch, seq, pts, observed, frames, rtp,
                                 bytes(self.buffer[HEADER.size:size])))
            del self.buffer[:size]
        if not self.buffer:
            self.partial_since = None
        elif self.partial_since is None or result:
            self.partial_since = now
        return result

    def check(self, now):
        if self.partial_since is not None and now - self.partial_since > 500_000_000:
            raise Fault('partial_record_timeout')
