"""One-shot, allowlisted AirPlay TXT reader; Python standard library only.

No cache, discovery, credentials, record dumps or playback changes. Direct
unicast is the default. Optional multicast sends the same legacy query from
an ephemeral port and still accepts replies only from the configured peer.
Fresh response matching is not cryptographic authentication or session ownership.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import ipaddress
import json
import math
import re
import secrets
import socket
import struct
import time

MAX_PACKET = 4096
MAX_JUMPS = 16
MAX_RECORDS = 64
MAX_DATAGRAMS = 32
PORT = 5353
MULTICAST = '224.0.0.251'
ALLOWED_KEYS = frozenset((b'gid', b'deviceid', b'flags', b'pi', b'gcgl'))
PRIVATE_NETS = tuple(ipaddress.IPv4Network(x) for x in
                    ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))
VALUE = re.compile(rb'[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z')
GROUP_UUID = re.compile(r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\Z')
SUFFIX = '._airplay._tcp.local.'


class PacketError(ValueError):
    """Only fixed diagnostic codes, never packet bytes or TXT values."""


@dataclass(frozen=True)
class Target:
    ip: str
    instance: str
    labels: tuple[bytes, ...] = field(init=False, repr=False)

    def __post_init__(self):
        try:
            addr = ipaddress.IPv4Address(self.ip)
        except (ValueError, TypeError):
            raise ValueError('configured_private_ipv4_required') from None
        if not any(addr in network for network in PRIVATE_NETS):
            raise ValueError('configured_private_ipv4_required')
        if not isinstance(self.instance, str):
            raise ValueError('exact_airplay_instance_required')
        name = self.instance if self.instance.endswith('.') else self.instance + '.'
        if not name.lower().endswith(SUFFIX):
            raise ValueError('exact_airplay_instance_required')
        # The prefix is ONE literal DNS-SD instance label, including any dots.
        # No backslash/presentation escape interpretation is performed.
        instance = name[:-len(SUFFIX)]
        if not instance or '\\' in instance or any(ord(c) < 32 or ord(c) == 127 for c in instance):
            raise ValueError('invalid_instance_label')
        try:
            label = instance.encode('utf-8')
        except UnicodeError:
            raise ValueError('invalid_instance_label') from None
        if len(label) > 63:
            raise ValueError('invalid_instance_label')
        object.__setattr__(self, 'ip', str(addr))
        object.__setattr__(self, 'instance', name)
        object.__setattr__(self, 'labels', (label, b'_airplay', b'_tcp', b'local'))


def _canonical(labels):
    return tuple(label.lower() for label in labels)


def canonical_group_uuid(value: str | None) -> str | None:
    """Normalize UUID letter case only. MAC/self IDs and unset IDs are unknown.

    A canonical UUID is still not evidence of an active or owned session.
    """
    if not isinstance(value, str) or not GROUP_UUID.fullmatch(value):
        return None
    normalized = value.lower()
    return normalized if normalized != '00000000-0000-0000-0000-000000000000' else None


def same_group_uuid(left: str | None, right: str | None) -> bool | None:
    """Text comparison only, not membership/ownership. Unknown is not False."""
    left, right = canonical_group_uuid(left), canonical_group_uuid(right)
    return left == right if left is not None and right is not None else None


def _wire_name(labels):
    return b''.join(bytes([len(label)]) + label for label in labels) + b'\0'


def build_query(target: Target, transaction_id: int) -> bytes:
    if not 1 <= transaction_id <= 65535:
        raise ValueError('nonzero_transaction_id_required')
    return (struct.pack('!6H', transaction_id, 0, 1, 0, 0, 0)
            + _wire_name(target.labels) + struct.pack('!HH', 16, 1))


def _name(packet: bytes, start: int):
    """Bounded DNS name decoder. Returns label bytes and end of original name."""
    pos, end, jumps, expanded = start, None, 0, 1
    labels, seen = [], set()
    while True:
        if pos >= len(packet) or pos in seen:
            raise PacketError('invalid_name_offset_or_cycle')
        seen.add(pos)
        size = packet[pos]
        if size & 0xc0 == 0xc0:
            if pos + 1 >= len(packet):
                raise PacketError('truncated_name_pointer')
            pointer = ((size & 0x3f) << 8) | packet[pos + 1]
            jumps += 1
            if jumps > MAX_JUMPS or pointer < 12 or pointer >= pos:
                raise PacketError('invalid_name_pointer')
            if end is None:
                end = pos + 2
            pos = pointer
            continue
        if size & 0xc0:
            raise PacketError('unsupported_name_label')
        pos += 1
        if size == 0:
            return tuple(labels), end if end is not None else pos
        if pos + size > len(packet):
            raise PacketError('truncated_name_label')
        expanded += size + 1
        if expanded > 255:
            raise PacketError('expanded_name_too_long')
        labels.append(packet[pos:pos + size])
        pos += size


def _txt(raw: bytes) -> dict[str, str]:
    pos, result = 0, {}
    while pos < len(raw):
        size = raw[pos]
        pos += 1
        if pos + size > len(raw):
            raise PacketError('truncated_txt_string')
        item = raw[pos:pos + size]
        pos += size
        key, separator, value = item.partition(b'=')
        key = key.lower()
        if key not in ALLOWED_KEYS:
            continue  # Unknown values, including pk, are never decoded or output.
        safe_key = key.decode('ascii')
        if safe_key in result:
            raise PacketError('duplicate_allowed_txt_key')
        if not separator or not VALUE.fullmatch(value):
            raise PacketError('invalid_allowed_txt_value')
        result[safe_key] = value.decode('ascii')
    if 'gid' not in result:
        raise PacketError('gid_missing')
    return result


def parse_response(packet: bytes, target: Target, transaction_id: int):
    """Parse one legacy response. Source IP/port are checked by the caller."""
    if not 12 <= len(packet) <= MAX_PACKET:
        raise PacketError('invalid_packet_size')
    tid, flags, qd, an, ns, ar = struct.unpack_from('!6H', packet)
    if tid != transaction_id:
        raise PacketError('transaction_mismatch')
    if not flags & 0x8000 or flags & 0x7800 or flags & 0x0200 or flags & 0x000f:
        raise PacketError('invalid_response_flags')
    if qd != 1 or not an or an + ns + ar > MAX_RECORDS:
        raise PacketError('invalid_section_counts')
    question, pos = _name(packet, 12)
    if pos + 4 > len(packet):
        raise PacketError('truncated_question')
    qtype, qclass = struct.unpack_from('!HH', packet, pos)
    pos += 4
    expected = _canonical(target.labels)
    if _canonical(question) != expected or (qtype, qclass) != (16, 1):
        raise PacketError('question_mismatch')
    selected = None
    for index in range(an + ns + ar):
        owner, pos = _name(packet, pos)
        if pos + 10 > len(packet):
            raise PacketError('truncated_record_header')
        typ, cls, ttl, length = struct.unpack_from('!HHIH', packet, pos)
        pos += 10
        finish = pos + length
        if finish > len(packet):
            raise PacketError('truncated_record_data')
        if index < an and typ == 16 and _canonical(owner) == expected:
            if cls != 1 or not ttl:
                raise PacketError('invalid_txt_class_or_ttl')
            if selected is not None:
                raise PacketError('multiple_matching_txt_records')
            selected = (_txt(packet[pos:finish]), ttl)
        pos = finish
    if pos != len(packet):
        raise PacketError('trailing_packet_data')
    if selected is None:
        raise PacketError('matching_txt_missing')
    return selected


def read_gid(target: Target, *, timeout: float = 1.0, mode: str = 'unicast',
             _socket_factory=None, _monotonic=None, _wall_ns=None, _new_id=None) -> dict:
    """One fresh query, <=1 s, <=32 datagrams, <=4096 bytes per receive.

    Underscored injection points are for deterministic network-boundary tests.
    No process/global cache or hidden fallback. Importing the module does no I/O.
    """
    if not isinstance(timeout, (float, int)) or not math.isfinite(timeout) or not 0 < timeout <= 1:
        raise ValueError('timeout_must_be_positive_and_at_most_one_second')
    if mode not in ('unicast', 'multicast-legacy'):
        raise ValueError('unsupported_query_mode')
    factory = _socket_factory or socket.socket
    clock = _monotonic or time.monotonic
    wall = _wall_ns or time.time_ns
    fresh = _new_id or (lambda: secrets.randbelow(65535) + 1)
    start = clock()
    deadline = start + timeout
    transaction_id = fresh()
    query = build_query(target, transaction_id)
    base = {'status': 'unknown', 'source_ip': target.ip, 'instance': target.instance,
            'query_mode': mode, 'fields': {}, 'observed_at_unix_ns': None}

    def result(reason, fields=None, ttl=None):
        value = dict(base, reason=reason, elapsed_ms=round((clock() - start) * 1000, 3))
        if fields is not None:
            value.update(status='ok', fields=fields, ttl_seconds=ttl,
                         observed_at_unix_ns=wall())
        return value

    try:
        with factory(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(('', 0))
            if sock.getsockname()[1] in (0, PORT):
                return result('ephemeral_port_unavailable')
            destination = target.ip
            if mode == 'multicast-legacy':
                destination = MULTICAST
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
            else:
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, 255)
            remaining = deadline - clock()
            if remaining <= 0:
                return result('timeout')
            sock.settimeout(remaining)
            sock.sendto(query, (destination, PORT))
            last_reason = 'timeout'
            for _ in range(MAX_DATAGRAMS):
                remaining = deadline - clock()
                if remaining <= 0:
                    return result(last_reason)
                sock.settimeout(remaining)
                try:
                    if hasattr(sock, 'recvmsg'):
                        packet, _ancillary, message_flags, sender = sock.recvmsg(MAX_PACKET)
                        truncated = bool(message_flags & socket.MSG_TRUNC)
                    else:
                        packet, sender = sock.recvfrom(MAX_PACKET)
                        # Platforms without recvmsg cannot disambiguate an exact
                        # size fit from truncation: conservatively decline it.
                        truncated = len(packet) == MAX_PACKET
                except (socket.timeout, TimeoutError):
                    return result(last_reason)
                if clock() > deadline:
                    return result('timeout')
                if sender != (target.ip, PORT):
                    continue
                if truncated:
                    last_reason = 'truncated_datagram'
                    continue
                try:
                    fields, ttl = parse_response(packet, target, transaction_id)
                except PacketError as error:
                    last_reason = str(error)
                    continue
                return result('matched_fresh_response', fields, ttl)
            return result('datagram_limit')
    except OSError:
        return result('network_unavailable')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ip', required=True, help='Configured RFC1918 IPv4 literal')
    parser.add_argument('--instance', required=True,
                        help='Exact literal instance followed by ._airplay._tcp.local.')
    parser.add_argument('--timeout', type=float, default=1.0)
    parser.add_argument('--mode', choices=('unicast', 'multicast-legacy'), default='unicast')
    args = parser.parse_args(argv)
    try:
        result = read_gid(Target(args.ip, args.instance), timeout=args.timeout, mode=args.mode)
    except ValueError:
        print(json.dumps({'status': 'unknown', 'reason': 'invalid_configuration', 'fields': {}}))
        return 2
    print(json.dumps(result, sort_keys=True, ensure_ascii=True))
    return 0 if result['status'] == 'ok' else 1


if __name__ == '__main__':
    raise SystemExit(main())
