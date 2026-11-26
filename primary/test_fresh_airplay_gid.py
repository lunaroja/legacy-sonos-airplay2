"""Synthetic untrusted DNS packets and network boundaries; never opens sockets."""
import contextlib
import io
import json
from pathlib import Path
import random
import socket
import struct
import unittest
from unittest.mock import patch

import fresh_airplay_gid as reader

TARGET = reader.Target('192.168.50.20', 'Office speaker._airplay._tcp.local.')
OTHER = reader.Target('192.168.50.21', 'Roam._airplay._tcp.local.')
TID = 43123
SECRET_SENTINEL = b'DO_NOT_OUTPUT_PRIVATE_KEY_MARKER'


def txt(*items):
    return b''.join(bytes([len(item)]) + item for item in items)


def record(raw, *, owner=b'\xc0\x0c', typ=16, cls=1, ttl=10):
    return owner + struct.pack('!HHIH', typ, cls, ttl, len(raw)) + raw


def response(raw=None, *, target=TARGET, tid=TID, flags=0x8400,
             question=None, answers=None, authority=(), extra=()):
    if raw is None:
        raw = txt(b'gid=01234567-89ab-cdef-0123-456789abcdef', b'pk=' + SECRET_SENTINEL)
    if question is None:
        question = reader.build_query(target, tid)[12:]
    if answers is None:
        answers = [record(raw)]
    header = struct.pack('!6H', tid, flags, 1, len(answers), len(authority), len(extra))
    return header + question + b''.join(answers) + b''.join(authority) + b''.join(extra)


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class FakeSocket:
    def __init__(self, clock, queue=(), error=None, port=49160):
        self.clock, self.queue, self.error, self.port = clock, list(queue), error, port
        self.sent, self.timeouts, self.options = [], [], []
        self.closed, self.receives = False, 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def bind(self, value):
        self.bound = value
        if self.error:
            raise self.error

    def getsockname(self):
        return ('192.168.50.10', self.port)

    def setsockopt(self, *args):
        self.options.append(args)

    def settimeout(self, value):
        assert 0 < value <= 1
        self.timeouts.append(value)

    def sendto(self, query, endpoint):
        self.sent.append((query, endpoint))

    def recvmsg(self, size):
        assert size == 4096
        self.receives += 1
        if not self.queue:
            self.clock.now += self.timeouts[-1]
            raise socket.timeout()
        packet, sender, flags, delay = self.queue.pop(0)
        self.clock.now += delay
        return packet, [], flags, sender


def read_fake(queue=(), *, mode='unicast', error=None, port=49160):
    clock = Clock()
    sock = FakeSocket(clock, queue, error, port)
    value = reader.read_gid(TARGET, mode=mode, _socket_factory=lambda *_: sock,
                            _monotonic=clock, _wall_ns=lambda: 123456789,
                            _new_id=lambda: TID)
    return value, sock, clock


def incoming(packet=None, *, sender=(TARGET.ip, 5353), flags=0, delay=.01):
    return (response() if packet is None else packet, sender, flags, delay)


class PacketTests(unittest.TestCase):
    def parsed(self, packet):
        return reader.parse_response(packet, TARGET, TID)

    def rejected(self, packet, code=None):
        with self.assertRaises(reader.PacketError) as raised:
            self.parsed(packet)
        if code:
            self.assertEqual(str(raised.exception), code)
        self.assertNotIn(SECRET_SENTINEL.decode(), str(raised.exception))

    def test_query_exact(self):
        query = reader.build_query(TARGET, TID)
        self.assertEqual(struct.unpack('!6H', query[:12]), (TID, 0, 1, 0, 0, 0))
        self.assertEqual(query[-4:], b'\x00\x10\x00\x01')

    def test_minimal_allowlist_and_no_pk(self):
        fields, ttl = self.parsed(response())
        self.assertEqual(set(fields), {'gid'})
        self.assertEqual(ttl, 10)
        self.assertNotIn(SECRET_SENTINEL.decode(), json.dumps(fields))

    def test_all_allowed_keys_and_case(self):
        fields, _ = self.parsed(response(txt(b'GID=C4:01:02:03:04:05', b'DeviceID=C4:01:02:03:04:05',
                                            b'flags=0x244', b'pi=1234-abcd', b'gcgl=1', b'pk=HIDDEN')))
        self.assertEqual(set(fields), {'gid', 'deviceid', 'flags', 'pi', 'gcgl'})

    def test_case_insensitive_dns_matching(self):
        target = reader.Target(TARGET.ip, 'OFFICE SPEAKER._AIRPLAY._TCP.LOCAL.')
        self.parsed(response(target=target))

    def test_literal_dots_in_instance(self):
        target = reader.Target(TARGET.ip, 'Office. speaker._airplay._tcp.local.')
        fields, _ = reader.parse_response(response(target=target), target, TID)
        self.assertIn('gid', fields)
        self.assertEqual(reader.build_query(target, TID)[12], len(b'Office. speaker'))

    def test_uncompressed_owner(self):
        owner = reader._wire_name(TARGET.labels)
        self.parsed(response(answers=[record(txt(b'gid=abc'), owner=owner)]))

    def test_unknown_fields_never_decoded(self):
        fields, _ = self.parsed(response(txt(b'gid=abc', b'pk=\xff\xfe' + SECRET_SENTINEL,
                                            b'unknown=\x00\xff', b'novalue', b'')))
        self.assertEqual(fields, {'gid': 'abc'})

    def test_duplicate_allowed_key(self):
        for values in ((b'gid=abc', b'GID=abc'), (b'gid=abc', b'gid=def'),
                       (b'gid=abc', b'pi=x', b'PI=y')):
            with self.subTest(values=values):
                self.rejected(response(txt(*values)), 'duplicate_allowed_txt_key')

    def test_invalid_allowed_values(self):
        for value in (b'gid', b'gid=', b'gid=hello\nworld', b'gid=\xff',
                      b'gid=' + b'a' * 129, b'gid=a b', b'gid=<script>', b'gid=abc=def'):
            with self.subTest(value=value):
                self.rejected(response(txt(value)), 'invalid_allowed_txt_value')

    def test_missing_gid(self):
        self.rejected(response(txt(b'pi=abc', b'pk=' + SECRET_SENTINEL)), 'gid_missing')

    def test_all_truncations(self):
        valid = response()
        for i in range(len(valid)):
            with self.subTest(length=i):
                self.rejected(valid[:i])

    def test_malformed_txt_length(self):
        self.rejected(response(b'\xffgid=abc'), 'truncated_txt_string')

    def test_bad_flags(self):
        for flags in (0, 0x8600, 0x8401, 0x8c00):
            with self.subTest(flags=flags):
                self.rejected(response(flags=flags), 'invalid_response_flags')

    def test_transaction_and_question_mismatch(self):
        self.rejected(response(tid=TID - 1), 'transaction_mismatch')
        self.rejected(response(target=OTHER), 'question_mismatch')
        self.rejected(response(question=reader.build_query(TARGET, TID)[12:-4] + b'\0\1\0\1'),
                      'question_mismatch')

    def test_wrong_answer_owner(self):
        wrong = record(txt(b'gid=wrong'), owner=reader._wire_name(OTHER.labels))
        self.rejected(response(answers=[wrong]), 'matching_txt_missing')

    def test_irrelevant_records_do_not_leak(self):
        wrong = record(txt(b'gid=' + SECRET_SENTINEL), owner=reader._wire_name(OTHER.labels))
        fields, _ = self.parsed(response(answers=[wrong, record(txt(b'gid=abc'))],
                                        extra=[record(SECRET_SENTINEL, typ=99)]))
        self.assertEqual(fields, {'gid': 'abc'})

    def test_additional_answer_is_insufficient(self):
        self.rejected(response(answers=[record(b'1234', typ=1)],
                               extra=[record(txt(b'gid=abc'))]), 'matching_txt_missing')

    def test_no_alias_following(self):
        self.rejected(response(answers=[record(reader._wire_name(OTHER.labels), typ=5)],
                               extra=[record(txt(b'gid=abc'), owner=reader._wire_name(OTHER.labels))]),
                      'matching_txt_missing')

    def test_duplicate_rrset_rejected(self):
        self.rejected(response(answers=[record(txt(b'gid=abc'))] * 2),
                      'multiple_matching_txt_records')

    def test_goodbye_and_bad_class(self):
        for cls, ttl in ((1, 0), (3, 10), (0x8001, 10)):
            with self.subTest(cls=cls, ttl=ttl):
                self.rejected(response(answers=[record(txt(b'gid=abc'), cls=cls, ttl=ttl)]),
                              'invalid_txt_class_or_ttl')

    def test_excess_counts_and_trailing_data(self):
        for field, value in ((2, 0), (2, 2), (3, 0), (3, 65), (5, 65535)):
            header = list(struct.unpack('!6H', response()[:12]))
            header[field] = value
            self.rejected(struct.pack('!6H', *header) + response()[12:], 'invalid_section_counts')
        self.rejected(response() + b'\0', 'trailing_packet_data')

    def test_pointer_self_forward_header_and_reserved_label(self):
        start = len(reader.build_query(TARGET, TID))
        for owner in (struct.pack('!H', 0xc000 | start),
                      struct.pack('!H', 0xc000 | (start + 2)), b'\xc0\0', b'\x40', b'\x80'):
            with self.subTest(owner=owner):
                self.rejected(response(answers=[record(txt(b'gid=abc'), owner=owner)]))

    def test_pointer_chain_limit(self):
        packet = b'\0' * 12 + reader._wire_name(TARGET.labels)
        previous = 12
        for _ in range(16):
            start = len(packet)
            packet += struct.pack('!H', 0xc000 | previous)
            previous = start
        labels, end = reader._name(packet, previous)
        self.assertEqual(labels, TARGET.labels)
        self.assertEqual(end, previous + 2)
        start = len(packet)
        packet += struct.pack('!H', 0xc000 | previous)
        with self.assertRaises(reader.PacketError):
            reader._name(packet, start)

    def test_cycle_and_oversized_expansion(self):
        with self.assertRaises(reader.PacketError):
            reader._name(b'\0' * 12 + b'\x01a\xc0\x0c', 12)
        with self.assertRaises(reader.PacketError):
            reader._name(b'\0' * 12 + (b'\x3f' + b'a' * 63) * 4 + b'\0', 12)

    def test_4096_limit(self):
        base = response(extra=[record(b'', typ=99)])
        size = 4096 - len(base)
        self.parsed(response(extra=[record(b'x' * size, typ=99)]))
        self.rejected(response(extra=[record(b'x' * (size + 1), typ=99)]), 'invalid_packet_size')

    def test_deterministic_hostile_mutations_never_escape(self):
        rng = random.Random(373)
        original = response()
        for _ in range(1000):
            packet = bytearray(original)
            for _ in range(rng.randrange(1, 6)):
                packet[rng.randrange(len(packet))] = rng.randrange(256)
            try:
                fields, ttl = self.parsed(bytes(packet))
            except reader.PacketError as error:
                self.assertNotIn(SECRET_SENTINEL.decode(), str(error))
            else:
                self.assertLessEqual(set(fields), {'gid', 'deviceid', 'flags', 'pi', 'gcgl'})
                self.assertNotIn(SECRET_SENTINEL.decode(), json.dumps(fields))


class NetworkTests(unittest.TestCase):
    def test_recvfrom_platform_accepts_bounded_short_response(self):
        class FallbackSocket(FakeSocket):
            def __getattribute__(self, name):
                if name == 'recvmsg':
                    raise AttributeError(name)
                return super().__getattribute__(name)

            def recvfrom(self, size):
                packet, _, _, sender = FakeSocket.recvmsg(self, size)
                return packet, sender
        clock = Clock()
        sock = FallbackSocket(clock, [incoming()])
        value = reader.read_gid(TARGET, _socket_factory=lambda *_: sock,
                                _monotonic=clock, _new_id=lambda: TID)
        self.assertEqual(value['status'], 'ok')
        self.assertTrue(sock.closed)

    def test_exact_full_receive_buffer_requires_truncation_metadata(self):
        size = 4096 - len(response(extra=[record(b'', typ=99)]))
        packet = response(extra=[record(b'x' * size, typ=99)])
        value, _, _ = read_fake([incoming(packet)])
        self.assertEqual(value['status'], 'ok')
        class FallbackSocket(FakeSocket):
            def __getattribute__(self, name):
                if name == 'recvmsg':
                    raise AttributeError(name)
                return super().__getattribute__(name)

            def recvfrom(self, size):
                packet, _, _, sender = FakeSocket.recvmsg(self, size)
                return packet, sender
        clock = Clock()
        sock = FallbackSocket(clock, [incoming(packet)])
        value = reader.read_gid(TARGET, _socket_factory=lambda *_: sock,
                                _monotonic=clock, _new_id=lambda: TID)
        self.assertEqual(value['reason'], 'truncated_datagram')
        self.assertEqual(value['fields'], {})

    def test_success_and_close(self):
        value, sock, clock = read_fake([incoming()])
        self.assertEqual(value['status'], 'ok')
        self.assertEqual(value['source_ip'], TARGET.ip)
        self.assertEqual(value['observed_at_unix_ns'], 123456789)
        self.assertEqual(sock.bound, ('', 0))
        self.assertEqual(sock.sent, [(reader.build_query(TARGET, TID), (TARGET.ip, 5353))])
        self.assertTrue(sock.closed)
        self.assertNotIn(SECRET_SENTINEL.decode(), json.dumps(value))

    def test_wrong_source_and_port_ignored(self):
        value, sock, _ = read_fake([incoming(sender=(OTHER.ip, 5353)),
                                    incoming(sender=(TARGET.ip, 53)), incoming()])
        self.assertEqual(value['status'], 'ok')
        self.assertEqual(sock.receives, 3)

    def test_old_transaction_ignored_then_current_accepted(self):
        value, sock, _ = read_fake([incoming(response(tid=TID - 1)), incoming()])
        self.assertEqual(value['status'], 'ok')
        self.assertEqual(sock.receives, 2)

    def test_timeout_unknown_no_fallback(self):
        value, sock, clock = read_fake()
        self.assertEqual(value['status'], 'unknown')
        self.assertEqual(value['reason'], 'timeout')
        self.assertEqual(len(sock.sent), 1)
        self.assertEqual(value['fields'], {})
        self.assertEqual(clock.now, 101.0)
        self.assertTrue(sock.closed)

    def test_late_valid_reply_rejected(self):
        value, _, _ = read_fake([incoming(delay=1.001)])
        self.assertEqual((value['status'], value['reason']), ('unknown', 'timeout'))

    def test_truncated_datagram_not_parsed(self):
        value, _, _ = read_fake([incoming(flags=socket.MSG_TRUNC)])
        self.assertEqual(value['reason'], 'truncated_datagram')
        self.assertEqual(value['fields'], {})

    def test_packet_limit_and_deadline_not_extended(self):
        value, sock, _ = read_fake([incoming(sender=(OTHER.ip, 5353), delay=.001)] * 100)
        self.assertEqual(value['reason'], 'datagram_limit')
        self.assertEqual(sock.receives, 32)
        self.assertTrue(all(a >= b for a, b in zip(sock.timeouts, sock.timeouts[1:])))

    def test_network_exception_redacted(self):
        value, sock, _ = read_fake(error=OSError(SECRET_SENTINEL.decode()))
        self.assertEqual(value['reason'], 'network_unavailable')
        self.assertNotIn(SECRET_SENTINEL.decode(), json.dumps(value))
        self.assertTrue(sock.closed)

    def test_reserved_port_not_used(self):
        value, sock, _ = read_fake(port=5353)
        self.assertEqual(value['reason'], 'ephemeral_port_unavailable')
        self.assertEqual(sock.sent, [])

    def test_multicast_explicit_still_filters_sender(self):
        value, sock, _ = read_fake([incoming(sender=(OTHER.ip, 5353)), incoming()], mode='multicast-legacy')
        self.assertEqual(value['status'], 'ok')
        self.assertEqual(sock.sent[0][1], ('224.0.0.251', 5353))
        self.assertIn((socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255), sock.options)
        self.assertEqual(sock.bound, ('', 0))

    def test_fresh_ids_per_call(self):
        sockets = []
        def factory(*_):
            sock = FakeSocket(Clock())
            sockets.append(sock)
            return sock
        with patch.object(reader.socket, 'socket', factory), \
             patch.object(reader.secrets, 'randbelow', side_effect=[100, 200]) as entropy:
            reader.read_gid(TARGET)
            reader.read_gid(TARGET)
        self.assertEqual([struct.unpack('!H', s.sent[0][0][:2])[0] for s in sockets], [101, 201])
        self.assertEqual(entropy.call_count, 2)


class ConfigurationTests(unittest.TestCase):
    def test_uuid_case_comparison(self):
        value = '01234567-89ab-cdef-0123-456789abcdef'
        self.assertTrue(reader.same_group_uuid(value, value.upper()))
        self.assertFalse(reader.same_group_uuid(value, '11234567-89ab-cdef-0123-456789abcdef'))

    def test_non_uuid_unset_and_malformed_gid_are_unknown(self):
        value = '01234567-89ab-cdef-0123-456789abcdef'
        for candidate in (None, '', 'C4:01:02:03:04:05', 'c40102030405',
                          '00000000-0000-0000-0000-000000000000',
                          value + '\n', '{' + value + '}', 'urn:uuid:' + value, value.replace('-', '')):
            with self.subTest(candidate=candidate):
                self.assertIsNone(reader.same_group_uuid(candidate, value))
                self.assertIsNone(reader.same_group_uuid(candidate, candidate))

    def test_uuid_helpers_do_not_claim_activation(self):
        # Even an idle pi UUID can textually equal itself. Caller must separately
        # validate active-session evidence; no flags-derived policy lives here.
        value = '01234567-89ab-cdef-0123-456789abcdef'
        self.assertTrue(reader.same_group_uuid(value, value))
        self.assertEqual(reader.canonical_group_uuid(value.upper()), value)

    def test_nonprivate_or_hostname_rejected(self):
        for ip in ('8.8.8.8', '127.0.0.1', '169.254.1.1', '224.0.0.251', '0.0.0.0',
                   '255.255.255.255', 'example.com', '::1', '192.0.2.1', '100.64.1.1'):
            with self.subTest(ip=ip), self.assertRaises(ValueError):
                reader.Target(ip, TARGET.instance)

    def test_names_restricted_to_one_configured_airplay_instance(self):
        for instance in ('_airplay._tcp.local.', 'Office._raop._tcp.local.',
                         'Office._airplay._tcp.example.', 'x' * 64 + reader.SUFFIX,
                         'bad\nname' + reader.SUFFIX, 'bad\\032name' + reader.SUFFIX):
            with self.subTest(instance=instance), self.assertRaises(ValueError):
                reader.Target(TARGET.ip, instance)

    def test_bad_timeouts_no_socket(self):
        with patch.object(reader.socket, 'socket', side_effect=AssertionError('unexpected socket')):
            for value in (0, -1, 1.1, float('nan'), float('inf'), '1'):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    reader.read_gid(TARGET, timeout=value)

    def test_cli_json_and_status(self):
        for status, exitcode in (('ok', 0), ('unknown', 1)):
            out = io.StringIO()
            with patch.object(reader, 'read_gid', return_value={'status': status, 'fields': {}}), \
                 contextlib.redirect_stdout(out):
                result = reader.main(['--ip', TARGET.ip, '--instance', TARGET.instance])
            self.assertEqual(result, exitcode)
            self.assertEqual(json.loads(out.getvalue())['status'], status)

    def test_cli_invalid_config_redacted(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            result = reader.main(['--ip', SECRET_SENTINEL.decode(), '--instance', TARGET.instance])
        self.assertEqual(result, 2)
        self.assertNotIn(SECRET_SENTINEL.decode(), out.getvalue())


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromModule(__import__(__name__))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    if result.wasSuccessful():
        Path(__file__).with_name('fresh-airplay-gid-test-results.json').write_text(
            json.dumps({'tests_passed': result.testsRun, 'synthetic_mutation_cases': 1000,
                        'live_requests_by_test_suite': 0, 'dependencies': 'Python standard library'}, indent=2) + '\n')
    raise SystemExit(0 if result.wasSuccessful() else 1)
