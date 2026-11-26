"""Replay Music route and transport sequences through the production parsers."""

from dataclasses import replace
import json
import time
import unittest
import uuid

from framed_pcm import HEADER, Records
from peer_runtime import Advertisement
from session import EVENT, Observer
from test_primary import C, G, NATIVE, T
import test_wire

SOLO_GROUP = '22345678-1234-1234-1234-123456789abc'


class ReceiverTrace:
    """Encode the receiver's events and PCM using synthetic session identifiers."""

    def __init__(self, clock, require):
        self.clock = clock
        self.require = require
        self.observer = Observer(clock=clock)
        self.parser = Records()
        self.generation = self.serial = self.sequence = self.epoch = self.first = 0
        self.kind = self.pts = self.frames = self.rtp = 0
        self.group = None
        self.volume_revision = self.volume_milli = 0
        self.event(1, 0)

    def event(self, kind, flags):
        self.serial += 1
        group = uuid.UUID(self.group).bytes if self.group else bytes(16)
        self.require(self.observer.feed(EVENT.pack(
            b'SPSE', 2, kind, flags | (2 if self.group else 0), self.serial,
            self.generation, int(self.clock() * 1e9) - 10, self.epoch, self.first,
            self.sequence, self.kind, self.pts, self.frames, self.rtp, group,
            self.volume_milli, self.volume_revision,
        )))
        self.observer.lineage_checked()

    def record(self, kind):
        self.sequence += 1
        self.kind = kind
        if kind == 3:
            self.epoch += 1
            self.first = self.sequence
        now = int(self.clock() * 1e9)
        self.pts = now + 2_000_000_000 if kind == 1 else 0
        self.frames = 352 if kind == 1 else 0
        self.rtp = self.sequence * 352 if self.frames else 0
        packet = HEADER.pack(b'SPTS', 2, kind, 4, self.epoch, self.sequence,
                             self.pts, now, self.frames, self.rtp)
        for record in self.parser.feed(packet + b'\x01\x00\x02\x00' * self.frames, now):
            self.require(self.observer.observe_record(record))

    def start(self, group):
        self.generation += 1
        self.group = group
        self.volume_revision = self.volume_milli = 0
        self.event(2, 9)
        self.record(3)
        self.event(3, 13)
        self.media()

    def media(self):
        self.record(1)
        self.event(4, 13)

    def flush(self):
        self.record(3)
        self.event(4, 13)

    def volume(self, milli):
        self.volume_revision += 1
        self.volume_milli = milli
        self.event(7, 13)

    def end(self):
        self.record(4)
        self.event(5, 12)


class PlaybackScenarios(unittest.TestCase):
    fixture = test_wire.WireIntegration.fixture

    def setUp(self):
        self.controller = self.fixture()
        self.now = time.monotonic()
        self.receiver = ReceiverTrace(self.clock, self.assertTrue)
        self.controller.session = self.receiver.observer.evidence
        self.controller.clock = self.peers.clock = self.peers.wire.clock = self.clock
        self.controller.sleep = self.advance
        for events in self.peers.events.values():
            events.clock = self.clock

    def clock(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds
        self.receiver.observer.lineage_checked()

    def roam(self, *, uri, transport):
        self.model.update('coordinator', uri=uri, transport=transport)
        self.model.emit_all()
        self.peers.ad = Advertisement(G if uri else None, self.now, True)

    def writes(self):
        return [(who, action) for who, action, _ in self.model.all_calls
                if not action.startswith('Get')]

    def rows(self):
        return [json.loads(line) for line in self.store.audit.read_text().splitlines()]

    def start_native(self):
        self.roam(uri=NATIVE, transport='PLAYING')
        self.receiver.start(G)
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.model.peers['target'].uri, 'x-rincon:' + C)

    def test_office_plays_alone_then_music_adds_roam_with_a_new_generation(self):
        self.roam(uri='', transport='STOPPED')
        self.receiver.start(SOLO_GROUP)
        self.assertEqual(self.controller.tick(), 'solo')
        standalone = self.model.peers['target'].uri
        self.assertTrue(standalone.endswith('.wav'))
        self.assertTrue(self.model.peers['coordinator'].idle())

        self.receiver.end()
        self.receiver.start(G)
        self.roam(uri=NATIVE, transport='PLAYING')
        self.assertEqual(self.controller.tick(), 'idle')
        def change_roam_gain(action):
            if action == 'SetAVTransportURI':
                self.model.update('coordinator', volume=46)
        self.model.after = change_roam_gain
        self.assertEqual(self.controller.tick(), 'both')
        self.assertFalse(self.wave.path)
        self.assertEqual(self.model.peers['target'].uri, 'x-rincon:' + C)
        self.assertEqual(self.model.peers['coordinator'].uri, NATIVE)
        self.assertEqual(self.model.peers['coordinator'].volume, 46)
        self.assertTrue(all(who == 'target' for who, _ in self.writes()))

    def test_solo_end_waits_for_stopped_event_before_starting_native_group(self):
        self.roam(uri='', transport='STOPPED')
        self.receiver.start(SOLO_GROUP)
        self.assertEqual(self.controller.tick(), 'solo')
        self.receiver.end()
        self.receiver.start(G)
        self.roam(uri=NATIVE, transport='PLAYING')
        self.model.update('target', transport='STOPPED')
        before = self.writes()
        def deliver_stop(seconds):
            self.assertEqual(self.writes(), before)
            self.model.emit_all()
            self.controller.sleep = self.advance
            self.advance(seconds)
        self.controller.sleep = deliver_stop
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertEqual(self.controller.tick(), 'both')
        self.assertTrue(any(row.get('retry_kind') == 'solo_target_stop_event_pending'
                            for row in self.rows()))

    def test_add_roam_with_stop_status_before_old_receiver_session_ends(self):
        self.roam(uri='', transport='STOPPED')
        self.receiver.start(SOLO_GROUP)
        self.assertEqual(self.controller.tick(), 'solo')
        self.roam(uri=NATIVE, transport='PLAYING')
        self.model.update('target', transport='STOPPED')
        self.assertTrue(self.controller._local())
        before = self.writes()

        def finish_route_change(seconds):
            self.assertEqual(self.writes(), before)
            self.receiver.end()
            self.receiver.start(G)
            self.model.emit_all()
            self.controller.sleep = self.advance
            self.advance(seconds)

        self.controller.sleep = finish_route_change
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.model.peers['target'].uri, 'x-rincon:' + C)
        self.assertEqual(self.model.peers['coordinator'].transport, 'PLAYING')

    def test_grouped_resume_waits_when_stopped_event_outlasts_first_retry(self):
        self.start_native()
        self.receiver.flush()
        self.model.update('target', transport='STOPPED')
        self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertTrue(self.controller.native_hold)
        self.receiver.media()
        self.model.update('target', transport='PLAYING')
        self.roam(uri=NATIVE, transport='PLAYING')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertFalse(self.controller.native_hold)
        self.model.update('target', transport='STOPPED')
        self.model.emit_all()
        self.model.update('target', transport='PLAYING')
        before = self.writes()
        waits = []

        def settle_resume(seconds):
            self.assertEqual(self.writes(), before)
            waits.append(seconds)
            if len(waits) == 2:
                self.model.emit_all()
            self.advance(seconds)

        self.controller.sleep = settle_resume
        self.assertEqual(self.controller.tick(), 'both')
        self.assertFalse(self.controller.native_hold)
        self.assertEqual(len(waits), 2)
        self.assertEqual(self.writes(), before)
        self.assertTrue(any(row.get('retry_kind') == 'native_target_resume_event_pending'
                            and row['event'] == 'fresh_evidence_retry' for row in self.rows()))

    def test_grouped_stop_with_stale_playing_event_waits_for_owned_cleanup(self):
        self.start_native()
        self.receiver.flush()
        self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
        self.model.update('target', transport='STOPPED')
        before = self.writes()

        def settle_stop(seconds):
            self.assertEqual(self.writes(), before)
            self.receiver.end()
            self.model.emit_all()
            self.controller.sleep = self.advance
            self.advance(seconds)

        self.controller.sleep = settle_stop
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].standalone())
        self.assertEqual(self.model.peers['coordinator'].uri, NATIVE)
        self.assertEqual(self.model.peers['coordinator'].transport, 'PAUSED_PLAYBACK')
        self.assertTrue(any(row.get('retry_kind') == 'native_target_stop_event_pending'
                            and row['event'] == 'fresh_evidence_cleanup_proven' for row in self.rows()))

    def test_solo_volume_read_conflict_defers_write_until_fresh_evidence(self):
        self.roam(uri='', transport='STOPPED')
        self.receiver.start(SOLO_GROUP)
        self.assertEqual(self.controller.tick(), 'solo')
        self.receiver.volume(-15000)
        before = self.writes()
        volume = self.controller._volume

        def change_target_event_before_volume():
            self.model.update('target', transport='STOPPED')
            self.model.emit_all()
            self.model.update('target', transport='PLAYING')
            return volume()

        self.controller._volume = change_target_event_before_volume
        self.assertEqual(self.controller.tick(), 'solo')
        self.assertEqual(self.writes(), before)
        failure = self.rows()[-1]
        self.assertEqual(failure['event'], 'volume_deferred')
        self.assertFalse(failure['speaker_command_sent'])
        self.assertEqual(failure['controller_operation'], 'SetVolume')
        self.assertEqual(failure['controller_phase'], 'before_intent')
        self.assertEqual(failure['retry_decision'], 'defer_unsent_volume')
        self.assertEqual(failure['checkpoint_phase'], 'verified')
        self.assertEqual(failure['gena_transport'], 'STOPPED')
        self.assertEqual(failure['soap_transport'], 'PLAYING')

        self.controller._volume = volume
        self.model.emit_all()
        self.assertEqual(self.controller.tick(), 'solo')
        self.assertEqual(len(self.writes()), len(before) + 1)
        self.assertEqual(self.controller.volume_revision, self.receiver.volume_revision)

    def test_solo_stop_waits_when_event_is_ahead_of_direct_transport_read(self):
        self.roam(uri='', transport='STOPPED')
        self.receiver.start(SOLO_GROUP)
        self.assertEqual(self.controller.tick(), 'solo')
        self.receiver.end()
        self.model.update('target', transport='STOPPED')
        self.model.emit_all()
        self.model.update('target', transport='PLAYING')
        before = self.writes()

        def settle_transport(seconds):
            self.assertEqual(self.writes(), before)
            self.model.update('target', transport='STOPPED')
            self.controller.sleep = self.advance
            self.advance(seconds)

        self.controller.sleep = settle_transport
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())

    def test_solo_stop_wait_does_not_claim_a_replaced_source(self):
        self.roam(uri='', transport='STOPPED')
        self.receiver.start(SOLO_GROUP)
        self.assertEqual(self.controller.tick(), 'solo')
        self.receiver.end()
        self.model.update('target', transport='STOPPED')
        before = self.writes()
        def deliver_foreign_source(seconds):
            self.model.update('target', uri='http://foreign.invalid/stream')
            self.model.emit_all()
            self.advance(seconds)
        self.controller.sleep = deliver_foreign_source
        self.assertEqual(self.controller.tick(), 'recovery')
        self.assertEqual(self.writes(), before)
        self.assertEqual(self.model.peers['target'].uri, 'http://foreign.invalid/stream')

    def test_remove_roam_with_delayed_end_event_and_fresh_office_generation(self):
        self.start_native()
        self.model.update('coordinator', uri='', transport='STOPPED')
        self.receiver.end()
        self.receiver.start(SOLO_GROUP)
        before = self.writes()
        def deliver_end(seconds):
            self.assertEqual(self.writes(), before)
            self.roam(uri='', transport='STOPPED')
            self.controller.sleep = self.advance
            self.advance(seconds)
        self.controller.sleep = deliver_end
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertEqual(self.controller.tick(), 'solo')
        self.assertEqual(self.model.peers['target'].transport, 'PLAYING')
        self.assertTrue(self.model.peers['target'].standalone())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertTrue(any(row['event'] == 'fresh_evidence_cleanup_proven'
                            and row['retry_kind'] == 'coordinator_end_event_pending'
                            for row in self.rows()))

    def test_stop_after_pause_accepts_end_event_arriving_during_group_read(self):
        self.start_native()
        self.receiver.flush()
        self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
        self.assertEqual(self.controller.tick(), 'both')
        self.receiver.end()
        before = self.writes()
        read = self.peers.read
        def race(who):
            fact = read(who)
            if who == 'coordinator':
                self.assertEqual(self.writes(), before)
                self.peers.read = read
                self.roam(uri='', transport='STOPPED')
            return fact
        self.peers.read = race
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertTrue(any(row['event'] == 'fresh_evidence_cleanup_proven'
                            and row['retry_kind'] == 'coordinator_end_event_pending'
                            for row in self.rows()))

    def test_stop_after_pause_waits_when_roam_event_still_says_paused(self):
        self.start_native()
        self.receiver.flush()
        self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
        self.assertEqual(self.controller.tick(), 'both')
        self.receiver.end()
        self.model.update('coordinator', uri='', transport='STOPPED')
        before = self.writes()
        def deliver_end(seconds):
            self.assertEqual(self.writes(), before)
            self.roam(uri='', transport='STOPPED')
            self.controller.sleep = self.advance
            self.advance(seconds)
        self.controller.sleep = deliver_end
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertTrue(any(row['event'] == 'fresh_evidence_cleanup_proven'
                            and row['retry_kind'] == 'coordinator_end_event_pending'
                            for row in self.rows()))

    def test_stop_after_pause_waits_when_roam_stop_read_retains_source(self):
        self.start_native()
        self.receiver.flush()
        self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
        self.assertEqual(self.controller.tick(), 'both')
        self.receiver.end()
        self.model.update('coordinator', transport='STOPPED')
        before = self.writes()
        def deliver_end(seconds):
            self.assertEqual(self.writes(), before)
            self.roam(uri='', transport='STOPPED')
            self.controller.sleep = self.advance
            self.advance(seconds)
        self.controller.sleep = deliver_end
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertTrue(any(row['event'] == 'fresh_evidence_cleanup_proven'
                            and row['retry_kind'] == 'coordinator_end_event_pending'
                            for row in self.rows()))

    def test_roam_ends_after_paused_release_is_authorized_before_detach(self):
        self.start_native()
        self.receiver.flush()
        self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
        self.assertEqual(self.controller.tick(), 'both')
        self.receiver.end()
        append_event = self.store.event
        def event(name, **data):
            append_event(name, **data)
            if name == 'owned_paused_release_authorized':
                self.roam(uri='', transport='STOPPED')
        self.store.event = event
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertEqual(self.writes().count(('target', 'BecomeCoordinatorOfStandaloneGroup')), 1)
        self.assertTrue(any(row['event'] == 'paused_release_end_recheck' for row in self.rows()))

    def test_roam_ends_while_paused_release_is_being_built(self):
        self.start_native()
        self.receiver.flush()
        self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
        self.assertEqual(self.controller.tick(), 'both')
        self.receiver.end()
        begin = self.peers.begin_owned_paused_release
        def race(*args):
            release = begin(*args)
            self.roam(uri='', transport='STOPPED')
            return release
        self.peers.begin_owned_paused_release = race
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertTrue(any(row['event']=='paused_context_end_recheck' for row in self.rows()))

    def test_rapid_stop_after_detach_ack_finalizes_only_when_office_is_idle(self):
        for idle in (True, False):
            with self.subTest(target_idle=idle):
                self.doCleanups()
                self.setUp()
                self.start_native()
                self.receiver.flush()
                self.roam(uri=NATIVE, transport='PAUSED_PLAYBACK')
                self.assertEqual(self.controller.tick(), 'both')
                self.receiver.end()
                append_event = self.store.event
                def event(name, **data):
                    append_event(name, **data)
                    if name == 'acknowledged' and data['action'] == 'BecomeCoordinatorOfStandaloneGroup':
                        self.roam(uri='', transport='STOPPED')
                        if idle:
                            self.model.update('target', uri='', transport='STOPPED')
                            self.model.emit_all()
                self.store.event = event
                self.assertEqual(self.controller.tick(), 'idle' if idle else 'recovery')
                self.assertEqual(self.writes(), [('target','SetAVTransportURI'),
                                                ('target','BecomeCoordinatorOfStandaloneGroup')])
                self.assertEqual(self.store.data['active'] is None, idle)
                self.assertEqual(any(r['event']=='paused_detach_already_idle' for r in self.rows()), idle)

    def test_remove_office_mid_track_keeps_roam_playing_and_reselects(self):
        self.start_native()
        original = self.model.peers['coordinator']
        self.receiver.end()
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertEqual(self.model.peers['coordinator'], replace(original, members=frozenset((C,))))

        self.peers.ad = Advertisement(G, self.now, True)
        self.receiver.start(G)
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.model.peers['coordinator'], original)
        self.assertEqual(self.model.peers['target'].members, frozenset((T, C)))

    def test_skip_flush_and_pause_resume_keep_the_same_native_source(self):
        self.start_native()
        original = self.writes()
        source = self.model.peers['coordinator'].uri
        self.receiver.flush()
        self.receiver.media()
        self.assertEqual(self.controller.tick(), 'both')
        self.receiver.flush()
        self.roam(uri=source, transport='PAUSED_PLAYBACK')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertTrue(self.controller.native_hold)
        self.receiver.media()
        self.roam(uri=source, transport='PLAYING')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertFalse(self.controller.native_hold)
        self.assertEqual(self.model.peers['coordinator'].uri, source)
        self.assertEqual(self.writes(), original)

    def test_solo_handoff_survives_acknowledged_gain_that_never_changes(self):
        self.start_native()
        self.receiver.end()
        self.roam(uri='', transport='STOPPED')
        self.receiver.start(SOLO_GROUP)
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertEqual(self.controller.tick(), 'solo')
        initial_gain = self.model.peers['target'].volume
        self.receiver.volume(-15000)
        before = self.writes()

        def keep_observed_gain(action):
            if action == 'SetVolume':
                self.model.update('target', volume=initial_gain)

        self.model.after = keep_observed_gain
        self.assertEqual(self.controller.tick(), 'solo')
        self.assertEqual(self.writes()[len(before):], [('target', 'SetVolume')])
        self.assertEqual(self.controller.tick(), 'solo')
        self.assertEqual(self.writes()[len(before):], [('target', 'SetVolume')])
        self.assertTrue(any(row['event'] == 'solo_volume_readback_unconfirmed' for row in self.rows()))
        self.assertEqual(self.model.peers['target'].transport, 'PLAYING')
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.receiver.end()
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())

    def test_roam_source_ends_during_acknowledged_volume_without_replay(self):
        self.start_native()
        initial_gain = self.model.peers['target'].volume
        self.receiver.volume(-15000)
        before = self.writes()

        def end_during_volume(action):
            if action == 'SetVolume':
                self.model.update('target', volume=initial_gain)
                self.roam(uri='', transport='STOPPED')
                self.receiver.end()

        self.model.after = end_during_volume
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.writes()[len(before):], [('target', 'SetVolume')])
        self.assertEqual(self.store.data['active']['phase'], 'verified')
        self.assertTrue(any(row['event'] == 'volume_acknowledged_during_native_end' for row in self.rows()))
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertEqual(self.writes().count(('target', 'SetVolume')), 1)

    def test_stop_during_acknowledged_volume_readback_does_not_replay_volume(self):
        self.start_native()
        initial_gain = self.model.peers['target'].volume
        self.receiver.volume(-15000)
        before = self.writes()

        def stop_during_volume(action):
            if action == 'SetVolume':
                self.model.update('target', volume=initial_gain, transport='STOPPED')
                self.roam(uri=NATIVE, transport='STOPPED')
                self.receiver.end()

        self.model.after = stop_during_volume
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.writes()[len(before):], [('target', 'SetVolume')])
        self.assertEqual(self.store.data['active']['phase'], 'verified')
        row = next(row for row in self.rows() if row['event'] == 'volume_acknowledged_during_native_stop')
        self.assertFalse(row['requested_gain_observed'])

        self.roam(uri='', transport='STOPPED')
        self.assertEqual(self.controller.tick(), 'idle')
        self.assertTrue(self.model.peers['target'].idle())
        self.assertTrue(self.model.peers['coordinator'].idle())
        self.assertEqual(self.writes().count(('target', 'SetVolume')), 1)

    def test_volume_events_overlap_before_send_and_after_acknowledgement(self):
        self.start_native()
        self.receiver.volume(-15000)
        original = self.store.event
        def event(name, **data):
            original(name, **data)
            if name == 'intent' and data['action'] == 'SetVolume':
                if self.receiver.volume_revision == 1:
                    self.receiver.volume(-18000)
                elif self.receiver.volume_revision == 2:
                    self.model.update('target', volume=34)
        self.store.event = event
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.model.peers['target'].volume, 34)
        self.assertEqual(self.writes(), [('target', 'SetAVTransportURI')])

        def adjust_native_gains(action):
            if action == 'SetVolume':
                self.model.update('target', volume=35)
                self.model.update('coordinator', volume=38)
        self.model.after = adjust_native_gains
        self.receiver.volume(-12000)
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.writes().count(('target', 'SetVolume')), 1)
        self.assertTrue(any(row['event'] == 'volume_settled_with_concurrent_change'
                            and not row['requested_gain_observed'] for row in self.rows()))

    def test_isolation_mute_supersedes_the_unsent_office_gain(self):
        self.start_native()
        self.receiver.volume(-15000)
        append_event = self.store.event
        def event(name, **data):
            append_event(name, **data)
            if name == 'intent' and data['action'] == 'SetVolume':
                self.model.update('target', volume=0, mute=True)
        self.store.event = event
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.controller.tick(), 'both')
        self.assertEqual(self.writes(), [('target', 'SetAVTransportURI')])
        self.assertTrue(self.model.peers['target'].mute)
        self.assertTrue(any(row.get('superseded_by_observed_mute') is True
                            and not row['speaker_command_sent'] for row in self.rows()))


if __name__ == '__main__':
    unittest.main()
