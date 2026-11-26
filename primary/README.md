# AirPlay receiver with automatic Sonos grouping

This standalone daemon streams Office AirPlay audio to a legacy Sonos player over
WAV. If Office and a configured AirPlay-compatible Sonos speaker participate in
the same current AirPlay session, it joins the target to that speaker's native
Sonos group. The compatible speaker is optional; Office plays alone without it.

The only remote writes are to the configured target: assign its WAV URI, Play,
join, detach, Stop, clear URI and SetVolume. The wire API cannot mutate the
coordinator. Matching a name, being idle, or merely advertising a group ID does
not authorize grouping. A fresh accepted Office receiver session, matching
AirPlay group, matching native URI, pinned device identities and continuous Sonos
source/topology evidence must agree.

Normal pause/FLUSH retains an owned native group. Explicit Office RTSP END
releases the target that this daemon joined, verifies its standalone topology,
then stops and clears that target. The coordinator receives no stop, play, URI,
volume or route command. Its source, transport and gains are checked through
cleanup. A FIFO STOP alone is not treated as explicit RTSP END. Pre-existing
Sonos groups and foreign sources are left alone.

## Run

Use Linux with Python 3.10+, systemd, Avahi and the normal Shairport/NQPTP clock
service. The ARM64 receiver is built from Shairport Sync 5.6.3 with
generic SPSE v2 session/volume and framed PCM support. Build it from the pinned source recipe under `receiver-source/`. Standard-library Python is sufficient;
there is no Node, FFmpeg, Home Assistant or agent dependency in this runtime.

The primary config enables framed PCM only. It does not enable timing-provenance
reanchoring or a custom packet sender. WAV uses the released standalone rate
(44.1 kHz, signed 16-bit stereo), a bounded memory ring, up to five seconds of
short-gap padding and conditional owned-stream reconnection. Audio is never
written to a recording file. Native grouping drains the local FIFO continuously.

1. Install the bundle in a root-owned directory such as `/opt/office-primary`.
   Reuse the released `play3-airplay` account, configure the existing NQPTP
   service and provide its shared-memory interface. The example unit uses `play3-airplay-clock.service`.
2. Copy `config.solo.example.json` or `config.group.example.json` to a protected
   configuration file. Set the target UUID/IP, local IP and ports. Leave
   `coordinator` as `null` for standalone operation. With grouping enabled, set
   the exact configured coordinator UUID/IP and AirPlay service instance.
3. Build the receiver with `sh receiver-source/build-container.sh receiver-source/out`.
   Install the Debian Trixie runtime packages listed in
   `receiver-source/runtime-packages.txt`, then verify the ELF using
   `receiver-source/verify-system.py`. The examples pin the reproducible build.
4. Stop the previous receiver/relay for this target before starting this unit;
   preserve its config and rollback path. Do not run two implementations against
   the same player. This bundle does not stop or alter other installed services.
5. Validate and run using the manifest-checking isolated entry point:

   ```sh
   python3 -I -S -B /opt/office-primary/launch.py --config /etc/office-primary/config.json validate
   python3 -I -S -B /opt/office-primary/launch.py --config /etc/office-primary/config.json run
   ```

`office-primary.service` demonstrates service ownership and shutdown ordering.
SIGTERM conditionally releases the owned target before closing its receiver,
FIFO, WAV clients and GENA subscriptions. Failures still close every local
resource and preserve durable recovery state. Systemd restarts failures after five seconds, bounded to three starts per minute.
Its startup gate validates active ownership journals. A recognized interrupted
operation waits for fresh proof that the target is standalone, stopped and
has an empty source. It then finalizes the local journal using the same
fingerprint and stable target reads as explicit idle recovery. No speaker write
is sent during this wait, and the receiver is advertised again after recovery.
An invalid journal still blocks startup. A transient observation failure
retries with fresh target subscriptions. The service never replays an ambiguous
speaker write. A failed idle process with no command journal may restart.
Sonos subscriptions are renewed even while no AirPlay audio is arriving.

## Recovery

Each target mutation has a durable intent, one attempt and two stable coherent
readbacks. Short expected transport/topology transitions are read-only settling;
foreign changes stop the sequence. An unacknowledged or uncertain write is never
replayed. Startup with an active journal requires explicit recovery:

```sh
python3 -I -S -B launch.py --config /etc/office-primary/config.json status
python3 -I -S -B launch.py --config /etc/office-primary/config.json recover
python3 -I -S -B launch.py --config /etc/office-primary/config.json recover --apply FINGERPRINT_FROM_PREVIEW
```

During an active owned native session, cleanup can handle the observed follower
state disagreement (event STOPPED, SOAP PLAYING) only after proving that the exact
owned coordinator source directly ended. URI, topology, boot, subscription and
freshness checks remain required through target detach, Stop and clear. A paused coordinator retaining that exact owned source instead selects a read-only
HOLD proof: local presence, the same generation, no media, unchanged ownership
and fresh continuous subscriptions are required. It never authorizes a speaker
write or cleans a journal.

An authenticated Office END can also release only the target while the owned
coordinator remains paused. Its dedicated cleanup context pins the current
coordinator source, revisions and independent gain settings through detach, Stop
and empty-URI clear. Presence loss, a pause, or an inferred UI change cannot
authorize the transport exception. Concurrent coordinator resume/source end
refuses the next action without replay; normal strictly coherent shutdown keeps
its existing ownership checks.

While leaving a verified HOLD, one target transport event/read race may receive
one fresh strict two-peer read after a 500 ms backoff. The original ownership
checkpoint stays fixed, both peers must match it, and the two-second evidence
freshness limit remains unchanged. The bounded retry is only in observation; it
never retries HTTP commands, pre-write guards, acknowledgements or recovery.
A verified native session also permits that single read-only recheck when the
coordinator reports PLAYING versus PAUSED_PLAYBACK on otherwise continuous,
unchanged source and topology evidence. It must converge to an ordinary strict
read against the original ownership checkpoint. Persistent disagreement, source
or topology changes, subscription loss and other transport states still refuse.

During a verified native session, a target STOPPED/PLAYING disagreement can also
select one read-only reclassification when the coordinator still proves the
original source and group. Ordinary playback revalidation requires aligned local
media and a playing coordinator; pending volume never uses an unresolved pause.
A pause or end during that window must pass the existing HOLD or cleanup proof.
The source checkpoint and all command guards remain unchanged.

Recovery only cleans up a fully verified, unchanged checkpoint. A changed
preview or an ambiguous intent is refused for manual review. Recovery never
starts playback. A verified detached target is not detached again.

If an operator has independently restored the physical target to standalone,
stopped and empty, an old journal can be acknowledged explicitly without any
speaker writes. Keep the exact original configuration and state directory:

```sh
python3 -I -S -B launch.py --config /etc/office-primary/config.json recover --finalize-idle
python3 -I -S -B launch.py --config /etc/office-primary/config.json recover --finalize-idle --apply FINGERPRINT_FROM_PREVIEW
```

This separate preview/apply path requires the process-held scope, continuous
matching target evidence and two stable idle snapshots. Apply rechecks the
fingerprint and target before clearing only local ownership. It never reads or
changes the optional coordinator, starts a receiver, replays a write, or performs
physical cleanup. A changed or non-idle target is refused. Ordinary `recover`
retains its checkpoint and ambiguous-intent rules.

## Build and test

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest discover -q
sh receiver-source/build-container.sh receiver-source/out
```

The pinned recipe verifies public source and ordered patches, uses signed Debian
snapshot packages and compiles without network access. Downloaded upstream
archives and compiled outputs stay ignored. See `receiver-source/README.md` for
build provenance and runtime packages. The source manifest checks this daemon's
reviewed files; the configured receiver executable is independently hash checked
before launch. It can live outside the source checkout.

Tests include synthetic lifecycle and actual SOAP/GENA serialization, Linux WAV
sockets, real child PID/UID/inherited-FD/FIFO lineage, explicit END, source changes,
volume isolation, bounded transitions, shutdown and explicit recovery. See
`VERIFICATION.md` for exact results and evidence limits.

## Security and state

Use a trusted LAN. UUID/IP/boot checks and continuous GENA revisions constrain
ownership but do not cryptographically authenticate the LAN. WAV and GENA use
random capability paths, source-IP restrictions and bounded parsing/connections.
The receiver IPC additionally verifies each message's kernel PID/UID and the
exact inherited socket and private FIFO inodes. Ordinary pause preserves presence
without refreshing old media timestamps. Initial play/group/volume requires
fresh accepted media; presence alone never starts audio.

The process-held scope lock coordinates local instances. It does not lock Sonos
apps or external controllers. Source changes, including away-and-back revisions,
are detected and do not authorize subsequent writes. Configuration and runtime
assets are bounded and owner/mode checked; the manifest is an integrity check,
not a signature. Private bounded journals can contain current source URIs;
stdout contains status and allowlisted error types only.

The verified hardware combination is a Play:3 and a Sonos Roam. Adding or
removing a route mid-track can briefly interrupt playback. Selecting both
speakers before starting a track avoids the mid-track join. Individual Music
volume controls and pause/detach combinations need further live testing.
See [verification](VERIFICATION.md) for the scope of the measured results.

## Stage an existing Pi installation

On a fresh Pi, complete steps 1–4 of the
[standalone installation](../README.md#install-on-a-64-bit-raspberry-pi) to
install the shared clock service and account. Start the shared clock with
`sudo systemctl enable --now play3-airplay-clock.service`. You can skip creating
a standalone profile for this target.

From the repository root, build the receiver with
`sh primary/receiver-source/build-container.sh primary/receiver-source/out`.
Copy an example to a private config and set the speaker identities, local address
and receiver hash and path. Keep state and scope under `/var/lib/office-primary`.
Stage the new service with:

```sh
sudo ./install-primary primary/receiver-source/out/shairport-sync /path/to/private-config.json
```

The installer requires the existing shared clock/account and refuses to overwrite
an existing primary installation. It copies only manifest-listed source, the
hash-pinned receiver and its generated notices/build record. It validates the
config and unit, then reloads systemd. It starts/stops no receiver.

If the target already has an idle standalone profile called `office`, stop it
before starting the grouping service:

```sh
sudo systemctl stop play3-airplay-receiver@office play3-airplay-relay@office
```

Start and inspect the grouping service:

```sh
sudo systemctl start office-primary
sudo systemctl status office-primary
```

Verify actual playback, target-only cleanup, source and volume protection before
making that handoff persistent. Once verified, disable the old **target profile**
pair and enable `office-primary`; retain their files and the shared clock.
Do not disable another profile or run both controllers against the same target.
If a source/journal is uncertain, inspect `status` and the conditional `recover`
preview first. Do not force a clean state or replay a write. After owned playback
is stopped and primary cleanup is verified, rollback stops `office-primary` and
restarts the preserved target profile pair. Restore the prior enabled states.
