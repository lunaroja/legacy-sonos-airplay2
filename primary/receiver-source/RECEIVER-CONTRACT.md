# Receiver output and local evidence contract

The shared ELF has two pipe formats. Choose the format according to the reader;
changing the binary alone does not make a raw PCM reader understand framed input.

## Existing raw PCM path

`pipe.timestamped` defaults to false. The existing released WAV reader consumes
44,100 Hz, signed 16-bit little-endian stereo PCM, and its legacy metadata FIFO
remains a separate contract. Leave timestamp framing off and do not supply
`SPS_SELECTION_FD` when running that unmodified reader. These are compatibility
settings, not proof that the released reader implements the new automatic grouping
or mid-song deselection behavior.

The existing source's `read_wav_audio` reads arbitrary FIFO chunks directly into
its audio ring. It must never receive an SPTS header as if it were audio.

## Framed PCM plus SPSE

For a supervised consumer that implements the framed reader and local evidence:

```text
general.service_type = airplay2
general.output_backend = pipe
general.ignore_volume_control = yes
pipe.output_rate = 44100
pipe.output_format = S16_LE
pipe.output_channels = 2
pipe.timestamped = true
pipe.timestamped_reanchor = false
pipe.timestamped_write_timeout_ms = 50
pipe.startup_diagnostics = false
```

This is a field contract, not a complete deployable config. The owning branch
generates the unique private FIFO path, advertised name/port, stable device ID,
existing nqptp interface and timing offset. No household-specific offset is set
by this shared package. The integrated consumer uses generation-bound SPSE volume
evidence; enable/disable a legacy metadata FIFO only according to that consumer.

The launcher creates an inherited connected AF_UNIX datagram FD and passes its
number in `SPS_SELECTION_FD`, using `pass_fds` with `close_fds=True`. The receiver
requires timestamped pipe mode when this variable is present. It removes the
variable from its environment after initialization and marks the descriptor
close-on-exec so helper executions cannot reuse the channel. Never reconnect an
observer to an unrelated or already-running receiver.

SPTS v2 uses a 40-byte big-endian header, `>4sBBHIIQQII`, followed by PCM:

```text
magic/version/kind/bytes-per-frame/epoch/sequence/
presentation-RAW-ns/observation-RAW-ns/frame-count/input-RTP
```

Kinds are TIMED=1, UNTIMED=2, FLUSH=3 and STOP=4. The primary consumer validates
framing, lengths, sequence and epoch, strips headers, and applies the intended
flush/stop policy before enqueueing PCM into its WAV path. The
reader uses these fields as evidence for timing and session checks.

SPSE v2 is an 88-byte message, `!4sBBHQQQIIIIQII16siI`, with receiver generation,
sequence, monotonic observation time, matching SPTS header evidence, group UUID,
accepted volume and volume revision. Event types are HELLO=1, BEGIN=2, ACCEPT=3,
FRAME=4, END=5, FAULT=6 and VOLUME=7. The consumer verifies per-message Linux
PID/UID credentials against the owned child and the dedicated FIFO/channel
incarnation. A supplied generation string, cached GID or a live PID alone is
not equivalent evidence.

Accepted volume belongs to the specific receiver connection and generation.
Keep software PCM volume ignored when applying that volume to the target speaker,
to avoid applying it twice. Preserve the primary branch's independent target
volume and manual-control rules.

## Product separation

The primary scope is standalone WAV/HTTP playback on the selected legacy target,
plus stock Sonos grouping only when the local endpoint and compatible Sonos are
explicitly participating in the same AirPlay group. Matching receiver/coordinator
group evidence needs the owner's fresh source/topology checks; an idle coordinator
or a self-device GID does not authorize borrowing it.

On mid-song local deselection, the primary owner makes the target quiet and
detaches only that target while the compatible Sonos track continues. It does
not install a silent source, mute the coordinator, inject Node packets or use
custom route settings. When both are selected, the primary owner uses stock
Sonos transport and avoids a duplicate local audio stream. HomePod and other
third outputs are deferred.

The receiver patches provide stream and session evidence. The Sonos lifecycle
policy is implemented by the [grouping receiver](../README.md).
