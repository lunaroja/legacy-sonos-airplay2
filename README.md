# AirPlay 2 for Legacy Sonos

Bring older Sonos players into the same AirPlay workflow as your other
speakers. A Raspberry Pi makes a selected player appear in the AirPlay
picker, where you can play music, skip tracks, and adjust volume from your
iPhone or Mac. Once the player is on your network, everyday playback works
from AirPlay. The **Play:3** is the only model we have confirmed so far.

Starting with v1.1.0, one Pi can run a separate AirPlay receiver for each
standalone player you opt in. Each appears in the AirPlay picker with its
own name, stream, and volume control.

Sonos can group an older player with an AirPlay-compatible Sonos speaker.
That speaker needs to be powered on and receive the stream, which may mean
buying another speaker or tying up one you already use. The Pi receives
AirPlay so the older player can play on its own.
[Sonos AirPlay and grouping guide](https://support.sonos.com/en/article/stream-airplay-audio-to-sonos)

![Apple Music sends AirPlay 2 to the Pi. The relay sends WAV audio and playback and volume commands to the Sonos player.](docs/airplay-flow.svg)

The Pi receives AirPlay through Shairport Sync and relays the audio to the
selected Sonos player over the network. It streams WAV by default, with FLAC available as an
option.

## Compatibility

These are standalone Sonos playback devices without native AirPlay. The
untested rows are candidates for testing, based on the bridge's use of local
Sonos transport and volume controls. Playback over WAV, volume, and track
changes still need to be verified on each candidate.

| Model | Bridge status | Setup |
| --- | --- | --- |
| Play:3 | **Confirmed on one physical speaker** | Discovery |
| Play:1 | Untested candidate | Discovery |
| Playbar | Untested candidate | Discovery |
| Play:5 (Gen 1) and ZonePlayer S5 | Untested candidates | Manual; S1 only |
| Connect and Connect:Amp (Gen 1) | Untested candidates | Manual; S1 only |
| Connect and Connect:Amp (Gen 2) | Untested candidates | Manual |
| ZonePlayer 80, 90, 100, and 120 | Untested candidates | Manual; S1 only |

Use the model and generation in your Sonos account to check eligibility.
[Sonos AirPlay compatibility](https://support.sonos.com/en/article/stream-airplay-audio-to-sonos)
· [Sonos app compatibility](https://support.sonos.com/en/article/sonos-app-version-compatibility)

### Multiple players on one Pi

We tested two simulated Sonos players through one bridge host and verified
playback and track skips in Apple Music on one physical Play:3. Running two or more
physical players from the same Pi still needs testing. If you try it, we'd
welcome your results, including how each speaker behaves when selected
individually and when playing at the same time.

## Sync with other AirPlay speakers

We ran click tests and tuned the timing to bring the Play:3 closer to another
AirPlay speaker. The gap can still change between tracks, so this works best
for casual listening across rooms. To tune it, set
`audio_backend_latency_offset_in_seconds` in the `general` section of the
Shairport Sync config and restart the services. A negative value moves playback
earlier. [Shairport Sync timing guide](https://github.com/mikebrady/shairport-sync/blob/5.6.3/ADVANCED%20TOPICS/AdjustingSync.md)

## Automatic native Sonos grouping

Version 1.2.0 adds an [opt-in receiver](primary/README.md) that automatically
groups a Play:3 with a configured Sonos Roam when you select both in the same
AirPlay session. Select the Play:3 alone for standalone WAV playback. Select
both speakers before pressing Play to avoid the audible interruption that can
occur when adding the Play:3 mid-track.

The grouping receiver requires Debian 13 ARM64, the pinned receiver build, and
the existing shared clock service. Follow its [installation and rollback
instructions](primary/README.md#stage-an-existing-pi-installation) to switch the
profile for your chosen speaker. The standalone installer below remains
available for other profiles and existing setups.

## Install on a 64-bit Raspberry Pi

The installer targets Raspberry Pi OS or Debian on ARM64 with systemd. Use a
trusted LAN and reserve IPv4 addresses for the Pi and your selected Sonos
player. Choose a player that's already standalone on the network.

1. Install the host tools on the Pi.

   ```sh
   sudo apt install avahi-daemon curl ffmpeg python3
   sudo systemctl enable --now avahi-daemon
   ```

2. Run discovery **on the Pi**, then note the UUID of each speaker you
   want to expose in AirPlay from the entries marked `eligible`. Generate a
   separate config file for each one.

   ```sh
   python3 discover-sonos.py
   python3 discover-sonos.py --select RINCON_00000000000001400 --output office.env
   ```

   Replace the example UUID with the value from the list. The helper skips
   native AirPlay 2 receivers and models it cannot classify. It currently
   recognizes the Play:1, Play:3 and Playbar as candidates. Playback has
   been tested on the Play:3.

   For a model marked **Manual**, or if discovery is unavailable, copy
   `relay.env.example` to that player's config file and set its Pi IP, speaker
   IP and UUID manually. The UUID appears in the speaker's device description at
   `http://<speaker-ip>:1400/xml/device_description.xml` as the first device's
   `UDN`. Remove the `uuid:` prefix when copying it into the config file.

3. Extract the official [Shairport Sync 5.6.3](https://github.com/mikebrady/shairport-sync/releases/tag/5.6.3)
   ARM64 image. With Docker available for this one-time step, run the
   following in the project directory. Docker is only needed for extraction.

   ```sh
   mkdir receiver-image
   receiver_container=$(docker create --platform linux/arm64 mikebrady/shairport-sync:5.6.3)
   docker export "$receiver_container" | tar -xf - -C receiver-image
   docker rm "$receiver_container"
   ```

4. Download the pinned [upstream relay](https://github.com/shinn-y/airplay2-dlna-bridge/tree/6089a33ae8ffa5020c10a98d30fcaa92e8423eec)
   and install the services. The installer verifies the relay checksum.

   ```sh
   curl -fL https://raw.githubusercontent.com/shinn-y/airplay2-dlna-bridge/6089a33ae8ffa5020c10a98d30fcaa92e8423eec/bridge/server.py -o server.py
   sudo ./install-pi receiver-image server.py
   ```

5. Enable each selected player. The profile ID (such as `office`) is local
   to the Pi. The name is what you see in the AirPlay picker.

   ```sh
   sudo /opt/play3-airplay/manage-player.py add office office.env --name "Office Sonos"
   sudo /opt/play3-airplay/manage-player.py add study study.env --name "Study Sonos"
   ```

   Select a player in Apple Music's AirPlay picker, play a track, adjust
   volume, then skip to another track. Each enabled player starts at boot.
   To stop advertising a player, remove its profile.

   ```sh
   sudo /opt/play3-airplay/manage-player.py remove study
   journalctl -u play3-airplay-receiver@office -u play3-airplay-relay@office
   ```

Each profile lives in `/etc/play3-airplay/players/<profile>/`. The optional
`--offset` at setup sets that player's timing offset in seconds. After editing
a profile, restart its receiver and relay. The shared clock service can keep
running.

Existing single-player installations use the original service names and files.
The installer still accepts an optional third argument for that setup. Updating
those installations to use profiles requires updating the installed wrapper and
adding the two template units and the profile manager before enabling profiles.

## Contributing

Test reports and PRs are welcome for multiple players on one Pi and for
other legacy Sonos models. Share the bridge version, speaker models, Sonos
software versions, and what happened when you started playback, skipped
tracks, and changed volume. For multi-speaker testing, include which
speakers were selected in AirPlay when the behavior occurred.

## Credits

Adapted from [airplay2-dlna-bridge](https://github.com/shinn-y/airplay2-dlna-bridge).
AirPlay reception and timing are provided by
[Shairport Sync](https://github.com/mikebrady/shairport-sync) and
[NQPTP](https://github.com/mikebrady/nqptp).
