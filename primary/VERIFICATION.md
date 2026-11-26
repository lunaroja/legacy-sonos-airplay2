# Playback verification

The exercised hardware combination is a Sonos Play:3 and Sonos Roam. Tests
covered standalone Play:3 WAV playback, automatic native grouping and mid-track
Play:3 removal in Apple Music on Mac and iPhone. The receiver observed the Roam
as the native coordinator and verified the Play:3 as its follower. Changing
either speaker’s native gain left the other speaker’s gain fixed while grouped.

Two opposite-order acoustic captures of steady grouped playback measured a
conservative recorded arrival difference of −8.94 to +7.72 ms. This is the
difference at one microphone under the tested conditions. Standalone WAV does
not provide that timing guarantee with a HomePod or other AirPlay output.

Mid-track AirPlay route changes can interrupt audio. A Mac Music test detected
missing cues during a join, including before the bridge’s first Sonos group
command. A separate start with both outputs already selected delivered all
15 expected opening cues in its measurement window. In one iPhone catalog
playback test, rejoining the Play:3 produced no multi-second silence in the
measured window. No exact-sample continuity claim is made for that capture.

The release includes a receiver pipe cancellation fix and a regression for a
grouped pause and source-end race. Live iPhone verification of the updated
pause/detach path and independent volume changes from the normal Music picker
remain open.

See [receiver build verification](receiver-source/VERIFICATION.md) for source
and binary provenance.
