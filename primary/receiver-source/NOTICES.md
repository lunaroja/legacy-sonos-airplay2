# Receiver source and license notices

This recipe builds Shairport Sync 5.6.3 from the content-pinned upstream source
identified in `source.json`, followed by four ordered local patches. It does
not redistribute Sonos firmware or any extracted device code, recordings, or a
private runtime-library bundle.

The project additions and build recipe are distributed under the accompanying
project MIT `LICENSE`. Existing upstream code retains its original notices.
Do not describe every component of the receiver as having one license.

The receiver output includes upstream `LICENSES`, `COPYING`, `tinyhttp/LICENSE`
and the actual copyright/license comments from source files in
`licenses/source-license-headers.json`. The upstream index covers Shairport,
Shairport Sync, ALAC, mDNS implementations, FFTConvolver, tinyhttp and pair_ap,
including its evrtsp code. Some indexed components are not enabled by this build;
their notices are retained rather than inferred away.

Keep these notices with a redistributed receiver binary and make its exact source
archive, four patches and recipe available using their recorded hashes. No local
private artifact is needed to obtain those public source inputs. The source
archive remains a downloaded/generated input, not a file to commit to the project.

Runtime libraries are installed from Debian by the operating system package
manager. Their exact observed versions, ownership and hashes are recorded in
`runtime-libraries.json`; their notices remain in the Debian packages under
`/usr/share/doc/<package>/copyright`. This recipe does not copy those libraries
into a binary distribution. A future distribution that bundles them requires its
own complete notice/source review.
