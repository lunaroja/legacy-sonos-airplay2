# Build the shared AirPlay receiver on Debian 13 ARM64

This package builds the patched Shairport Sync 5.6.3 receiver with Debian Trixie
system libraries. It supplies the receiver's raw/framed PCM output and local
session/volume evidence. It contains no relay controller, Sonos UDP sender, SNTP
server, silent coordinator source, grouping policy or product configuration.

The primary WAV/HTTP relay can use this receiver's framed PCM and SPSE fixes
through its own validated reader. See [the receiver contract](RECEIVER-CONTRACT.md)
before connecting another reader.

## Build

Requirements are Docker with ARM64 support, Python 3 and public network access
for source/package downloads. From this directory:

```sh
sh build-container.sh out
```

`out` must be empty. The script verifies the public source and all patch
hashes, then compiles in a read-only, network-disabled container with temporary
build storage. It does not install packages on the host or start a service.

Pinned inputs:

- ARM64 Debian base:
  `debian@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2`.
- Signed Debian `trixie`, `trixie-updates` and `trixie-security` snapshots at
  `20261117T000000Z`; signatures remain enforced.
- [Shairport Sync 5.6.3](https://github.com/mikebrady/shairport-sync/releases/tag/5.6.3),
  commit `7ce90d89529dd760488f20592a7cbff626672f6c`. The verified public archive
  hash is `670f9cefdb9c2d70f7df71890e63ab0a31d08a815dd0734245d935bfd01e766e`.
- The ordered startup/framed-pipe, observer, volume and pipe cancellation patches in `source.json`;
  each applies with zero fuzz. These patches define receiver behavior and contain
  no Sonos injection protocol or coordinator-borrow policy.
- Configure flags: `--with-pipe --with-airplay-2 --with-ssl=openssl --with-avahi --with-metadata`.

The output includes `build.json`, package/version metadata, system-library
ownership/hashes, generated observer C/header, license notices and `SHA256SUMS`.
Downloaded archives, logs and compiled output stay ignored. The source archive
is fetched from its public URL and checked against its content pin; no private
archive or home-directory path is needed.

## Runtime dependencies

The direct receiver package list is in `runtime-packages.txt`:

```text
libavcodec61 libswresample5 libavutil59 libuuid1 libgcrypt20
libsodium23 libplist-2.0-4 libavahi-client3 libssl3t64 libconfig11 libpopt0
```

APT supplies transitive dependencies. The project's Python 3, Avahi daemon and
existing `play3-airplay-clock.service` / nqptp setup remain the service prerequisites.
Reuse that shared clock provider; this recipe does not install a second nqptp.
The receiver requires shared-memory interface **smi11**. Device identity, interface
name and timing offset come from the owning branch's existing configuration.

`libconfig11:arm64` version `1.7.3-2` provides `libconfig.so.11` on Trixie.
[Debian metadata](https://packages.debian.org/trixie/libconfig11)
For an existing host missing only that library, the bounded package action is:

```sh
sudo apt-get --no-install-recommends --no-upgrade --no-remove install libconfig11=1.7.3-2
```

Simulate and inspect the host's actual transaction before installation. This
instruction does not imply that all hosts need the same one-package repair.
Use the host's normal signed repositories for deployment updates; the historical
snapshots pin this build's environment.

Verify a reviewed ELF without starting an AirPlay session:

```sh
python3 -B verify-system.py /path/to/shairport-sync --sha256 REVIEWED_SHA256
```

The checker verifies the binary hash, system-loader resolution and `-V` with no
loader environment overrides. Launch the ELF directly from its owning service or
supervisor to preserve its PID and inherited observer FD. No private library
bundle, shell wrapper or `LD_LIBRARY_PATH` is required.

## Verification and deployment boundary

The ARM64 receiver for v1.2.0 has SHA-256 `f7896a553373631492f3148a45ae77f31d5e22a77436771f526c19430cac060a`.
The offline build resolves all 108 system-library records. The isolated ARM64
pipe regression verified that cancellation cannot interrupt the bookkeeping for
a completed framed write. See [verification](VERIFICATION.md) for the current
receiver build and the earlier recipe checks.

Keep `LICENSE`, `NOTICES.md` and generated `out/licenses/` with distributed
receivers. System libraries remain Debian-managed and are not bundled here.
