# Receiver build verification

The v1.2.0 ARM64 build produces a 712,656-byte receiver with this SHA-256.

```text
f7896a553373631492f3148a45ae77f31d5e22a77436771f526c19430cac060a
```

The offline build used the pinned Debian 13 environment, Shairport Sync 5.6.3
and all four ordered patches. All 108 system-library records resolved. The
binary was used in an isolated ARM64 pipe regression and live playback trials.

The first three patches were also verified in two earlier byte-identical clean
builds. Those earlier results are recorded separately in `validation.json`.
They include a UNIX socket observer test that verified kernel PID, UID and GID
and volume observations across three connection generations.

## Pipe cancellation replay

The fourth patch defers cancellation until the current deadline-bounded framed
write and its epoch/sequence bookkeeping complete. The receiver reports the
original failure when a later operation encounters a closed writer.

An isolated ARM64 kernel-pipe replay forced cancellation during a full-pipe
write. The original ordering returned `SPW_POISONED` on the next FLUSH. With
the patched transaction boundaries, the record completed and the subsequent
STOP and next FLUSH succeeded. The pinned offline build resolved all 108
system-library records. Real sender playback is a separate acceptance test.
