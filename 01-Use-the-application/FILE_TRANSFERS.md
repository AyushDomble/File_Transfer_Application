# File transfers

Files travel in bounded chunks over a multiplexed TLS connection. The default chunk
size is 1 MiB. Each transfer has its own identity, metadata, state and progress;
cancelling or failing one file does not deliberately close the shared connection.

## Integrity and completion

Each chunk carries an offset, sequence and SHA-256 checksum. The receiver validates
the chunk before acknowledging it. Whole-file size and SHA-256 are checked before
final publication. Completion means application verification succeeded, not just
that the sender filled a socket buffer.

Received data is written to a transfer-owned partial file on disk. The final
destination is published only after verification and ownership checks. Concurrent
writers to the same destination are rejected. Overwrite intent is explicit; a
late cancellation does not undo an already committed destination.

## Memory and disk

The application uses RAM for buffers and metadata, not normally for the entire
file. Browser uploads are staged on disk because retries and resumes require a
seekable source. The OS may also cache disk data in memory. Source preparation,
staging, partial files and ZIP creation must all be included in capacity planning.

Keep source files unchanged while a transfer may need retry or recovery. A checksum
failure is not resolved by disabling verification; inspect the source and storage.

## Transfer acceptance rules

| Check | Why it matters |
| --- | --- |
| Metadata before scheduling | A worker must not run for an unaccepted request |
| Explicit overwrite intent | Avoid silent replacement and destination races |
| Chunk size and cumulative offset | Prevent overruns and out-of-order writes |
| Chunk hash | Detect corrupted chunk data before confirmation |
| Durable checkpoint before ACK | Preserve meaningful recovery progress |
| Whole-file hash and length | Prevent publishing incomplete or altered content |
| Final ownership check | Prevent a different transfer from publishing this path |

A failed transfer may leave owned partial state for recovery; cancellation and
cleanup have different intent. RAM includes buffers, TLS state, queues and OS file
cache, so disk-backed transfer does not mean zero memory use.

## Verify

```powershell
python -m pytest tests/test_chunk_ack_stress.py tests/test_pause_resume.py tests/test_scheduler.py tests/test_tls_reliability.py -q
```

These tests cover random-size byte-exact transfers, concurrent/sequential session
reuse, pause/cancel interaction, scheduler isolation and concurrent TLS reliability.
Use [Storage and recovery](../02-Understand-and-integrate/STORAGE_AND_RECOVERY.md) for restart tests and
[Performance](../03-Validate-and-operate/PERFORMANCE.md) for measured workload sizes.
