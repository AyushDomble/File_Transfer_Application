# ZIP packaging

Select **Send selection as one compressed ZIP** before choosing files to package
the group into a local, disk-backed DEFLATE archive. Leave it unchecked to send
the files independently under the same group approval.

## Behavior

The app stages the selected files, calculates integrity metadata, creates the
archive, and requests host approval. The approved member names, sizes and hashes
are checked against the received archive before publication. Original-file rules
still apply: ZIP mode is not a way to send a prohibited extension.

The host saves a ZIP file and does **not** automatically extract it. A ZIP that the
user independently selected as an existing file is treated as a normal `.zip`
transfer, not recursively inspected as an app-generated selection archive.

## Capacity and trade-offs

Allow room for staged originals and the archive on the sender, plus received data
on the host. Compression can reduce text-heavy data but adds CPU/disk preparation
work. Photos, videos and already-compressed data may shrink little. The maximum
file-size rule applies to original members and the resulting container.

Packaging is separate from the wire protocol: ordinary DATA frames are not
transparently compressed. Keep the app open while preparation is running.

## Preparation and publication

```mermaid
flowchart LR
  Files[Selected originals] --> Stage[Disk staging]
  Stage --> Hash[Compute member metadata]
  Hash --> Zip[Create DEFLATE archive]
  Zip --> Approve[Host approves original entries and container]
  Approve --> Transfer[Transfer ZIP]
  Transfer --> Verify[Verify archive and approved members]
  Verify --> Publish[Publish ZIP without extracting]
```

### Rules

ZIP mode must not loosen original extension/size rules. Archive verification must
reject changed members. A normal user-selected archive is not equivalent to a
generated group archive with a trusted approval manifest. Reserve local space
for both staged originals and generated output; compression ratio is not guaranteed.

## Verify

```powershell
python -m pytest tests/test_batches.py -q
$env:FT_BENCHMARK_UPLOAD='1'
python -m pytest tests/test_performance_upload.py -q -o junit_family=xunit1 --junitxml=zip-measurements.xml
Remove-Item Env:FT_BENCHMARK_UPLOAD
```

Batch tests exercise ZIP/plain flows, archive policy and tampered member rejection.
The upload benchmark covers ZIP on/off with two synthetic content classes over
authenticated HTTP staging and TLS transfer. It verifies extracted member bytes
in test code; the application does not automatically extract received ZIPs.
See [Performance](../03-Validate-and-operate/PERFORMANCE.md) for units, workload conditions and interpretation.
