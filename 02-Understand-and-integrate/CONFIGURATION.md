# Configuration

Startup configuration is read from environment variables by `filetransfer/config.py`.
Apply changes before launch. The host UI persists supported runtime settings in
SQLite; startup defaults do not necessarily override settings already saved there.

| Variable | Default | Purpose |
| --- | --- | --- |
| `FT_HOST` | `0.0.0.0` | Peer listener address; not the UI bind address |
| `FT_PORT` | `5000` | Peer TCP port |
| `FT_WEBAPP_PORT` | `8765` | UI port; launcher binds loopback |
| `FT_DISCOVERY_PORT` | `54545` | UDP discovery |
| `FT_DISCOVERY_TIMEOUT` | `2.5` | Discovery wait in seconds |
| `FT_SERVER_DIR` | `server_files` | Host data directory |
| `FT_CLIENT_DIR` | `client_files` | Client data directory |
| `FT_STATE_DIR` | Role directory's `.ft-state` | State-root override |
| `FT_MAX_CLIENTS` | `20` | Connection admission limit |
| `FT_GLOBAL_WINDOW_BYTES` | `8388608` | Session byte window |
| `FT_TRANSFER_WINDOW_BYTES` | `2097152` | Per-transfer byte window |
| `FT_KEEPALIVE_INTERVAL` | `2` | Keepalive interval, seconds |
| `FT_KEEPALIVE_TIMEOUT` | `10` | Keepalive timeout, seconds |
| `FT_SOCKET_TIMEOUT` | `60` | Active read/frame timeout, seconds |
| `FT_SOCKET_IDLE_TIMEOUT` | `1800` | Idle socket timeout, seconds |
| `FT_PIN_LENGTH` | `6` | PIN digits |
| `FT_PENDING_TIMEOUT` | `60` | Connection approval wait, seconds |
| `FT_PIN_EXPIRY_SECONDS` | `300` | Unused PIN expiry; zero disables timed rotation |
| `FT_TLS_ENABLED` | `true` | Desktop requires TLS; keep enabled |
| `FT_TLS_CERT_FILE`, `FT_TLS_KEY_FILE` | Generated identity | Supply both for explicit identity files |
| `FT_MAX_UPLOAD_SIZE_BYTES` | `0` | Startup size limit; zero means unlimited |
| `FT_BLOCKED_EXTENSIONS` | `.exe,.bat,.sh,.msi,.cmd` | Startup extension deny list |
| `FT_LOG_LEVEL` | `INFO` | Logging level |
| `FT_LOG_FILE` | `filetransfer.log` | Log destination |
| `FT_LOG_MAX_BYTES` | `2000000` | Rotation threshold |
| `FT_LOG_BACKUP_COUNT` | `3` | Rotated backup count |

Relative paths resolve from the process working directory. Prefer absolute writable
paths for stable deployments. No `FT_MAX_CONCURRENT` setting is documented: file
concurrency is configured by the host/client constructors. See [Concurrency](CONCURRENCY_AND_FLOW_CONTROL.md).

## Configuration ownership rules

| Setting category | Owner | Application rule |
| --- | --- | --- |
| Listener, paths, timeouts and byte-window defaults | Startup environment | Set before launch; restart to apply startup changes |
| Supported host rules and permissions | Settings UI and persisted SQLite state | Read back effective values; saved settings can supersede defaults |
| Active-file admission | Host/client constructor arguments | Changing staging parallelism does not change this value |
| Certificate identity | Persistent protected files | Preserve identity across normal restarts; investigate unexpected changes |

Use separate state directories for independently running instances. Ensure the
process account can write its data and state directories without granting unrelated
accounts access to identity keys. A configured unlimited file-size policy does not
mean unlimited disk capacity; capacity remains a separate constraint.

## Verify

```powershell
python -m pytest tests/test_host.py tests/test_flow_control.py tests/test_release_launcher.py -q
```

These cover host settings, window behavior and launcher paths. For an actual
deployment, inspect effective settings after restart and run the isolated source or
executable smoke command in [Build and packaging](../03-Validate-and-operate/BUILD_AND_PACKAGING.md).
