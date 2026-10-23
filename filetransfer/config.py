"""
Centralized configuration, loaded from environment variables (with sane
defaults). These are the *startup defaults* -- several (max upload size,
blocked extensions) can also be changed at runtime by the host through
the UI; see TransferHost.update_settings() and HostBatches.configure().
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


HOST = os.environ.get("FT_HOST", "0.0.0.0")
PORT = _env_int("FT_PORT", 5000)

SERVER_DIR = os.environ.get("FT_SERVER_DIR", "server_files")
CLIENT_DIR = os.environ.get("FT_CLIENT_DIR", "client_files")
STATE_DIR = os.environ.get('FT_STATE_DIR')  # default: private .ft-state under each role's directory
GLOBAL_WINDOW_BYTES = _env_int('FT_GLOBAL_WINDOW_BYTES', 8 * 1024 * 1024)
TRANSFER_WINDOW_BYTES = _env_int('FT_TRANSFER_WINDOW_BYTES', 2 * 1024 * 1024)
KEEPALIVE_INTERVAL_SECONDS = float(os.environ.get('FT_KEEPALIVE_INTERVAL', '2'))
KEEPALIVE_TIMEOUT_SECONDS = float(os.environ.get('FT_KEEPALIVE_TIMEOUT', '10'))

MAX_CLIENTS = _env_int("FT_MAX_CLIENTS", 20)
# Applied only during active reads (handshake, and inside a single
# upload/download's byte transfer) -- a stall here means something is
# genuinely wrong, so this stays short.
SOCKET_TIMEOUT_SECONDS = _env_int("FT_SOCKET_TIMEOUT", 60)
# Applied while a connected session is just waiting for the user's next
# action (browsing the file list, deciding what to upload, sitting on
# the page). Long on purpose -- being idle is normal, not an error.
SOCKET_IDLE_TIMEOUT_SECONDS = _env_int("FT_SOCKET_IDLE_TIMEOUT", 1800)  # 30 min

# PIN-based pairing
PIN_LENGTH = _env_int("FT_PIN_LENGTH", 6)
PENDING_APPROVAL_TIMEOUT_SECONDS = _env_int("FT_PENDING_TIMEOUT", 60)
# PIN auto-regenerates after being used once, AND after this many seconds
# of being unused (0 disables the time-based rotation).
PIN_EXPIRY_SECONDS = _env_int("FT_PIN_EXPIRY_SECONDS", 300)

LOG_LEVEL = os.environ.get("FT_LOG_LEVEL", "INFO")
LOG_FILE = os.environ.get("FT_LOG_FILE", "filetransfer.log")
LOG_MAX_BYTES = _env_int("FT_LOG_MAX_BYTES", 2_000_000)
LOG_BACKUP_COUNT = _env_int("FT_LOG_BACKUP_COUNT", 3)

TLS_ENABLED = os.environ.get("FT_TLS_ENABLED", "true").lower() == "true"
TLS_CERT_FILE = os.environ.get("FT_TLS_CERT_FILE")
TLS_KEY_FILE = os.environ.get("FT_TLS_KEY_FILE")
HANDSHAKE_TIMEOUT_SECONDS = 10

WEBAPP_PORT = _env_int("FT_WEBAPP_PORT", 8765)

# LAN auto-discovery (UDP broadcast request/response). The host listens
# for a discovery request on this port and replies with its info; the
# manual IP entry still works exactly as before -- discovery is purely
# an optional shortcut for filling it in.
DISCOVERY_PORT = _env_int("FT_DISCOVERY_PORT", 54545)
DISCOVERY_MAGIC = "FILETRANSFER_DISCOVER_V1"  # ignore any UDP noise that isn't our own protocol
DISCOVERY_TIMEOUT_SECONDS = float(os.environ.get("FT_DISCOVERY_TIMEOUT", "2.5"))

# Host-configurable transfer limits (defaults; changeable at runtime)
# 0 = unlimited
MAX_UPLOAD_SIZE_BYTES = _env_int("FT_MAX_UPLOAD_SIZE_BYTES", 0)
BLOCKED_EXTENSIONS = os.environ.get("FT_BLOCKED_EXTENSIONS", ".exe,.bat,.sh,.msi,.cmd")

