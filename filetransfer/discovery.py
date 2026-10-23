"""
LAN auto-discovery. A lightweight custom UDP request/response protocol --
not mDNS/zeroconf, no extra dependency needed.

How it works:
  - While hosting, DiscoveryResponder listens on config.DISCOVERY_PORT
    for a small "who's out there" request and replies directly to the
    sender with this host's IP, port, and a friendly name.
  - discover_hosts() (called from the client side) broadcasts that
    request and collects replies for a short window.

This is entirely additive: manual IP entry keeps working exactly as
before regardless of whether discovery finds anything.
"""

import json
import socket
import threading

from . import config, netinfo
from .logsetup import get_logger

log = get_logger("filetransfer.discovery")

_REQUEST = json.dumps({"magic": config.DISCOVERY_MAGIC, "type": "discover"}).encode("utf-8")


class DiscoveryResponder:
    """Runs alongside a hosting session. Started/stopped by
    TransferHost.start()/stop() -- not used standalone."""

    def __init__(self, get_port, get_pin):
        # callables so the responder always reports current values,
        # even if the PIN rotates while it's running
        self._get_port = get_port
        self._get_pin = get_pin
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop_flag = threading.Event()
        from .rate_limit import AttemptLimiter
        self._requests = AttemptLimiter(per_source=10, global_limit=100, window=1, capacity=256)

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", config.DISCOVERY_PORT))
        except OSError as e:
            # Non-fatal -- hosting still works fine without discovery,
            # clients just fall back to typing the IP manually.
            log.warning("Discovery responder could not bind port %s (%s) -- LAN auto-discovery disabled, manual IP entry still works", config.DISCOVERY_PORT, e)
            return
        sock.settimeout(0.5)
        self._sock = sock
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        log.info("Discovery responder listening on UDP port %s", config.DISCOVERY_PORT)

    def stop(self):
        self._stop_flag.set()
        if self._sock:
            self._sock.close()
        if self._thread:
            self._thread.join(timeout=2)

    def _loop(self):
        while not self._stop_flag.is_set():
            try:
                data, addr = self._sock.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break  # socket closed during stop()

            try:
                msg = json.loads(data.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            if not isinstance(msg, dict) or msg.get("magic") != config.DISCOVERY_MAGIC or msg.get("type") != "discover":
                continue  # ignore anything that isn't our own protocol
            if not self._requests.allow(addr[0]):
                continue

            reply = json.dumps({
                "magic": config.DISCOVERY_MAGIC,
                "type": "announce",
                "ip": netinfo.get_local_ip(),
                "port": self._get_port(),
                "name": socket.gethostname(),
                "pin_set": bool(self._get_pin()),
            }).encode("utf-8")
            try:
                self._sock.sendto(reply, addr)
            except OSError:
                pass


def discover_hosts(timeout: float = config.DISCOVERY_TIMEOUT_SECONDS) -> list[dict]:
    """
    Broadcasts a discovery request and collects replies for `timeout`
    seconds. Returns a list of {"ip","port","name"} dicts, deduplicated
    by IP. Never raises -- a network that blocks broadcast (some
    restrictive routers, some sandboxed environments) just yields an
    empty list, and manual IP entry remains the fallback.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.3)

    # Broadcast for real LAN discovery across devices.
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(_REQUEST, ("255.255.255.255", config.DISCOVERY_PORT))
    except OSError as e:
        log.debug("Broadcast discovery send failed (continuing): %s", e)

    # Also try loopback directly -- covers same-machine testing/dev,
    # and restrictive environments where broadcast is blocked but
    # localhost still works.
    try:
        sock.sendto(_REQUEST, ("127.0.0.1", config.DISCOVERY_PORT))
    except OSError as e:
        log.debug("Loopback discovery send failed (continuing): %s", e)

    found = {}
    deadline_thread = threading.Event()

    def _collect():
        import time
        end = time.time() + timeout
        while time.time() < end:
            try:
                data, addr = sock.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            if not isinstance(msg, dict) or msg.get("magic") != config.DISCOVERY_MAGIC or msg.get("type") != "announce":
                continue
            if (type(msg.get('port')) is not int or not 1 <= msg['port'] <= 65535
                    or not isinstance(msg.get('name'), str) or not 1 <= len(msg['name']) <= 128
                    or len(found) >= 256):
                continue
            found[addr[0]] = {
                "ip": addr[0],
                "port": msg.get("port"),
                "name": msg.get("name", addr[0]),
            }
        deadline_thread.set()

    t = threading.Thread(target=_collect, daemon=True)
    t.start()
    t.join(timeout=timeout + 0.5)
    sock.close()

    return list(found.values())
