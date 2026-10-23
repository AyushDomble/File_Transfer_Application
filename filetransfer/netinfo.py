"""
LAN IP detection. 127.0.0.1 only works for same-machine testing -- a
client on another device needs the host's actual local network address.
"""

import socket


def get_local_ip() -> str:
    """
    Return this machine's LAN IP (e.g. '192.168.1.42').

    Trick: open a UDP socket "connected" to an external address. UDP
    connect() doesn't send any packets -- it just makes the OS pick
    which local interface/IP would be used for that route, which we
    then read back. Falls back to 127.0.0.1 if no network is available.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()
