"""
Security helpers: filename sanitization and PIN-based pairing.

No accounts, no stored passwords -- each hosting session generates a
fresh random PIN. A client must supply it to even be considered for
connection, and the host must then explicitly accept the request.
"""

import os
import secrets


def safe_filename(name: str) -> str:
    """
    Strip any path components from a client-supplied filename so it can
    only ever refer to a file directly inside the server/client directory.
    This closes the path-traversal hole in the original project, where
    a name like '../../etc/passwd' would be used as-is.
    """
    if not name:
        raise ValueError("Empty filename")
    cleaned = os.path.basename(name.replace("\\", "/"))
    if cleaned in ("", ".", ".."):
        raise ValueError(f"Invalid filename: {name!r}")
    return cleaned


def generate_pin(length: int = 6) -> str:
    """Generate a random numeric PIN, e.g. '048213'. Zero-padded."""
    n = secrets.randbelow(10 ** length)
    return str(n).zfill(length)


def shared_file_path(directory: str, name: str) -> str:
    """Only ordinary, visible direct children are remotely shareable.

    Local users who can concurrently replace files remain outside the trust
    boundary; these checks do not claim to prevent malicious local TOCTOU races.
    """
    if (not isinstance(name, str) or safe_filename(name) != name
            or name.startswith('.') or name.lower().endswith('.part')
            or name.endswith((' ', '.')) or any(ord(c) < 32 or c in '<>:"|?*' for c in name)):
        raise ValueError('File is not available for sharing')
    path = os.path.join(directory, name)
    root = os.path.normcase(os.path.realpath(directory))
    target = os.path.normcase(os.path.realpath(path))
    if (os.path.islink(path) or os.path.commonpath([root, target]) != root
            or not os.path.isfile(path)):
        raise ValueError('File is not available for sharing')
    return path


def verify_pin(supplied: str, expected: str) -> bool:
    """Constant-time PIN comparison."""
    return secrets.compare_digest(str(supplied), str(expected))
