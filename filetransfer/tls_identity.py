"""Persisted LAN identity. Trust is an out-of-band SHA-256 certificate pin,
not unauthenticated discovery or automatic trust-on-first-use.
"""
import datetime
import hashlib
import hmac
import os
from pathlib import Path
import ssl
import threading

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from . import config

_lock = threading.Lock()


def normalize_pin(value):
    value = value.replace(':', '').replace(' ', '').lower()
    if len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('Enter the full SHA-256 fingerprint shown on the host (64 hex digits).')
    return value


def server_context(dest_dir):
    with _lock:
        folder = Path(config.STATE_DIR or Path(dest_dir) / '.ft-state') / 'tls'
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        cert_path = Path(config.TLS_CERT_FILE) if config.TLS_CERT_FILE else folder / 'identity.pem'
        key_path = Path(config.TLS_KEY_FILE) if config.TLS_KEY_FILE else cert_path
        if bool(config.TLS_CERT_FILE) != bool(config.TLS_KEY_FILE):
            raise ValueError('Configure both FT_TLS_CERT_FILE and FT_TLS_KEY_FILE.')
        if not config.TLS_CERT_FILE and not cert_path.exists():
            key = ec.generate_private_key(ec.SECP256R1())
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'File Transfer LAN host')])
            now = datetime.datetime.now(datetime.timezone.utc)
            cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
                    .public_key(key.public_key()).serial_number(x509.random_serial_number())
                    .not_valid_before(now - datetime.timedelta(minutes=5))
                    .not_valid_after(now + datetime.timedelta(days=3650))
                    .sign(key, hashes.SHA256()))
            data = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                     serialization.NoEncryption()) + cert.public_bytes(serialization.Encoding.PEM)
            # Exclusive creation: never silently rotate an existing trusted identity.
            fd = os.open(cert_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as output:
                output.write(data)
        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(cert_path, key_path)
        return ctx, cert.fingerprint(hashes.SHA256()).hex()


def pinned_socket(raw, host, expected):
    expected = normalize_pin(expected)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    # Self-signed LAN identity: certificate pin replaces the public CA/hostname
    # trust model. No application bytes may be sent before this comparison.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    wrapped = ctx.wrap_socket(raw, server_hostname=host)
    try:
        der = wrapped.getpeercert(binary_form=True)
        if not hmac.compare_digest(hashlib.sha256(der).hexdigest(), expected):
            raise ssl.SSLError('Host certificate changed or fingerprint does not match. Verify on the host.')
        cert = x509.load_der_x509_certificate(der)
        now = datetime.datetime.now(datetime.timezone.utc)
        if not cert.not_valid_before_utc <= now <= cert.not_valid_after_utc:
            raise ssl.SSLError('Host certificate is expired or not yet valid.')
        return wrapped
    except Exception:
        wrapped.close()
        raise
