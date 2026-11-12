"""PII encryption (AES-256-GCM) and deterministic blind indexing (HMAC-SHA256)."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

ENV_KEY_VAR = "AU_PII_KEY"


def _parse_key_material(raw: str) -> bytes:
    s = raw.strip()
    if len(s) == 64:
        try:
            return bytes.fromhex(s)
        except ValueError:
            pass
    try:
        decoded = base64.urlsafe_b64decode(s.encode("ascii"))
        if len(decoded) == 32:
            return decoded
    except Exception:
        pass
    return hashlib.sha256(s.encode("utf-8")).digest()


def load_or_create_master_key(db_path: str | Path) -> bytes:
    """
    Resolve the 32-byte master PII key.
    1. Use AU_PII_KEY environment variable if set.
    2. Otherwise load from '<db_path>.key' if it exists, or generate and persist one.
       (For ':memory:' databases without AU_PII_KEY, generates an ephemeral key.)
    """
    env_val = os.environ.get(ENV_KEY_VAR)
    if env_val:
        return _parse_key_material(env_val)

    db_str = str(db_path)
    if db_str == ":memory:":
        return os.urandom(32)

    key_path = Path(f"{db_str}.key")
    if key_path.exists():
        return _parse_key_material(key_path.read_text(encoding="utf-8"))

    key_path.parent.mkdir(parents=True, exist_ok=True)
    master_key = os.urandom(32)
    key_path.write_text(master_key.hex(), encoding="utf-8")
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return master_key


class PIICipher:
    """Encrypts PII at rest with AES-256-GCM and creates HMAC-SHA256 blind indexes."""

    def __init__(self, master_key: bytes) -> None:
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes")
        enc_key = hmac.new(master_key, b"au:pii:aes256gcm:v1", hashlib.sha256).digest()
        idx_key = hmac.new(master_key, b"au:pii:hmac_idx:v1", hashlib.sha256).digest()
        self._aesgcm = AESGCM(enc_key)
        self._idx_hmac_base = hmac.new(idx_key, b"", hashlib.sha256)

    def blind_index(self, canonical_value: str) -> bytes:
        """Deterministic 32-byte keyed digest for equality lookups without plaintext storage."""
        h = self._idx_hmac_base.copy()
        h.update(canonical_value.encode("utf-8"))
        return h.digest()

    def encrypt_text(self, plaintext: str) -> bytes:
        nonce = os.urandom(12)
        ct = self._aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
        return nonce + ct

    def decrypt_text(self, blob: bytes) -> str:
        nonce, ct = blob[:12], blob[12:]
        return self._aesgcm.decrypt(nonce, ct, None).decode("utf-8")

    def encrypt_json(self, obj: dict[str, str]) -> bytes:
        return self.encrypt_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))

    def decrypt_json(self, blob: bytes) -> dict[str, str]:
        return json.loads(self.decrypt_text(blob))

