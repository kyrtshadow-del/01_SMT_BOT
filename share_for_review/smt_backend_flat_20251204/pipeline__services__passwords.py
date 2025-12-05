"""Password hashing helpers for web users.

We avoid external dependencies and use PBKDF2-HMAC with a per-user salt.
Format of stored hash:

    pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>
"""

from __future__ import annotations

import hashlib
import hmac
import os
from typing import Tuple


def _pbkdf2_sha256(password: str, salt: bytes, iterations: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)


def hash_password(password: str, *, iterations: int = 200_000) -> str:
    """Hash a plaintext password and return the encoded string."""

    salt = os.urandom(16)
    dk = _pbkdf2_sha256(password, salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}"


def _parse(encoded: str) -> Tuple[int, bytes, bytes]:
    try:
        algo, iter_s, salt_hex, hash_hex = encoded.split("$", 3)
    except ValueError as exc:
        raise ValueError("invalid password hash format") from exc
    if algo != "pbkdf2_sha256":
        raise ValueError(f"unsupported hash algorithm: {algo}")
    try:
        iterations = int(iter_s)
    except ValueError as exc:
        raise ValueError("invalid iteration count in password hash") from exc
    try:
        salt = bytes.fromhex(salt_hex)
        digest = bytes.fromhex(hash_hex)
    except ValueError as exc:
        raise ValueError("invalid hex in password hash") from exc
    return iterations, salt, digest


def verify_password(password: str, encoded: str) -> bool:
    """Return True if password matches encoded hash."""

    try:
        iterations, salt, expected = _parse(encoded)
    except ValueError:
        return False
    candidate = _pbkdf2_sha256(password, salt, iterations)
    return hmac.compare_digest(candidate, expected)

