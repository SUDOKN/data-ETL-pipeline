import hashlib
import secrets
from typing import Optional


def derive_key(password: str, salt: str) -> bytes:
    """Derive a key using PBKDF2 with SHA-512"""
    password_bytes = password.encode("utf-8")
    salt_bytes = salt.encode("utf-8")

    return hashlib.pbkdf2_hmac(
        "sha512",
        password_bytes,
        salt_bytes,
        1000,  # iterations
        64,  # 512 bits / 8 = 64 bytes
    )


def hash_password(password: str, salt: Optional[str]) -> tuple[str, str]:
    """Hash a password with optional salt"""
    if not salt:
        salt = secrets.token_hex(16)

    derived_key = derive_key(password, salt)
    return salt, derived_key.hex()


def verify_password(candidate_password: str, salt: str, hash: str) -> bool:
    """Verify a password against a hash"""
    derived_key = derive_key(candidate_password, salt)
    return derived_key.hex() == hash
