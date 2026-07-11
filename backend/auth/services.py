"""
auth/service.py — Password hashing, JWT creation/validation, FastAPI dependencies.

Tokens:
  Access token  — short-lived (15 min), sent in Authorization: Bearer header
  Refresh token — long-lived (7 days), stored in DB + returned to client
                  Client sends it to POST /auth/refresh to get a new access token.

Environment variables:
  JWT_SECRET_KEY      — long random string (generate with: openssl rand -hex 32)
  JWT_ALGORITHM       — default HS256
  ACCESS_TOKEN_EXPIRE_MINUTES   — default 15
  REFRESH_TOKEN_EXPIRE_DAYS     — default 7
"""

from __future__ import annotations

import hashlib
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from ..database.database import get_db
from backend.database.models import RefreshToken, User

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SECRET_KEY: str = os.environ.get("JWT_SECRET_KEY", "CHANGE_ME_in_production")
ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))
EMAIL_OTP_EXPIRE_MINUTES: int = int(os.getenv("EMAIL_OTP_EXPIRE_MINUTES", "10"))

# ---------------------------------------------------------------------------
# Password hashing  (bcrypt via passlib)
# ---------------------------------------------------------------------------

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        res = pwd_context.verify(plain, hashed)
    except:
        return False
    return res


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def create_access_token(user_id: uuid.UUID, email: str) -> str:
    """Create a short-lived JWT access token."""
    expire = _utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": expire,
        "iat": _utcnow(),
        "type": "access",
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_access_token(token: str) -> dict:
    """
    Decode and validate a JWT. Raises HTTPException on any failure so it can
    be used directly in FastAPI dependencies.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise credentials_exception

    if payload.get("type") != "access":
        raise credentials_exception

    return payload


# ---------------------------------------------------------------------------
# Email verification via OTP (one-time code, hashed and stored on the user row)
# ---------------------------------------------------------------------------


def _hash_otp(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def generate_email_otp() -> str:
    """Generate a 6-digit numeric one-time code, e.g. '042817'."""
    return f"{secrets.randbelow(1_000_000):06d}"


async def set_email_otp(user: User, db: AsyncSession) -> str:
    """
    Generate a fresh OTP for the user, persist its hash + expiry, and return
    the raw code so the caller can email it. The raw code is never stored.
    """
    raw = generate_email_otp()
    user.otp_hash = _hash_otp(raw)
    user.otp_expires_at = _utcnow() + timedelta(minutes=EMAIL_OTP_EXPIRE_MINUTES)
    await db.flush()
    return raw


def verify_email_otp(user: User, raw_otp: str) -> bool:
    """Check a submitted code against the stored hash, respecting expiry."""
    if not user.otp_hash or not user.otp_expires_at:
        return False
    if user.otp_expires_at.replace(tzinfo=timezone.utc) < _utcnow():
        return False
    return secrets.compare_digest(user.otp_hash, _hash_otp(raw_otp))


def clear_email_otp(user: User) -> None:
    """Invalidate the code after a successful verification (one-time use)."""
    user.otp_hash = None
    user.otp_expires_at = None


# ---------------------------------------------------------------------------
# Refresh token helpers (opaque token stored in DB)
# ---------------------------------------------------------------------------


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


async def create_refresh_token(user_id: uuid.UUID, db: AsyncSession) -> str:
    """
    Generate a cryptographically random refresh token, persist its hash,
    and return the raw value to send to the client.
    """
    raw = secrets.token_urlsafe(48)
    token_hash = _hash_token(raw)
    expires_at = _utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    row = RefreshToken(
        user_id=user_id,
        token_hash=token_hash,
        expires_at=expires_at,
    )
    db.add(row)
    await db.flush()
    return raw


async def rotate_refresh_token(
    raw_token: str,
    db: AsyncSession,
) -> tuple[str, RefreshToken]:
    """
    Validate an incoming refresh token, revoke it, and issue a new one.
    Raises 401 if the token is invalid, expired, or already revoked.
    """
    token_hash = _hash_token(raw_token)
    result = await db.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    )
    record = result.scalar_one_or_none()

    if not record or record.revoked:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token."
        )

    if record.expires_at.replace(tzinfo=timezone.utc) < _utcnow():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token expired."
        )

    # Revoke old token (rotation — one-time use)
    record.revoked = True

    # Issue replacement
    new_raw = await create_refresh_token(record.user_id, db)
    return new_raw, record


async def revoke_refresh_token(raw_token: str, db: AsyncSession) -> None:
    """Mark a specific refresh token as revoked (used on logout)."""
    token_hash = _hash_token(raw_token)
    result = await db.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    )
    record = result.scalar_one_or_none()
    if record:
        record.revoked = True


# ---------------------------------------------------------------------------
# FastAPI dependency: get the currently authenticated user
# ---------------------------------------------------------------------------

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    Decode the Bearer token and return the matching User row.
    Raises 401 if the token is invalid or the user doesn't exist / is inactive.
    """
    payload = decode_access_token(token)
    user_id_str: Optional[str] = payload.get("sub")

    if not user_id_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload."
        )

    try:
        user_uuid = uuid.UUID(user_id_str)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload."
        )

    result = await db.execute(select(User).where(User.id == user_uuid))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found."
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Account is deactivated."
        )

    return user


async def get_verified_user(user: User = Depends(get_current_user)) -> User:
    """Like get_current_user but also requires the email to be verified."""
    if not user.is_verified:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Please verify your email address before continuing.",
        )
    return user
