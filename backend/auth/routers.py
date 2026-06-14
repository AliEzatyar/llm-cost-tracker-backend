"""
auth/router.py — Email + password authentication routes.

Routes:
  POST /auth/register   → create account
  POST /auth/login      → exchange credentials for tokens
  POST /auth/refresh    → swap a refresh token for a new access token
  POST /auth/logout     → revoke the current refresh token
  GET  /auth/me         → return the authenticated user's profile
  POST /auth/change-password → update password (requires current password)
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from pydantic import BaseModel, EmailStr, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.database import get_db
from backend.database.models import User
from .services import (
    create_access_token,
    create_refresh_token,
    get_current_user,
    hash_password,
    revoke_refresh_token,
    rotate_refresh_token,
    verify_password,
)

router = APIRouter(prefix="/auth", tags=["auth"])


# ===========================================================================
# SCHEMAS
# ===========================================================================


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters.")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit.")
        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter.")
        return v


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters.")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit.")
        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter.")
        return v


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    # refresh_token is sent as an HttpOnly cookie — NOT in the body


class UserProfile(BaseModel):
    id: str
    email: str
    is_verified: bool
    created_at: str
    last_login: str | None
    is_admin: bool
    model_config = {"from_attributes": True}


# ===========================================================================
# HELPERS
# ===========================================================================

COOKIE_NAME = "refresh_token"
COOKIE_MAX_AGE = 60 * 60 * 24 * 7  # 7 days in seconds


def _set_refresh_cookie(response: Response, raw_token: str) -> None:
    """Store the refresh token in an HttpOnly Secure cookie."""
    response.set_cookie(
        key=COOKIE_NAME,
        value=raw_token,
        httponly=True,  # not accessible from JS
        secure=True,  # HTTPS only
        samesite="lax",
        max_age=COOKIE_MAX_AGE,
        path="/auth",  # only sent to /auth/* routes
    )


def _clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(key=COOKIE_NAME, path="/auth")


# ===========================================================================
# ROUTES
# ===========================================================================

# ── POST /auth/register ───────────────────────────────────────────────────────


@router.post(
    "/register", status_code=status.HTTP_201_CREATED, response_model=TokenResponse
)
async def register(
    body: RegisterRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new user account.
    Returns an access token immediately; also sets an HttpOnly refresh token cookie.
    """
    # Check uniqueness
    existing = await db.execute(select(User).where(User.email == body.email))
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An account with this email already exists.",
        )

    user = User(
        email=body.email,
        hashed_password=hash_password(body.password),
    )
    db.add(user)
    await db.flush()  # get user.id before creating the refresh token

    access_token = create_access_token(user.id, user.email)
    raw_refresh = await create_refresh_token(user.id, db)
    _set_refresh_cookie(response, raw_refresh)

    return TokenResponse(access_token=access_token)


# ── POST /auth/login ──────────────────────────────────────────────────────────


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """
    Exchange email + password for tokens.
    Intentionally vague error message to prevent user enumeration.
    """
    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()

    # Constant-time rejection — always run verify_password even on miss
    dummy_hash = "$2b$12$placeholderplaceholderplaceholderplaceholder"
    password_ok = verify_password(
        body.password, user.hashed_password if user else dummy_hash
    )

    if not user or not password_ok:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password.",
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated.",
        )

    # Update last_login timestamp
    user.last_login = datetime.now(timezone.utc)

    access_token = create_access_token(user.id, user.email)
    raw_refresh = await create_refresh_token(user.id, db)
    _set_refresh_cookie(response, raw_refresh)

    return TokenResponse(access_token=access_token)


# ── POST /auth/refresh ────────────────────────────────────────────────────────


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    response: Response,
    refresh_token: str | None = Cookie(default=None, alias=COOKIE_NAME),
    db: AsyncSession = Depends(get_db),
):
    """
    Issue a new access token using the HttpOnly refresh token cookie.
    The old refresh token is revoked (rotation) and a new one is set.
    """
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token missing.",
        )

    new_raw, old_record = await rotate_refresh_token(refresh_token, db)

    # Fetch user to build a fresh access token
    result = await db.execute(select(User).where(User.id == old_record.user_id))
    user = result.scalar_one_or_none()

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found."
        )

    access_token = create_access_token(user.id, user.email)
    _set_refresh_cookie(response, new_raw)

    return TokenResponse(access_token=access_token)


# ── POST /auth/logout ─────────────────────────────────────────────────────────


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    response: Response,
    refresh_token: str | None = Cookie(default=None, alias=COOKIE_NAME),
    db: AsyncSession = Depends(get_db),
):
    """
    Revoke the refresh token and clear the cookie.
    The short-lived access token will expire on its own (15 min).
    """
    if refresh_token:
        await revoke_refresh_token(refresh_token, db)
    _clear_refresh_cookie(response)


# ── GET /auth/me ──────────────────────────────────────────────────────────────


@router.get("/me", response_model=UserProfile)
async def me(current_user: User = Depends(get_current_user)):
    """Return the authenticated user's profile."""
    
    return UserProfile(
        is_admin=current_user.is_admin,
        id=str(current_user.id),
        email=current_user.email,
        is_verified=current_user.is_verified,
        created_at=current_user.created_at.isoformat(),
        last_login=(
            current_user.last_login.isoformat() if current_user.last_login else None
        ),
    )


# ── POST /auth/change-password ────────────────────────────────────────────────


@router.post("/change-password", status_code=status.HTTP_204_NO_CONTENT)
async def change_password(
    body: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update the user's password after verifying their current one."""
    if not verify_password(body.current_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect.",
        )
    if body.current_password == body.new_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="New password must differ from the current password.",
        )

    current_user.hashed_password = hash_password(body.new_password)
