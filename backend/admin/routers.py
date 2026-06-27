# ===========================================================================
# ROUTES
# ===========================================================================

from datetime import datetime, timezone
import uuid
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, validator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from backend.auth.services import get_current_user
from backend.database.database import get_db
from backend.database.models import User, SYSTEM_DEFINED_MODEL

router = APIRouter(prefix="/admin", tags=["dashboard"])


# ── Auth dependency ───────────────────────────────────────────────────────────
# Still used to gate operations on SYSTEM pricing rows specifically. No longer
# applied as a blanket dependency on every pricing route — non-SYSTEM pricing
# is available to any authenticated user, scoped to their own rows.
async def require_admin(current_user: User) -> User:
    """Raise 403 if the given user is not an admin."""
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


class PricingRowIn(BaseModel):
    provider: str = Field(..., min_length=1, max_length=60)
    model_id: str = Field(..., min_length=1, max_length=120)

    # ── Ownership / scope ──────────────────────────────────────────────────────
    # model_type determines the authorization + uniqueness rules for this row.
    # "SYSTEM" rows are global and admin-managed; any other value is a
    # user-owned custom pricing row scoped to author_id.
    # NOTE: author_id is intentionally NOT a field here. It must never be
    # client-suppliable — the server always sets it from the authenticated
    # user (or NULL for SYSTEM rows). See create_pricing_row / update_pricing_row.
    model_type: str = Field(SYSTEM_DEFINED_MODEL, min_length=1, max_length=40)

    # ── Text token rates ──────────────────────────────────────────────────────
    input_per_million: float = Field(..., ge=0)
    output_per_million: float = Field(..., ge=0)
    cache_write_per_million: float = Field(0.0, ge=0)
    cache_read_per_million: float = Field(0.0, ge=0)

    # ── Billing mode flags ────────────────────────────────────────────────────
    audio_billing: str = Field("per_minute")  # "per_minute" | "per_token"
    speech_billing: str = Field("per_character")  # "per_character" | "per_token"

    # ── Modality-specific token rates ─────────────────────────────────────────
    # Audio token billing: gpt-4o-transcribe (token mode), gpt-4o-realtime, gpt-4o-mini-tts
    audio_input_per_million: float = Field(0.0, ge=0)
    audio_output_per_million: float = Field(0.0, ge=0)

    # Image token billing (gpt-image-1 family) — always per-token, five dimensions
    image_text_input_per_million: float = Field(0.0, ge=0)
    image_cached_text_input_per_million: float = Field(0.0, ge=0)
    image_input_per_million: float = Field(0.0, ge=0)
    image_cached_input_per_million: float = Field(0.0, ge=0)
    image_output_per_million: float = Field(0.0, ge=0)

    # ── Legacy flat rates ─────────────────────────────────────────────────────
    per_minute_rate: float = Field(0.0, ge=0)  # whisper-1, gpt-4o-transcribe simple
    per_1k_chars_rate: float = Field(0.0, ge=0)  # tts-1, tts-1-hd

    batch_discount: float = Field(0.5, ge=0, le=1)
    effective_from: datetime = Field(
        default_factory=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc)
    )

    @validator("model_type")
    def normalize_model_type(cls, v):
        return v.strip().upper()

    @validator("audio_billing")
    def validate_audio_billing(cls, v):
        if v not in ("per_minute", "per_token"):
            raise ValueError("audio_billing must be 'per_minute' or 'per_token'")
        return v

    @validator("speech_billing")
    def validate_speech_billing(cls, v):
        if v not in ("per_character", "per_token"):
            raise ValueError("speech_billing must be 'per_character' or 'per_token'")
        return v


@router.get("/pricing")
async def list_pricing(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return system pricing and the current user's custom pricing."""
    result = await db.execute(
        text("""
            SELECT id, provider, model_id, model_type, author_id,
                   input_per_million, output_per_million,
                   cache_write_per_million, cache_read_per_million,
                   audio_billing, speech_billing,
                   audio_input_per_million, audio_output_per_million,
                   image_text_input_per_million, image_cached_text_input_per_million,
                   image_input_per_million, image_cached_input_per_million,
                   image_output_per_million,
                   per_minute_rate, per_1k_chars_rate,
                   batch_discount, effective_from, updated_at
            FROM model_pricing
            WHERE model_type = 'SYSTEM'
               OR author_id = :author_id
            ORDER BY provider, model_id, effective_from DESC
        """),
        {"author_id": user.id},
    )

    return [dict(r) for r in result.mappings()]


@router.post("/pricing", status_code=201)
async def create_pricing_row(
    body: PricingRowIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Insert a brand-new pricing record. Never updates an existing one.

    Authorization:
      - model_type == 'SYSTEM': only admins may create.
      - model_type != 'SYSTEM': any authenticated user may create; the row
        is always owned by the caller (author_id is server-assigned, never
        taken from the request body).

    Cross-scope rule:
      If a SYSTEM row already exists for (provider, model_id), NO new row
      may be inserted for that pair by anyone — not another SYSTEM row,
      and not a user's custom row. The only valid way to change pricing
      for a (provider, model_id) that already has a SYSTEM row is for an
      admin to PUT/update that existing row. This is checked explicitly
      below because it spans two different uniqueness scopes and can't be
      expressed as a single Postgres constraint.

    Uniqueness (within scope, once the cross-scope rule above has passed):
      - SYSTEM:     one row per (provider, model_id)
      - non-SYSTEM: one row per (provider, model_id, model_type, author_id)

    On a same-scope conflict, the insert is skipped (ON CONFLICT DO NOTHING)
    and we return 409 — no pre-insert existence check for that part,
    Postgres enforces it. The cross-scope rule above is the one case that
    does need an explicit pre-check, with FOR UPDATE locking to close the
    race against a concurrent insert of the same SYSTEM row.
    """
    is_system = body.model_type == SYSTEM_DEFINED_MODEL

    if is_system:
        await require_admin(user)
        author_id = None
    else:
        author_id = user.id

    # Cross-scope guard: lock and check for an existing SYSTEM row on this
    # (provider, model_id) before attempting ANY insert (SYSTEM or user).
    # FOR UPDATE ensures a concurrent request creating the SYSTEM row can't
    # slip in between this check and our insert.
    existing_system = await db.execute(
        text("""
            SELECT id FROM model_pricing
            WHERE provider = :provider
              AND model_id = :model_id
              AND model_type = :system_type
            FOR UPDATE
        """),
        {
            "provider": body.provider,
            "model_id": body.model_id,
            "system_type": SYSTEM_DEFINED_MODEL,
        },
    )
    if existing_system.first() is not None:
        await db.rollback()
        raise HTTPException(
            status_code=409,
            detail="SYSTEM pricing already exists for this provider/model.",
        )

    row_id = str(uuid.uuid4())
    params = {
        "id": row_id,
        "author_id": author_id,
        **body.model_dump(),
    }

    if is_system:
        # Uniqueness scoped to (provider, model_id) for SYSTEM rows, enforced
        # by the partial unique index defined in models.py:
        #   Index("uq_model_pricing_system", "provider", "model_id",
        #         unique=True, postgresql_where=(model_type == "SYSTEM"))
        # Partial indexes must be targeted by their column list + predicate,
        # not by name, in ON CONFLICT. (This also backstops the FOR UPDATE
        # guard above against any edge case it doesn't catch.)
        result = await db.execute(
            text("""
                INSERT INTO model_pricing
                    (id, provider, model_id, model_type, author_id,
                     input_per_million, output_per_million,
                     cache_write_per_million, cache_read_per_million,
                     audio_billing, speech_billing,
                     audio_input_per_million, audio_output_per_million,
                     image_text_input_per_million, image_cached_text_input_per_million,
                     image_input_per_million, image_cached_input_per_million,
                     image_output_per_million,
                     per_minute_rate, per_1k_chars_rate,
                     batch_discount, effective_from)
                VALUES
                    (:id, :provider, :model_id, :model_type, :author_id,
                     :input_per_million, :output_per_million,
                     :cache_write_per_million, :cache_read_per_million,
                     :audio_billing, :speech_billing,
                     :audio_input_per_million, :audio_output_per_million,
                     :image_text_input_per_million, :image_cached_text_input_per_million,
                     :image_input_per_million, :image_cached_input_per_million,
                     :image_output_per_million,
                     :per_minute_rate, :per_1k_chars_rate,
                     :batch_discount, :effective_from)
                ON CONFLICT (provider, model_id) WHERE model_type = 'SYSTEM'
                DO NOTHING
                RETURNING id
            """),
            params,
        )
    else:
        # Uniqueness scoped to (provider, model_id, model_type, author_id)
        # for non-SYSTEM rows, enforced by the named UniqueConstraint
        # "uq_model_pricing_user" defined on ModelPricing in models.py.
        result = await db.execute(
            text("""
                INSERT INTO model_pricing
                    (id, provider, model_id, model_type, author_id,
                     input_per_million, output_per_million,
                     cache_write_per_million, cache_read_per_million,
                     audio_billing, speech_billing,
                     audio_input_per_million, audio_output_per_million,
                     image_text_input_per_million, image_cached_text_input_per_million,
                     image_input_per_million, image_cached_input_per_million,
                     image_output_per_million,
                     per_minute_rate, per_1k_chars_rate,
                     batch_discount, effective_from)
                VALUES
                    (:id, :provider, :model_id, :model_type, :author_id,
                     :input_per_million, :output_per_million,
                     :cache_write_per_million, :cache_read_per_million,
                     :audio_billing, :speech_billing,
                     :audio_input_per_million, :audio_output_per_million,
                     :image_text_input_per_million, :image_cached_text_input_per_million,
                     :image_input_per_million, :image_cached_input_per_million,
                     :image_output_per_million,
                     :per_minute_rate, :per_1k_chars_rate,
                     :batch_discount, :effective_from)
                ON CONFLICT ON CONSTRAINT uq_model_pricing_user
                DO NOTHING
                RETURNING id
            """),
            params,
        )

    inserted = result.first()
    if inserted is None:
        await db.rollback()
        if is_system:
            raise HTTPException(
                status_code=409,
                detail="SYSTEM pricing already exists for this provider/model.",
            )
        raise HTTPException(
            status_code=409,
            detail="You already have pricing for this provider/model.",
        )

    await db.commit()
    return {"id": row_id, "author_id": author_id, **body.model_dump()}


@router.put("/pricing/{row_id}")
async def update_pricing_row(
    row_id: str,
    body: PricingRowIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Update an existing pricing record in place. Never creates a new one.

    Authorization:
      - SYSTEM row: only admins may update.
      - non-SYSTEM row: only the owner (row.author_id == user.id) may update.
    author_id and model_type are immutable and are never written here.
    """
    existing = await db.execute(
        text("SELECT id, model_type, author_id FROM model_pricing WHERE id = :id"),
        {"id": row_id},
    )
    row = existing.mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Pricing row not found")

    if row["model_type"] == SYSTEM_DEFINED_MODEL:
        await require_admin(user)
    elif row["author_id"] != user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to update this pricing row.",
        )

    params = {"id": row_id, **body.model_dump(exclude={"model_type"})}

    try:
        result = await db.execute(
            text("""
                UPDATE model_pricing SET
                    provider                            = :provider,
                    model_id                            = :model_id,
                    input_per_million                   = :input_per_million,
                    output_per_million                  = :output_per_million,
                    cache_write_per_million              = :cache_write_per_million,
                    cache_read_per_million               = :cache_read_per_million,
                    audio_billing                        = :audio_billing,
                    speech_billing                       = :speech_billing,
                    audio_input_per_million              = :audio_input_per_million,
                    audio_output_per_million             = :audio_output_per_million,
                    image_text_input_per_million         = :image_text_input_per_million,
                    image_cached_text_input_per_million  = :image_cached_text_input_per_million,
                    image_input_per_million              = :image_input_per_million,
                    image_cached_input_per_million       = :image_cached_input_per_million,
                    image_output_per_million             = :image_output_per_million,
                    per_minute_rate                      = :per_minute_rate,
                    per_1k_chars_rate                    = :per_1k_chars_rate,
                    batch_discount                       = :batch_discount,
                    effective_from                       = :effective_from,
                    updated_at                            = NOW()
                WHERE id = :id
                RETURNING id, provider, model_id, model_type, author_id,
                          input_per_million, output_per_million,
                          cache_write_per_million, cache_read_per_million,
                          audio_billing, speech_billing,
                          audio_input_per_million, audio_output_per_million,
                          image_text_input_per_million, image_cached_text_input_per_million,
                          image_input_per_million, image_cached_input_per_million,
                          image_output_per_million,
                          per_minute_rate, per_1k_chars_rate,
                          batch_discount, effective_from, updated_at
            """),
            params,
        )
        updated = result.mappings().first()
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Another pricing record already exists with the same provider/model/type/owner.",
        )

    if updated is None:
        # Row existed in the pre-check but vanished before the UPDATE
        # (e.g. concurrent delete) — treat as not found.
        raise HTTPException(status_code=404, detail="Pricing row not found")

    return dict(updated)


@router.delete("/pricing/{row_id}", status_code=204, tags=["admin"])
async def delete_pricing_row(
    row_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Delete a pricing row by ID.

    Not covered by the provided spec, so the prior access rule is kept but
    loosened consistently with create/update: admins may delete SYSTEM rows,
    owners may delete their own non-SYSTEM rows.
    """
    existing = await db.execute(
        text("SELECT model_type, author_id FROM model_pricing WHERE id = :id"),
        {"id": row_id},
    )
    row = existing.mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Pricing row not found")

    if row["model_type"] == SYSTEM_DEFINED_MODEL:
        await require_admin(user)
    elif row["author_id"] != user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to delete this pricing row.",
        )

    await db.execute(
        text("DELETE FROM model_pricing WHERE id = :id"),
        {"id": row_id},
    )
    await db.commit()