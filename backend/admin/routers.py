# ===========================================================================
# ROUTES
# ===========================================================================

from datetime import datetime, timezone
import uuid
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.auth.services import get_current_user
from backend.database.database import get_db
from backend.database.models import User

router = APIRouter(prefix="/admin", tags=["dashboard"])


# ── Auth dependency ───────────────────────────────────────────────────────────
async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """Raise 403 if the authenticated user is not an admin."""
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# ── Pricing admin routes ──────────────────────────────────────────────────────
class PricingRowIn(BaseModel):
    provider: str = Field(..., min_length=1, max_length=60)
    model_id: str = Field(..., min_length=1, max_length=120)
    input_per_million: float = Field(..., ge=0)
    output_per_million: float = Field(..., ge=0)
    cache_write_per_million: float = Field(0.0, ge=0)
    cache_read_per_million: float = Field(0.0, ge=0)
    batch_discount: float = Field(0.5, ge=0, le=1)
    effective_from: datetime = Field(
        default_factory=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc)
    )


@router.get("/pricing", tags=["admin"])
async def list_pricing(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Return all pricing rows ordered by provider / model / date."""
    result = await db.execute(text("""
            SELECT id, provider, model_id,
                   input_per_million, output_per_million,
                   cache_write_per_million, cache_read_per_million,
                   batch_discount, effective_from, updated_at
            FROM   model_pricing
            ORDER  BY provider, model_id, effective_from DESC
        """))
    return [dict(r) for r in result.mappings()]


@router.post("/pricing", status_code=201, tags=["admin"])
async def create_pricing_row(
    body: PricingRowIn,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Insert a new pricing row (a new effective_from = new price period)."""
    row_id = str(uuid.uuid4())
    await db.execute(
        text("""
            INSERT INTO model_pricing
                (id, provider, model_id,
                 input_per_million, output_per_million,
                 cache_write_per_million, cache_read_per_million,
                 batch_discount, effective_from)
            VALUES
                (:id, :provider, :model_id,
                 :input_per_million, :output_per_million,
                 :cache_write_per_million, :cache_read_per_million,
                 :batch_discount, :effective_from)
            ON CONFLICT (provider, model_id, effective_from)
            DO UPDATE SET
                input_per_million       = EXCLUDED.input_per_million,
                output_per_million      = EXCLUDED.output_per_million,
                cache_write_per_million = EXCLUDED.cache_write_per_million,
                cache_read_per_million  = EXCLUDED.cache_read_per_million,
                batch_discount          = EXCLUDED.batch_discount,
                updated_at              = NOW()
        """),
        {"id": row_id, **body.model_dump()},
    )
    await db.commit()
    return {"id": row_id, **body.model_dump()}


@router.put("/pricing/{row_id}", tags=["admin"])
async def update_pricing_row(
    row_id: str,
    body: PricingRowIn,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Update an existing pricing row in-place (same effective_from)."""
    result = await db.execute(
        text("""
            UPDATE model_pricing SET
                provider                = :provider,
                model_id                = :model_id,
                input_per_million       = :input_per_million,
                output_per_million      = :output_per_million,
                cache_write_per_million = :cache_write_per_million,
                cache_read_per_million  = :cache_read_per_million,
                batch_discount          = :batch_discount,
                effective_from          = :effective_from,
                updated_at              = NOW()
            WHERE id = :id
            RETURNING id
        """),
        {"id": row_id, **body.model_dump()},
    )
    await db.commit()
    if not result.first():
        raise HTTPException(status_code=404, detail="Pricing row not found")
    return {"id": row_id, **body.model_dump()}


@router.delete("/pricing/{row_id}", status_code=204, tags=["admin"])
async def delete_pricing_row(
    row_id: str,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Delete a pricing row by ID."""
    await db.execute(
        text("DELETE FROM model_pricing WHERE id = :id"),
        {"id": row_id},
    )
    await db.commit()
