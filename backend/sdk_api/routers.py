# ===========================================================================
# ROUTES
# ===========================================================================

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from backend.dashboard.services import (
    BillingService,
    CostService,
)
from backend.database.database import get_db
from backend.configs import (
    CostEventBatch,
)
from backend.configs import limiter
from backend.database.models import CostEvent, Threshold
from backend.sdk_api.cost_calculator import calculate_cost
from sqlalchemy import text

router = APIRouter(prefix="/api", tags=["backend"])


async def _fetch_db_rates(
    db: AsyncSession, provider: str, model_id: str
) -> dict | None:
    """
    Pull the most recent pricing row for (provider, model_id) from the DB.
    Falls back to None so calculate_cost can try the static file instead.
    """
    result = await db.execute(
        text("""
            SELECT
                input_per_million,
                output_per_million,
                cache_write_per_million,
                cache_read_per_million,
                batch_discount
            FROM model_pricing
            WHERE provider       = :provider
              AND model_id       = :model_id
              AND effective_from <= NOW()
            ORDER BY effective_from DESC
            LIMIT 1
        """),
        {"provider": provider, "model_id": model_id},
    )
    row = result.mappings().first()
    return dict(row) if row else None


# ── POST /ingest ─────────────────────────────────────────────────────────────
# NOTE: This endpoint is intended for SDK use only (via API key), not the
# dashboard UI. It is kept here for completeness but the dashboard never
# calls it.


@router.post("/ingest", status_code=202, tags=["costs"])
@limiter.limit("1000/minute")
async def ingest_cost_event(
    request: Request,
    events: CostEventBatch,
    db: AsyncSession = Depends(get_db),
):
    """Receive a batch of SDK cost events, calculate their cost, and persist them."""
    project_id = request.state.project_id
    billing = BillingService(db)
    await billing.enforce_limit(project_id)
    cost_svc = CostService(db)
    counter = 0

    for ev in events.events:
        # ── Resolve provider from model name ──────────────────────────────────
        from ..sdk_api.pricing import infer_provider

        provider = infer_provider(ev.model) or "unknown"

        # ── Attempt DB rates lookup first, fall back to static pricing.py ────
        db_rates = await _fetch_db_rates(db, provider, ev.model)

        breakdown = calculate_cost(
            model=ev.model,
            input_tokens=ev.input_tokens,
            output_tokens=ev.output_tokens,
            provider=provider,
            # Cache token fields — pass whatever the event carries (defaults to 0)
            cached_tokens=getattr(ev, "cached_tokens", 0),
            cache_read_tokens=getattr(ev, "cache_read_tokens", 0),
            cache_write_tokens=getattr(ev, "cache_write_tokens", 0),
            is_batch=getattr(ev, "is_batch", False),
            db_rates=db_rates,  # injected from DB; None = use static table
        )

        cost_usd = breakdown.total_cost

        await cost_svc.write_event(project_id, ev, cost_usd)
        counter += 1

    return {"accepted": counter}


from datetime import datetime, timezone
from fastapi import Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession


@router.get("/check-limit", status_code=200, tags=["check limit"])
@limiter.limit("1000/minute")
async def check_usage_limit(
    request: Request,
    tenant_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    cost_result = await db.execute(
        select(func.coalesce(func.sum(CostEvent.cost_usd), 0)).where(
            CostEvent.tenant_id == tenant_id,
            CostEvent.created_at >= month_start,
            CostEvent.created_at <= now,
        )
    )
    total_cost = float(cost_result.scalar())

    threshold_result = await db.execute(
        select(Threshold).where(Threshold.tenant_id == tenant_id)
    )
    threshold = threshold_result.scalar_one_or_none()

    if threshold is None:
        return {
            "tenant_id": tenant_id,
            "total_cost_usd": total_cost,
            "limit_usd": None,
            "limit_exceeded": False,
            "period_start": month_start.isoformat(),
            "period_end": now.isoformat(),
        }

    limit_usd = float(threshold.limit_usd)

    return {
        "tenant_id": tenant_id,
        "total_cost_usd": total_cost,
        "limit_usd": limit_usd,
        "limit_exceeded": total_cost >= limit_usd,
        "period_start": month_start.isoformat(),
        "period_end": now.isoformat(),
    }


from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import insert as pg_insert


class SetThresholdRequest(BaseModel):
    tenant_id: str
    limit_usd: float


@router.post("/thresholds", status_code=200, tags=["thresholds"])
async def set_threshold(
    request: Request,
    body: SetThresholdRequest,
    db: AsyncSession = Depends(get_db),
):
    project_id = request.state.project_id
    stmt = (
        pg_insert(Threshold)
        .values(
            project_id=project_id,
            tenant_id=body.tenant_id,
            limit_usd=body.limit_usd,
        )
        .on_conflict_do_update(
            constraint="uq_threshold_pc",
            set_={
                "limit_usd": body.limit_usd,
                "updated_at": datetime.now(timezone.utc),
            },
        )
    )
    await db.execute(stmt)
    await db.commit()

    return {"tenant_id": body.tenant_id, "limit_usd": body.limit_usd}
