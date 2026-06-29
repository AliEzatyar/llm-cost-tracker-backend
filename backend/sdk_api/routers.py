# ===========================================================================
# ROUTES
# ===========================================================================
from datetime import datetime, time, timezone
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, Request, status
from backend.auth.services import get_current_user
import redis
from sqlalchemy.ext.asyncio import AsyncSession
from backend import redis_client
from backend.dashboard.services import (
    CostService,
)
import json
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import insert as pg_insert

from backend.database.database import get_db
from backend.redis_client import _cache_get, _cache_set
from backend.sdk_api.schemas import (
    CostEventBatch,
)
from backend.configs import get_redis, limiter
from backend.database.models import (
    CostEvent,
    ModelPricing,
    Threshold,
    User,
    get_project_by_api_key,
)
from backend.sdk_api.cost_calculator import calculate_cost
from sqlalchemy import text
from datetime import datetime, timezone
from fastapi import Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
import logging

router = APIRouter(prefix="/api", tags=["backend"])
logger = logging.getLogger(__name__)


from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def _fetch_db_rates(
    db: AsyncSession,
    provider: str,
    model_id: str,
    user_id: int | None,
) -> dict | None:
    """
    Fetch the latest pricing for (provider, model_id).

    Priority:
      1. SYSTEM pricing (author_id IS NULL)
      2. Current user's custom pricing (author_id = :user_id)

    Other users' pricing is never considered.
    """
    print("getting rates form db, user id:", user_id)
    result = await db.execute(
        text("""
            SELECT
                input_per_million,
                output_per_million,
                cache_write_per_million,
                cache_read_per_million,
                batch_discount,
                audio_billing,
                speech_billing,
                audio_input_per_million,
                audio_output_per_million,
                image_text_input_per_million,
                image_cached_text_input_per_million,
                image_input_per_million,
                image_cached_input_per_million,
                image_output_per_million,
                per_minute_rate,
                per_1k_chars_rate
            FROM model_pricing
            WHERE provider = :provider
              AND model_id = :model_id
              AND effective_from <= NOW()
              AND (
                    author_id IS NULL
                 OR author_id = :author_id
              )
            ORDER BY
                CASE
                    WHEN author_id IS NULL THEN 0
                    ELSE 1
                END,
                effective_from DESC
            LIMIT 1
        """),
        {
            "provider": provider,
            "model_id": model_id,
            "author_id": user_id,
        },
    )

    row = result.mappings().first()
    return dict(row) if row else None


# ───────────────────────────────────────────────────────────────
#           Ingest Cost Event Endpoint
# ───────────────────────────────────────────────────────────────
# NOTE: This endpoint is intended for SDK use only (via API key), not the
# dashboard UI. It is kept here for completeness but the dashboard never
# calls it.
async def check_plan_limits(request: Request):
    project = request.state.project
    if project["is_blocked"] == True:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={
                "code": "plan_limit_reached",
                "message": f"Project {project['id']} must upgrade to access this feature.",
            },
        )


@router.post(
    "/ingest",
    status_code=202,
    tags=["costs"],
    dependencies=[Depends(check_plan_limits)],
)
@limiter.limit("1000/seconds")
async def ingest_cost_event(
    request: Request,
    events: CostEventBatch,
    db: AsyncSession = Depends(get_db),
    redis_client=Depends(get_redis),
):
    """Receive a batch of SDK cost events, calculate their cost, and persist them."""
    project_id = request.state.project_id
    cost_svc = CostService(db)
    counter = 0

    for ev in events.events:
        from ..sdk_api.pricing import infer_provider
        print("event:::",ev.total_tokens)
        provider = infer_provider(ev.model) or ev.provider or "unknown"
        tenant_id = ev.tenant_id
        db_rates = await _fetch_db_rates(db, provider, ev.model,request.state.user_id)
        breakdown = calculate_cost(
            model=ev.model,
            input_tokens=ev.input_tokens,
            output_tokens=ev.output_tokens,
            provider=provider,
            cached_tokens=ev.cached_tokens,
            cache_read_tokens=getattr(ev, "cache_read_tokens", 0),
            cache_write_tokens=getattr(ev, "cache_write_tokens", 0),
            audio_input_tokens=ev.audio_input_tokens,
            audio_output_tokens=ev.audio_output_tokens,
            duration_seconds=ev.duration_seconds,
            character_count=ev.character_count,
            input_text_tokens=ev.input_text_tokens,
            cached_input_text_tokens=ev.cached_input_text_tokens,
            input_image_tokens=ev.input_image_tokens,
            cached_input_image_tokens=ev.cached_input_image_tokens,
            output_image_tokens=ev.output_image_tokens,
            is_batch=getattr(ev, "is_batch", False),
            db_rates=db_rates,
        )
        cost_usd = breakdown.total_cost
        await cost_svc.write_event(project_id, ev, cost_usd)

        # Keep any already-warm cost cache entries for this tenant/project in
        # sync, rather than invalidating them. The event is saved to the DB
        # regardless of cache state; this only updates the cache so
        # check-tenant-limit doesn't have to wait out the TTL or hit the DB
        # to see this event. Never creates a cache key (see apply_cost_to_cache).
        await apply_cost_to_cache(redis_client, tenant_id, project_id, cost_usd)

        counter += 1

    return {"accepted": counter}


# ────────────────────────────────────────────────────────────────────────────────────────
#           Check tenant usage Endpoint
# ────────────────────────────────────────────────────────────────────────────────────────


async def get_today_total_cost(db: Session, tenant_id: str, project_id: str) -> float:
    # 1. Calculate the UTC boundaries
    now_utc = datetime.now(timezone.utc)
    start_of_day_utc = datetime.combine(now_utc.date(), time.min, tzinfo=timezone.utc)

    # 2. Build the sum query
    stmt = select(func.sum(CostEvent.cost_usd)).where(
        CostEvent.created_at >= start_of_day_utc,
        CostEvent.created_at <= now_utc,
        CostEvent.tenant_id == tenant_id,
        CostEvent.project_id == project_id,
    )

    # 3. Execute and extract the scalar value
    total_cost = await db.scalar(stmt)

    # 4. Handle None case if no records exist yet today
    return float(total_cost) if total_cost is not None else 0.0


from datetime import datetime, timezone
import calendar
from sqlalchemy import func, select
from sqlalchemy.orm import Session


async def get_custom_monthly_cost(
    db: AsyncSession, effective_date_iso: str, tenant_id: str, project_id: str
) -> float:
    """
    Calculates total cost for the current custom billing month.
    Input format: ISO string (e.g., '2026-04-05T14:56:00+00:00')
    """
    # 1. Parse the ISO string and standardize to UTC
    effective_dt = datetime.fromisoformat(effective_date_iso).astimezone(timezone.utc)

    # 2. Get the current exact live moment in UTC
    now_utc = datetime.now(timezone.utc)

    # 3. Try to build the anchor start time using the current calendar month
    try:
        start_time = now_utc.replace(
            day=effective_dt.day,
            hour=effective_dt.hour,
            minute=effective_dt.minute,
            second=effective_dt.second,
            microsecond=0,
        )
    except ValueError:
        # Fallback if current month has fewer days than the anchor day (e.g., 31st vs 30th)
        _, last_day = calendar.monthrange(now_utc.year, now_utc.month)
        start_time = now_utc.replace(
            day=last_day,
            hour=effective_dt.hour,
            minute=effective_dt.minute,
            second=0,
            microsecond=0,
        )

    # 4. If the anchor time hasn't happened yet this month, roll back to last month
    if start_time > now_utc:
        prev_month = now_utc.month - 1 if now_utc.month > 1 else 12
        prev_year = now_utc.year if now_utc.month > 1 else now_utc.year - 1

        try:
            start_time = start_time.replace(
                year=prev_year, month=prev_month, day=effective_dt.day
            )
        except ValueError:
            # NOTE: also guards the case where start_time.day was already
            # clamped to a "last_day" value that's invalid for prev_month
            # (e.g. clamped to 30 from a 31-day month, but prev_month is Feb).
            _, last_day = calendar.monthrange(prev_year, prev_month)
            start_time = start_time.replace(
                year=prev_year, month=prev_month, day=last_day
            )

    # 5. Execute database aggregation sum
    stmt = select(func.sum(CostEvent.cost_usd)).where(
        CostEvent.created_at >= start_time,
        CostEvent.created_at <= now_utc,
        CostEvent.tenant_id == tenant_id,
        CostEvent.project_id == project_id,
    )

    total_cost = await db.scalar(stmt)
    return float(total_cost) if total_cost is not None else 0.0


# ── cache tuning ───────────────────────────────────────────────────────
THRESHOLD_CACHE_TTL = 24 * 3600  # 6 hours — thresholds change rarely, admin-driven
COST_CACHE_TTL = 10 * 60  # 10 min — safe now that ingest_cost_event keeps the cache
# in sync on every write (see apply_cost_to_cache below).
# Falls back to a DB recompute only on first use after a
# threshold is newly configured, or after 10 idle minutes.


async def _get_cached_cost(
    redis_client: redis.Redis,
    cache_key: str,
    compute_fn,
) -> float:
    """
    Generic short-TTL cache wrapper for a cost computation.
    `compute_fn` is a zero-arg callable (sync or already-awaited) returning a float.
    """
    cached = await _cache_get(redis_client, cache_key)
    if cached is not None:
        try:
            return float(cached)
        except (ValueError, TypeError):
            logger.warning(
                "corrupt cost cache value for key=%s, recomputing", cache_key
            )

    cost_usd = await compute_fn()
    await _cache_set(redis_client, cache_key, str(cost_usd), ex=COST_CACHE_TTL)
    return cost_usd


# Atomically: if the key exists, increment it by `amount` and refresh its TTL
# to `ttl` seconds; if the key does NOT exist, do absolutely nothing — the
# key is never created here. Daily/monthly cost cache keys are only ever
# created by get_today_total_cost_cached / get_custom_monthly_cost_cached
# (i.e. via check-tenant-limit), which only run for tenants that actually
# have a Threshold row for that frequency. If ingest created keys on its
# own, the existence of a key would no longer reliably indicate "this
# tenant has a threshold of this frequency."
# Combining the existence check + INCRBYFLOAT + EXPIRE into one Lua script
# also avoids a check-then-act race under concurrent ingest requests, where
# the key could expire (or never have existed) between a separate EXISTS
# and INCRBYFLOAT call.
_INCR_IF_EXISTS_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    local new_val = redis.call('INCRBYFLOAT', KEYS[1], ARGV[1])
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    return new_val
else
    return false
end
"""


async def _increment_cost_cache_if_exists(
    redis_client: redis.Redis,
    cache_key: str,
    amount: float,
    ttl: int = COST_CACHE_TTL,
) -> None:
    """
    Bumps an existing cost cache entry by `amount` and resets its TTL.
    Does nothing if the key isn't currently cached — this function must
    never create a key. See _INCR_IF_EXISTS_LUA comment for why.
    """
    try:
        await redis_client.eval(
            _INCR_IF_EXISTS_LUA, 1, cache_key, str(amount), str(ttl)
        )
    except Exception:
        # Cache is a performance optimization, not a source of truth — never
        # let a Redis hiccup break event ingestion.
        logger.warning(
            "failed to increment cost cache for key=%s", cache_key, exc_info=True
        )


async def apply_cost_to_cache(
    redis_client: redis.Redis,
    tenant_id: str,
    project_id: str,
    cost_usd: float,
) -> None:
    """
    Increments both the daily and monthly cached cost for a tenant/project
    by cost_usd, but only for whichever of the two keys are already cached.

    Never creates a daily or monthly cache key. A key's existence reflects
    "this tenant has a Threshold row for this frequency" (the only thing
    that populates these keys, via check-tenant-limit). If neither key is
    cached — no threshold configured yet, or the TTL lapsed with no
    traffic — this is a no-op; the next check-tenant-limit call will
    populate from the DB as normal, and that DB read already includes this
    event since it's written before this function is called.
    """
    daily_key = f"cost:daily:{tenant_id}:{project_id}"
    monthly_key = f"cost:monthly:{tenant_id}:{project_id}"

    await _increment_cost_cache_if_exists(redis_client, daily_key, cost_usd)
    await _increment_cost_cache_if_exists(redis_client, monthly_key, cost_usd)


async def get_today_total_cost_cached(
    db: AsyncSession, redis_client: redis.Redis, tenant_id: str, project_id: str
) -> float:
    cache_key = f"cost:daily:{tenant_id}:{project_id}"

    async def _compute():
        return await get_today_total_cost(db, tenant_id, project_id=project_id)

    return await _get_cached_cost(redis_client, cache_key, _compute)


async def get_custom_monthly_cost_cached(
    db: AsyncSession,
    redis_client: redis.Redis,
    tenant_id: str,
    effective_date_iso: str,
    project_id: str,
) -> float:
    cache_key = f"cost:monthly:{tenant_id}:{project_id}"

    async def _compute():
        return await get_custom_monthly_cost(
            db, effective_date_iso, tenant_id, project_id=project_id
        )

    return await _get_cached_cost(redis_client, cache_key, _compute)


async def _get_thresholds(
    redis_client: redis.Redis,
    db: AsyncSession,
    project_id: str,
    tenant_id: str,
) -> list[Threshold] | None:
    # --------------------------------------------------------------------------------
    #       Check cache
    # --------------------------------------------------------------------------------
    thresholds = []
    cache_key = f"thr:{project_id}:{tenant_id}"
    user_thresholds_cached = await _cache_get(redis_client, cache_key)

    if user_thresholds_cached is not None:
        try:
            data = json.loads(user_thresholds_cached)
            return data
        except (json.JSONDecodeError, KeyError):
            logger.warning(
                "corrupt threshold cache value for tenant_id=%s, refetching", tenant_id
            )

    # --------------------------------------------------------------------------------
    #       Get from db
    # --------------------------------------------------------------------------------
    user_daily_threshold_result = await db.execute(
        select(Threshold).where(
            Threshold.tenant_id == tenant_id, Threshold.frequency == Threshold.PER_DAY
        )
    )
    daily_threshold = user_daily_threshold_result.scalar_one_or_none()
    if daily_threshold is not None:
        thresholds.append(daily_threshold.to_dict())

    # monthly
    user_monthly_threshold_result = await db.execute(
        select(Threshold).where(
            Threshold.tenant_id == tenant_id, Threshold.frequency == Threshold.PER_MONTH
        )
    )
    monthly_threshold = user_monthly_threshold_result.scalar_one_or_none()
    if monthly_threshold is not None:
        thresholds.append(monthly_threshold.to_dict())

    await _cache_set(
        redis_client, cache_key, json.dumps(thresholds), ex=THRESHOLD_CACHE_TTL
    )
    # --------------------------------------------------------------------------------
    #       return result
    # --------------------------------------------------------------------------------
    return thresholds


@router.get("/check-tenant-limit", status_code=200, tags=["check tenant limit"])
@limiter.limit("1000/second")
async def check_tenant_usage_limit(
    request: Request,
    tenant_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
):
    limit_exceeded = False
    project_id = request.state.project_id
    thresholds = await _get_thresholds(redis_client, db, project_id, tenant_id)

    for t in thresholds:
        if t["frequency"] == Threshold.PER_DAY:
            print("frequence per day..")
            cost_usd = await get_today_total_cost_cached(
                db, redis_client, tenant_id, project_id
            )
            print("cost usd this day", cost_usd)
            print("threshold limit", t["limit_usd"])
            if cost_usd >= t["limit_usd"]:
                limit_exceeded = True
        elif t["frequency"] == Threshold.PER_MONTH:
            effective_date = t["effective_date"]
            cost_usd = await get_custom_monthly_cost_cached(
                db, redis_client, tenant_id, effective_date, project_id=project_id
            )
            if cost_usd >= t["limit_usd"]:
                limit_exceeded = True

    return {
        "limit_exceeded": limit_exceeded,
    }


class SetThresholdRequest(BaseModel):
    tenant_id: str
    limit_usd: float
    effective_date: str
    frequency: str


@router.post("/thresholds", status_code=200, tags=["thresholds"])
async def set_threshold(
    request: Request,
    body: SetThresholdRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
):
    project_id = request.state.project_id
    stmt = (
        pg_insert(Threshold)
        .values(
            project_id=project_id,
            tenant_id=body.tenant_id,
            limit_usd=body.limit_usd,
            effective_date=datetime.fromisoformat(body.effective_date),
            frequency=body.frequency,
        )
        .on_conflict_do_update(
            constraint="uq_threshold_pc",
            set_={
                "limit_usd": body.limit_usd,
                "updated_at": datetime.now(timezone.utc),
                "frequency": body.frequency,
                "effective_date": datetime.fromisoformat(body.effective_date),
            },
        )
    )

    await db.execute(stmt)
    await db.commit()
    # Invalidate the cache so check-tenant-limit picks up the new limit
    # immediately instead of waiting out THRESHOLD_CACHE_TTL. Soft-fail:
    # a stale entry self-heals within the TTL anyway, so a Redis hiccup
    # here shouldn't turn a successful write into a 500.
    try:
        await redis_client.delete(f"thr:{project_id}:{body.tenant_id}")
    except Exception:
        logger.warning(
            "failed to invalidate threshold cache for tenant_id=%s",
            body.tenant_id,
            exc_info=True,
        )
    return {"tenant_id": body.tenant_id, "limit_usd": body.limit_usd}
