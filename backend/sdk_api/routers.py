# ===========================================================================
# ROUTES
# ===========================================================================
from datetime import datetime, time, timezone
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, Request, status
import redis
from sqlalchemy.ext.asyncio import AsyncSession
from backend.dashboard.services import (
    CostService,
)
import json
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import insert as pg_insert

from backend.database.database import get_db, AsyncSessionLocal
from backend.redis_client import _cache_get, _cache_set
from backend.sdk_api.schemas import (
    CostEventBatch,
)
from backend.configs import get_redis, limiter
from backend.database.models import (
    CostEvent,
    Threshold,
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


async def _fetch_db_rates_batch(
    db: AsyncSession,
    pairs: set[tuple[str, str]],
    user_id: int | None,
) -> dict[tuple[str, str], dict | None]:
    """
    Fetch the latest pricing for every distinct (provider, model_id) pair
    in a single query.

    NOTE on scope: `user_id` (author_id) comes from `request.state.user_id`,
    which is constant for the whole /ingest request — it is NOT a per-event
    dimension. The old _fetch_db_rates() was being called once per event
    with the same user_id every time, issuing up to len(events) separate
    round-trips (minus in-request cache hits) when at most
    len({(provider, model_id) for ev in events}) distinct rows were ever
    needed. This fetches all of them in one query instead.

    Priority per pair (unchanged from the old per-event lookup):
      1. SYSTEM pricing (author_id IS NULL)
      2. Caller's custom pricing (author_id = :user_id)
    Tie-broken by most recent effective_from within whichever tier wins.
    """
    if not pairs:
        return {}

    pair_list = list(pairs)
    values_clause = ", ".join(f"(:p{i}, :m{i})" for i in range(len(pair_list)))
    params: dict[str, object] = {"author_id": user_id}
    for i, (provider, model_id) in enumerate(pair_list):
        params[f"p{i}"] = provider
        params[f"m{i}"] = model_id

    result = await db.execute(
        text(f"""
            SELECT
                provider,
                model_id,
                author_id,
                effective_from,
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
            WHERE (provider, model_id) IN ({values_clause})
              AND effective_from <= NOW()
              AND (
                    author_id IS NULL
                 OR author_id = :author_id
              )
        """),
        params,
    )

    rows = [dict(r) for r in result.mappings().all()]

    # Replicate the old ORDER BY (author_id IS NULL ? 0 : 1) ASC,
    # effective_from DESC, LIMIT 1 semantics per (provider, model_id) group.
    # Two stable sorts: effective_from DESC first, then rank ASC — Python's
    # sort is stable, so the final order is rank primary, effective_from
    # secondary, exactly matching the original single-row query per pair.
    rows.sort(key=lambda r: r["effective_from"], reverse=True)
    rows.sort(key=lambda r: 0 if r["author_id"] is None else 1)

    best: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (row["provider"], row["model_id"])
        if key not in best:
            best[key] = row

    return {pair: best.get(pair) for pair in pair_list}


# ───────────────────────────────────────────────────────────────
#           Ingest Cost Event Endpoint
# ───────────────────────────────────────────────────────────────
# NOTE: This endpoint is intended for SDK use only (via API key), not the
# dashboard UI. It is kept here for completeness but the dashboard never
# calls it.
#
# As of the Redis Streams refactor, this endpoint validates the request,
# calculates cost, updates the Redis tenant cost cache, and XADDs the
# event to the durable write queue. It does NOT write to PostgreSQL —
# that's the worker's job (workers/cost_event_worker.py), which reads
# the stream via a consumer group and bulk-inserts in batches.


async def check_plan_limits(request: Request):
    """
    protects from non-project requests and blocked projects
    """
    project = getattr(request.state, "project", None)
    if project in (None,""):
        logger.error("unauthorized request: project not found in request state")
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "unauthorized",
                "message": "Auth failed",
            },
        )
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
async def ingest_cost_event(
    request: Request,
    events: CostEventBatch,
    redis_client=Depends(get_redis),
):
    """
    Receive a batch of SDK cost events, calculate their cost, update the
    live Redis tenant cache, and enqueue each event onto the durable
    Redis Stream write queue (see backend.dashboard.streams) before
    returning 202. This endpoint no longer writes to PostgreSQL directly
    — a separate worker process consumes the stream and performs the
    durable insert. See workers/cost_event_worker.py.

    Perf notes (see conversation history for the load-test diagnosis this
    fixes):
      - No more `db: AsyncSession = Depends(get_db)`. That tied a Postgres
        connection's checkout to the entire request lifetime — including
        the whole Redis loop below — even though the only DB work needed
        is a single rate lookup taking milliseconds. Under load this held
        connections open for 6-17s each instead of ms, exhausting the pool
        long before Postgres's own max_connections math would predict.
        A short-lived session is opened below just for that lookup.
      - Rate lookup batched into ONE query for all distinct
        (provider, model_id) pairs in the batch (see _fetch_db_rates_batch),
        instead of one query per event.
      - Cache-sync writes (apply_cost_to_cache) are queued onto a Redis
        pipeline and flushed in a single round trip after the loop, instead
        of being awaited one at a time per event.

    ASSUMPTION FLAGGED: CostService.enqueue_event still receives the raw
    redis_client and is still awaited per event below (its internals live
    in services.py, which wasn't available to me). If it does its own
    per-event SETNX/XADD round-trips, that's the next thing to pipeline —
    share services.py to close that out. Also assumed CostService no longer
    needs a DB session per the docstring above ("no longer writes to
    PostgreSQL directly"); if that's wrong, CostService will need its own
    explicitly-scoped session rather than reusing the rate_db one below,
    which is closed before the loop runs.
    """
    from ..sdk_api.pricing import infer_provider

    project_id = request.state.project_id
    user_id = request.state.user_id

    prepared = []
    pairs: set[tuple[str, str]] = set()
    for ev in events.events:
        provider = infer_provider(ev.model) or ev.provider or "unknown"
        pairs.add((provider, ev.model))
        prepared.append((ev, provider))

    # Short-lived session: alive only for this one batched query, then
    # released back to the pool before any Redis work starts.
    async with AsyncSessionLocal() as rate_db:
        rates_by_pair = await _fetch_db_rates_batch(rate_db, pairs, user_id)

    cost_svc = CostService(None)  # Redis-only path — see docstring note above

    # Phase 0: pure computation (cost calc + payload prep), no Redis I/O.
    # Kept as its own pass so the Redis work below can be batched instead
    # of interleaved one-event-at-a-time with this loop.
    costed: list[tuple] = []  # (ev, cost_usd) pairs, same order as `prepared`
    for ev, provider in prepared:
        db_rates = rates_by_pair.get((provider, ev.model))
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
        costed.append((ev, breakdown.total_cost))

    # Phase 1-3 (see CostService.enqueue_events_batch docstring): reserve
    # dedup keys for the whole batch, XADD concurrently, close out TTL
    # bookkeeping — all in ~3 Redis round trips total instead of up to
    # 3*len(events) sequential ones. This replaces the old per-event
    # `await cost_svc.enqueue_event(...)` loop, which was the actual
    # throughput bottleneck under load (each event cost up to 3 sequential
    # network round trips: SETNX, XADD, EXPIRE).
    results = await cost_svc.enqueue_events_batch(redis_client, project_id, costed)

    # Statuses that mean the event was actually queued durably (or already
    # was, on a prior attempt) — only these should sync the live cost
    # cache. Using an explicit allow-list here (rather than the previous
    # `!= "duplicate_ignored"` deny-list) fixes a real bug: a rejected
    # event (e.g. missing event_id, or a failed XADD) was previously still
    # falling through to queue_apply_cost_to_cache, bumping the tenant's
    # live budget cache for an event that was never durably recorded —
    # a cache/ledger inconsistency that only gets worse the more ingest
    # traffic comes from old SDKs or under Redis memory pressure.
    QUEUED_STATUSES = {"accepted"}

    cache_pipe = redis_client.pipeline(transaction=False)
    pending_cache_writes = 0
    counter = 0

    for (ev, cost_usd), result in zip(costed, results):
        tenant_id = ev.tenant_id

        # Keep any already-warm cost cache entries for this tenant/project in
        # sync, rather than invalidating them. This only updates the
        # cache so check-tenant-limit doesn't have to wait out the TTL or
        # hit the DB to see this event. Never creates a cache key (see
        # apply_cost_to_cache). Since Redis is updated synchronously here
        # (ahead of the worker's eventual Postgres insert), live budget
        # enforcement via check-tenant-limit stays correct even if the
        # worker falls behind — Redis is the source of truth for limits,
        # Postgres is the historical ledger.
        #
        # Skipped for duplicate_ignored (the original attempt already
        # incremented this cache — a retry must not double-count it) and
        # for anything rejected (never durably queued, so nothing to
        # reflect in the live cache yet).
        #
        # Queued onto cache_pipe rather than awaited immediately — the
        # whole batch's cache-sync writes get flushed in one round trip
        # after the loop instead of up to 2*len(events) sequential ones.
        if result["status"] in QUEUED_STATUSES:
            queue_apply_cost_to_cache(cache_pipe, tenant_id, project_id, cost_usd)
            pending_cache_writes += 1

        counter += 1

    if pending_cache_writes:
        try:
            await cache_pipe.execute()
        except Exception:
            # Same soft-fail contract as the old per-call try/except inside
            # _increment_cost_cache_if_exists: cache is a perf optimization,
            # never a reason to fail ingestion.
            logger.warning(
                "failed to flush cost cache pipeline for project_id=%s",
                project_id,
                exc_info=True,
            )

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

    Kept for any caller that wants an immediate, non-batched write (e.g.
    a single event outside of /ingest's loop). For batches, use
    queue_apply_cost_to_cache below instead — awaiting this per event is
    exactly the sequential-round-trip pattern that made /ingest slow under
    load.
    """
    daily_key = f"cost:daily:{tenant_id}:{project_id}"
    monthly_key = f"cost:monthly:{tenant_id}:{project_id}"

    await _increment_cost_cache_if_exists(redis_client, daily_key, cost_usd)
    await _increment_cost_cache_if_exists(redis_client, monthly_key, cost_usd)


def queue_apply_cost_to_cache(
    pipe,
    tenant_id: str,
    project_id: str,
    cost_usd: float,
    ttl: int = COST_CACHE_TTL,
) -> None:
    """
    Pipelined sibling of apply_cost_to_cache: queues the daily + monthly
    increment EVALs onto `pipe` (a redis pipeline, e.g. from
    redis_client.pipeline(transaction=False)) instead of awaiting them.

    Not a coroutine — redis-py's pipeline command methods just append to
    the pipeline's internal command stack synchronously; nothing is sent
    to Redis until the caller does `await pipe.execute()`. Call this once
    per event in a batch, then execute() once after the loop — that's what
    collapses up to 2*N sequential round trips into one.

    Same semantics as apply_cost_to_cache/_increment_cost_cache_if_exists:
    never creates a key, only bumps ones that already exist. See
    _INCR_IF_EXISTS_LUA for why the existence-check + INCRBYFLOAT + EXPIRE
    has to be a single atomic script rather than separate calls.
    """
    daily_key = f"cost:daily:{tenant_id}:{project_id}"
    monthly_key = f"cost:monthly:{tenant_id}:{project_id}"

    pipe.eval(_INCR_IF_EXISTS_LUA, 1, daily_key, str(cost_usd), str(ttl))
    pipe.eval(_INCR_IF_EXISTS_LUA, 1, monthly_key, str(cost_usd), str(ttl))


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
            Threshold.tenant_id == tenant_id,
            Threshold.frequency == Threshold.PER_DAY,
            Threshold.project_id == project_id,
        )
    )
    daily_threshold = user_daily_threshold_result.scalar_one_or_none()
    if daily_threshold is not None:
        thresholds.append(daily_threshold.to_dict())

    # monthly
    user_monthly_threshold_result = await db.execute(
        select(Threshold).where(
            Threshold.tenant_id == tenant_id,
            Threshold.frequency == Threshold.PER_MONTH,
            Threshold.project_id == project_id,
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


@router.get(
    "/check-tenant-limit",
    status_code=200,
    tags=["check tenant limit"],
    dependencies=[Depends(check_plan_limits)],
)
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
            cost_usd = await get_today_total_cost_cached(
                db, redis_client, tenant_id, project_id
            )
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


@router.post(
    "/thresholds",
    status_code=200,
    tags=["thresholds"],
    dependencies=[Depends(check_plan_limits)],
)
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
