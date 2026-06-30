# ===========================================================================
# REDIS STREAMS CONTRACT (shared by the ingest API and the ingestion worker)
# ===========================================================================
#
# Single source of truth for stream name, consumer group name, and the field
# layout written by XADD / read by XREADGROUP. Keeping this in one module
# means the producer (FastAPI /ingest) and the consumer (worker process)
# can never silently drift out of sync on field names.

import json
import logging

import redis.asyncio as redis

logger = logging.getLogger(__name__)

COST_EVENTS_STREAM = "stream:cost_events"
COST_EVENTS_GROUP = "cost_events_workers"

# Cap the stream itself so a stalled/dead worker can't grow it unbounded.
# This is an approximate trim (~ prefix), which is cheap and non-blocking.
STREAM_MAX_LEN_APPROX = 1_000_000


async def ensure_consumer_group(redis_client: redis.Redis) -> None:
    """
    Create the consumer group if it doesn't exist yet, and create the
    stream itself if needed (mkstream=True). Idempotent — safe to call
    on every worker startup.
    """
    try:
        await redis_client.xgroup_create(
            name=COST_EVENTS_STREAM,
            groupname=COST_EVENTS_GROUP,
            id="0",
            mkstream=True,
        )
        logger.info(
            "created consumer group=%s on stream=%s",
            COST_EVENTS_GROUP,
            COST_EVENTS_STREAM,
        )
    except redis.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            # Group already exists — expected on every restart after the first.
            logger.debug(
                "consumer group=%s already exists on stream=%s",
                COST_EVENTS_GROUP,
                COST_EVENTS_STREAM,
            )
        else:
            raise


class StreamEnqueueError(Exception):
    """
    Raised when XADD fails to durably queue a cost event (e.g. Redis is
    at maxmemory with a noeviction policy and rejects the write with
    OOM, or any other connection/command failure).

    This is deliberately a distinct, narrow exception type rather than
    letting redis.ResponseError/ConnectionError propagate raw, so that
    callers (services.py's enqueue_event) can catch *specifically*
    "the durable write didn't happen" and react correctly — most
    importantly, not commit the dedup guard for an event that was
    never actually queued. Wraps the original exception as __cause__
    so the underlying Redis error is still visible in logs/tracebacks.
    """


async def enqueue_cost_event(
    redis_client: redis.Redis,
    event_id: str,
    project_id: str,
    event_payload: dict,
    cost_usd: float,
) -> str:
    """
    XADD a single cost event onto the durable write queue.

    The full event is JSON-encoded into one field ("data") rather than
    spread across many stream fields, since the event schema already
    has ~20 optional fields (audio/image/text token variants) and Redis
    stream fields are flat strings only. event_id/project_id are also
    kept as their own top-level fields so the worker (or anyone running
    XRANGE for debugging) doesn't have to deserialize JSON just to see
    those two.

    cost_usd is serialized with str(), not repr(). repr() is meant for
    unambiguous round-tripping back to a Python reader (e.g. repr() of
    a Decimal includes the class name: "Decimal('0.0023')"), not for
    producing a clean numeric string for the worker's float() parse on
    the other end. A plain float happens to repr()/str() the same way,
    which is exactly what made this easy to miss — but if cost_usd is
    ever a Decimal (plausible for money math), repr() breaks the
    contract silently.

    Raises StreamEnqueueError if the XADD itself fails (e.g. Redis OOM
    under maxmemory+noeviction, or a connection error) — this is NOT
    swallowed here, since the caller needs to know the write didn't
    happen in order to avoid committing dedup state for it.

    Returns the stream message ID assigned by Redis (not the same as
    event_id — this is the XADD-assigned "<ms>-<seq>" id).
    """
    fields = {
        "event_id": event_id,
        "project_id": project_id,
        "cost_usd": str(cost_usd),
        "data": json.dumps(event_payload, default=str),
    }
    try:
        message_id = await redis_client.xadd(
            name=COST_EVENTS_STREAM,
            fields=fields,
            maxlen=STREAM_MAX_LEN_APPROX,
            approximate=True,
        )
    except redis.RedisError as exc:
        logger.error(
            "XADD failed for event_id=%s project_id=%s stream=%s: %s",
            event_id,
            project_id,
            COST_EVENTS_STREAM,
            exc,
        )
        raise StreamEnqueueError(
            f"failed to enqueue event_id={event_id} project_id={project_id} "
            f"onto stream={COST_EVENTS_STREAM}"
        ) from exc
    return message_id
