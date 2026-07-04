# ===========================================================================
# COST EVENT INGESTION WORKER
# ===========================================================================
#
# Standalone, long-running process. Reads cost events off the Redis Stream
# durable write queue (written by the /ingest endpoint via XADD) using a
# consumer group, bulk-inserts each batch into PostgreSQL within a single
# transaction, and XACKs only after a successful commit.
#
# Run as its own Docker Compose service (see docker-compose.yml: `worker`).
# Designed to run with N replicas sharing one consumer group — each replica
# uses a distinct CONSUMER_NAME (defaults to hostname, i.e. the container
# id, which is unique per replica under Compose/Swarm/K8s).
#
# Failure model:
#   - PostgreSQL insert/commit fails -> exception propagates, batch is NOT
#     acked. The unacked messages stay in this consumer's Pending Entries
#     List (PEL). On the next loop iteration, the worker explicitly
#     re-reads its own pending entries (XREADGROUP ... streams={stream: "0"})
#     before pulling new ones, so the same batch is retried as long as
#     this process stays alive.
#   - Redis becomes unreachable -> exception propagates to the top-level
#     loop, which logs and backs off before retrying. No data is lost
#     since nothing was acked.
#   - This worker process itself dies mid-batch -> messages remain in its
#     PEL. On startup, XAUTOCLAIM sweeps entries idle longer than
#     CLAIM_IDLE_MS and reassigns them to whichever consumer starts next,
#     so they aren't stuck forever under a dead consumer name.

import asyncio
import logging
import os
import signal
import time
from contextlib import suppress

import redis.asyncio as redis
from sqlalchemy.exc import DataError, IntegrityError

from backend.database.database import AsyncSessionLocal
from backend.dashboard.services import CostService
from backend.dashboard.streams import (
    COST_EVENTS_GROUP,
    COST_EVENTS_STREAM,
    ensure_consumer_group,
)
from backend.sdk_api.schemas import CostEventIn

# Postgres errors that mean "this row can never succeed, no matter how many
# times it's retried" — malformed UUIDs, bad enum values, NOT NULL/FK/unique
# violations, out-of-range numerics, etc. These are permanent and must be
# dropped (dead-lettered via logging) rather than retried forever. Anything
# else (connection drops, timeouts, deadlocks/serialization failures) is
# treated as transient and keeps the existing retry-forever behavior.
PERMANENT_DB_ERRORS = (IntegrityError, DataError)

logger = logging.getLogger("cost_event_worker")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

# ── configuration (env-overridable; sane defaults for moderate traffic) ──
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/10")
CONSUMER_NAME = os.getenv("WORKER_CONSUMER_NAME") or os.uname().nodename
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "100"))
BLOCK_MS = int(os.getenv("WORKER_BLOCK_MS", "5000"))  # XREADGROUP block timeout
CLAIM_IDLE_MS = int(
    os.getenv("WORKER_CLAIM_IDLE_MS", str(5 * 60 * 1000))
)  # reclaim entries pending >5min from dead consumers
ERROR_BACKOFF_SECONDS = float(os.getenv("WORKER_ERROR_BACKOFF_SECONDS", "2.0"))
MAX_ERROR_BACKOFF_SECONDS = float(os.getenv("WORKER_MAX_ERROR_BACKOFF_SECONDS", "30.0"))


def _row_from_stream_fields(message_id: str, fields: dict) -> dict | None:
    """
    Parse one stream message's fields back into a cost_events insert row.
    Returns None (and logs) if the payload is malformed, so one corrupt
    message can't wedge the whole batch — it's acked and dropped rather
    than retried forever. This is deliberate: a poison message that fails
    to parse will never succeed no matter how many times it's redelivered.

    created_at is derived from the stream message id's millisecond
    timestamp component ("<ms>-<seq>"), i.e. the time the event was
    XADD'd by /ingest — not the time the worker happens to process it,
    which could lag behind under backlog.
    """
    import json
    from datetime import datetime, timezone

    try:
        data = json.loads(fields["data"])
        cost_usd = float(fields["cost_usd"])
    except (KeyError, ValueError, TypeError) as exc:
        logger.error(
            "dropping unparseable stream message id=%s event_id=%s error=%s",
            message_id,
            fields.get("event_id", "?"),
            exc,
        )
        return None

    try:
        event = CostEventIn(
            **{k: v for k, v in data.items() if k not in ("id", "project_id")}
        )
    except Exception as exc:
        logger.error(
            "dropping invalid cost event payload event_id=%s error=%s",
            data.get("id", "?"),
            exc,
        )
        return None

    ms_part = message_id.split("-")[0]
    created_at = datetime.fromtimestamp(int(ms_part) / 1000, tz=timezone.utc)

    return {
        "id": data["id"],
        "project_id": data["project_id"],
        "cost_usd": cost_usd,
        "created_at": created_at,
        **event.model_dump(),
    }


async def _claim_stale_pending(redis_client: redis.Redis) -> None:
    """
    On startup, sweep for messages that were claimed by a now-dead
    consumer (crashed mid-batch, never acked) and reassign them to this
    consumer so they aren't stuck forever. Safe to run on every startup —
    XAUTOCLAIM is idempotent and only touches entries idle longer than
    CLAIM_IDLE_MS.

    NOTE: redis-py's XAUTOCLAIM return shape has changed across versions
    (2-tuple of (cursor, claimed) in some versions, 3-tuple adding a list
    of deleted/trimmed message ids in others). Rather than assume a fixed
    arity — which would crash the worker at startup on a version mismatch
    — this unpacks defensively and only relies on the first two elements,
    which have been stable across versions.
    """
    cursor = "0-0"
    claimed_total = 0
    while True:
        result = await redis_client.xautoclaim(
            name=COST_EVENTS_STREAM,
            groupname=COST_EVENTS_GROUP,
            consumername=CONSUMER_NAME,
            min_idle_time=CLAIM_IDLE_MS,
            start_id=cursor,
            count=BATCH_SIZE,
        )
        # First two elements (cursor, claimed) are consistent across the
        # redis-py versions that support XAUTOCLAIM at all; anything past
        # that (e.g. a third "deleted" element) is ignored on purpose.
        cursor, claimed = result[0], result[1]
        claimed_total += len(claimed)
        if cursor == "0-0" or not claimed:
            break
    if claimed_total:
        logger.info(
            "reclaimed stale pending messages count=%d consumer=%s",
            claimed_total,
            CONSUMER_NAME,
        )


async def _insert_one(row: dict) -> None:
    """Insert+commit a single row in its own session/transaction. Used as
    the row-by-row fallback when a batch insert fails with a permanent DB
    error, so we can isolate exactly which row(s) are poisoned."""
    async with AsyncSessionLocal() as session:
        cost_svc = CostService(session)
        await cost_svc.bulk_insert_events([row])
        await session.commit()


async def _process_batch(
    redis_client: redis.Redis, messages: list[tuple[str, dict]]
) -> None:
    """
    Parse, bulk-insert, commit once, then XACK the whole batch.

    On a *transient* DB failure (connection drop, timeout, deadlock/
    serialization failure), the exception propagates without acking.
    Those messages stay in the consumer group's Pending Entries List
    (PEL) under this consumer's name and are retried on the next loop
    iteration, exactly as before.

    On a *permanent* DB failure (IntegrityError/DataError — malformed
    UUID, bad enum, constraint violation, etc.), the batch can never
    succeed as a whole no matter how many times it's retried. Rather
    than wedge every message behind it forever, we fall back to
    inserting rows one at a time: rows that succeed individually are
    acked and committed; rows that fail with the same permanent error
    are logged and dropped (acked without being inserted, i.e. dead-
    lettered via logs); rows that fail with something transient are
    left unacked so they're retried normally. If this consumer dies
    before acking, _claim_stale_pending reassigns any still-pending
    entries to whichever consumer starts next, once they've been idle
    past CLAIM_IDLE_MS.
    """
    t_parse_start = time.monotonic()
    parsed: list[tuple[str, dict]] = []  # (message_id, row)
    message_ids = []
    for message_id, fields in messages:
        message_ids.append(message_id)
        row = _row_from_stream_fields(message_id, fields)
        if row is not None:
            parsed.append((message_id, row))
    parse_ms = (time.monotonic() - t_parse_start) * 1000

    if not parsed:
        # Every message in the batch was a poison/unparseable payload —
        # ack them anyway so the queue doesn't stall on permanently
        # invalid data.
        if message_ids:
            await redis_client.xack(COST_EVENTS_STREAM, COST_EVENTS_GROUP, *message_ids)
        return

    rows = [row for _, row in parsed]

    t_insert_start = time.monotonic()
    try:
        async with AsyncSessionLocal() as session:
            cost_svc = CostService(session)
            inserted = await cost_svc.bulk_insert_events(rows)
            await session.commit()
    except PERMANENT_DB_ERRORS:
        # The batch as a whole is poisoned by at least one bad row.
        # Isolate the culprit(s) with a row-by-row fallback instead of
        # leaving the entire batch (including good rows) unacked forever.
        logger.warning(
            "batch insert hit a permanent DB error, falling back to "
            "row-by-row insert to isolate poison rows batch_size=%d",
            len(rows),
        )
        inserted = 0
        # Messages that never parsed into a row at all were already
        # permanently dropped above (poison JSON/schema) — ack them here
        # too, since they were never part of `parsed` and would otherwise
        # never get acked.
        parsed_ids = {mid for mid, _ in parsed}
        ack_ids: list[str] = [mid for mid in message_ids if mid not in parsed_ids]
        transient_failures = 0
        for message_id, row in parsed:
            try:
                await _insert_one(row)
                ack_ids.append(message_id)
                inserted += 1
            except PERMANENT_DB_ERRORS as row_exc:
                logger.error(
                    "dropping poison row: permanent DB error, will never "
                    "succeed on retry message_id=%s event_id=%s error=%s",
                    message_id,
                    row.get("id", "?"),
                    row_exc,
                )
                ack_ids.append(message_id)  # ack = drop, do not retry
            except Exception as row_exc:
                logger.error(
                    "row insert failed with transient error, leaving "
                    "unacked for retry message_id=%s event_id=%s error=%s",
                    message_id,
                    row.get("id", "?"),
                    row_exc,
                    exc_info=True,
                )
                transient_failures += 1
                # deliberately not added to ack_ids -> stays in PEL

        if ack_ids:
            await redis_client.xack(COST_EVENTS_STREAM, COST_EVENTS_GROUP, *ack_ids)

        insert_ms = (time.monotonic() - t_insert_start) * 1000
        logger.info(
            "row-by-row fallback complete batch_size=%d inserted=%d "
            "dropped=%d transient=%d insert_latency_ms=%.1f",
            len(rows),
            inserted,
            len(ack_ids) - inserted,
            transient_failures,
            insert_ms,
        )

        if transient_failures:
            # At least one row failed for a non-permanent reason and is
            # still unacked. Raise so the outer loop marks it for retry
            # (the successful/dropped rows are already acked above, so
            # only the genuinely-retryable ones will be re-read next time).
            raise
        return
    except Exception:
        insert_ms = (time.monotonic() - t_insert_start) * 1000
        logger.error(
            "batch insert failed with a transient error, leaving %d "
            "messages unacked for retry batch_size=%d insert_latency_ms=%.1f",
            len(message_ids),
            len(messages),
            insert_ms,
            exc_info=True,
        )
        raise

    insert_ms = (time.monotonic() - t_insert_start) * 1000

    # Only ack after a successful commit.
    await redis_client.xack(COST_EVENTS_STREAM, COST_EVENTS_GROUP, *message_ids)

    logger.info(
        "batch committed batch_size=%d inserted=%d parse_latency_ms=%.1f "
        "insert_latency_ms=%.1f",
        len(messages),
        inserted,
        parse_ms,
        insert_ms,
    )


async def run_worker(stop_event: asyncio.Event) -> None:
    redis_client = redis.from_url(
        REDIS_URL, decode_responses=True, socket_timeout=10, socket_connect_timeout=2
    )

    # Startup can race with Redis still coming up despite the Compose
    # healthcheck dependency (e.g. healthy-but-not-yet-accepting-writes
    # during AOF load on restart). Retry with backoff here rather than
    # letting an unhandled exception crash the process before the
    # try/finally below — under `restart: always` that would crash-loop
    # tightly instead of waiting Redis out.
    startup_backoff = ERROR_BACKOFF_SECONDS
    while not stop_event.is_set():
        try:
            await ensure_consumer_group(redis_client)
            await _claim_stale_pending(redis_client)
            break
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error(
                "worker startup failed (Redis unreachable or not ready), "
                "retrying in %.1fs",
                startup_backoff,
                exc_info=True,
            )
            await asyncio.sleep(startup_backoff)
            startup_backoff = min(startup_backoff * 2, MAX_ERROR_BACKOFF_SECONDS)
    else:
        # stop_event was set while still retrying startup — shut down
        # cleanly instead of proceeding into the main loop.
        with suppress(Exception):
            await redis_client.aclose()
        return

    logger.info(
        "worker started consumer=%s stream=%s group=%s batch_size=%d block_ms=%d",
        CONSUMER_NAME,
        COST_EVENTS_STREAM,
        COST_EVENTS_GROUP,
        BATCH_SIZE,
        BLOCK_MS,
    )

    backoff = ERROR_BACKOFF_SECONDS
    has_unacked_from_last_failure = False

    try:
        while not stop_event.is_set():
            try:
                # Queue depth — logged each poll so it's visible in metrics
                # scraping logs even without a separate metrics exporter.
                try:
                    depth = await redis_client.xlen(COST_EVENTS_STREAM)
                    pending = await redis_client.xpending(
                        COST_EVENTS_STREAM, COST_EVENTS_GROUP
                    )
                    pending_count = pending["pending"] if pending else 0
                    logger.debug(
                        "queue depth stream_len=%d pending=%d", depth, pending_count
                    )
                except Exception:
                    logger.warning("failed to read queue depth metrics", exc_info=True)

                if has_unacked_from_last_failure:
                    # The previous batch failed to commit and is still
                    # sitting unacked in this consumer's PEL. Retry it
                    # explicitly before pulling any new ('>') messages —
                    # '>' never re-surfaces a live consumer's own pending
                    # entries, so without this they'd sit untouched until
                    # this process restarts and XAUTOCLAIM sweeps them.
                    retry_response = await redis_client.xreadgroup(
                        groupname=COST_EVENTS_GROUP,
                        consumername=CONSUMER_NAME,
                        streams={COST_EVENTS_STREAM: "0"},
                        count=BATCH_SIZE,
                    )
                    retry_messages = retry_response[0][1] if retry_response else []
                    if retry_messages:
                        logger.info(
                            "retrying %d previously unacked messages for consumer=%s",
                            len(retry_messages),
                            CONSUMER_NAME,
                        )
                        try:
                            await _process_batch(redis_client, retry_messages)
                        except Exception:
                            # Still failing — leave the flag set so the
                            # next iteration retries again, instead of
                            # falling through to clearing it below.
                            raise
                    has_unacked_from_last_failure = False
                    backoff = ERROR_BACKOFF_SECONDS
                    continue

                response = await redis_client.xreadgroup(
                    groupname=COST_EVENTS_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={COST_EVENTS_STREAM: ">"},
                    count=BATCH_SIZE,
                    block=10,
                )

                if not response:
                    # Timed out waiting for new messages — loop back around
                    # so stop_event gets checked regularly even when idle.
                    backoff = ERROR_BACKOFF_SECONDS
                    continue

                # response is [(stream_name, [(message_id, fields), ...])]
                _, messages = response[0]
                try:
                    await _process_batch(redis_client, messages)
                except Exception:
                    # Only a failure *inside* batch processing leaves
                    # messages genuinely unacked in this consumer's PEL —
                    # that's the one case worth an explicit self-retry
                    # next iteration. Re-raise so the outer handler still
                    # logs + backs off; the flag is set here, not there,
                    # so unrelated failures (e.g. the queue-depth logging
                    # block, or xreadgroup itself failing before any
                    # messages were read) don't trigger a pointless retry
                    # read against an empty PEL.
                    has_unacked_from_last_failure = True
                    raise
                backoff = ERROR_BACKOFF_SECONDS  # reset after a clean batch

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(
                    "worker loop error, backing off %.1fs before retry",
                    backoff,
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_ERROR_BACKOFF_SECONDS)
    finally:
        logger.info("worker shutting down, closing redis connection")
        with suppress(Exception):
            await redis_client.aclose()


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig_name: str) -> None:
        logger.info("received signal=%s, requesting graceful shutdown", sig_name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown, sig.name)

    await run_worker(stop_event)


if __name__ == "__main__":
    asyncio.run(main())
