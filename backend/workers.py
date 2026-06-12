"""
workers.py — Layer 3: Background Workers

Uses APScheduler (AsyncIOScheduler) running inside the FastAPI process.
Three jobs:
  • every 1 min  — check cost thresholds, fire alerts if breached
  • every 1 hour — rollup raw cost_events → customer_cost_daily
  • every 1 day  — placeholder for future model-pricing sync

Dependencies:
    pip install apscheduler

Upgrade path:
    When you outgrow in-process scheduling, replace each job function body
    with a Celery task and point the scheduler at a Redis broker instead.
"""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .database.database import AsyncSessionLocal
from .dashboard.services import AlertService, CostService

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="UTC")


# ---------------------------------------------------------------------------
# Job: check thresholds (every 1 minute)
# ---------------------------------------------------------------------------


async def job_check_thresholds() -> None:
    """
    Runs every minute.
    Scans all (project, customer) pairs against their configured thresholds.
    Inserts an alert row and fires email + webhook for any new breach.
    Skips customers that already received an alert in the last hour.
    """
    logger.info("[worker] job_check_thresholds: starting")
    try:
        async with AsyncSessionLocal() as session:
            alert_svc = AlertService(session)
            fired = await alert_svc.check_thresholds()  # checks ALL projects
            await session.commit()

        if fired:
            logger.warning(
                "[worker] job_check_thresholds: %d new alert(s) fired — %s",
                len(fired),
                [f"{a['tenant_id']}@{a['project_id']}" for a in fired],
            )
        else:
            logger.debug("[worker] job_check_thresholds: no breaches found")

    except Exception:
        logger.exception("[worker] job_check_thresholds: unhandled error")


# ---------------------------------------------------------------------------
# Job: rollup daily costs (every 1 hour)
# ---------------------------------------------------------------------------


async def job_rollup_daily() -> None:
    """
    Runs every hour.
    Aggregates yesterday's raw cost_events rows into customer_cost_daily
    using an UPSERT so re-runs are idempotent.
    """
    logger.info("[worker] job_rollup_daily: starting")
    try:
        async with AsyncSessionLocal() as session:
            cost_svc = CostService(session)
            upserted = await cost_svc.rollup_daily()  # rolls up ALL projects
            await session.commit()

        logger.info("[worker] job_rollup_daily: %d row(s) upserted", upserted)

    except Exception:
        logger.exception("[worker] job_rollup_daily: unhandled error")


# ---------------------------------------------------------------------------
# Job: sync model pricing (every 1 day — stubbed for future use)
# ---------------------------------------------------------------------------


async def job_sync_model_pricing() -> None:
    """
    Runs once a day at 02:00 UTC.
    Placeholder: fetch the latest per-token prices from your pricing source
    (OpenAI API, a config file, or a CMS) and write them into a
    `model_pricing` table so cost calculations stay current.

    To activate:
        1. Create a `model_pricing` table with (model, input_usd, output_usd, updated_at).
        2. Replace the stub below with a real fetch + upsert.
    """
    logger.info("[worker] job_sync_model_pricing: stub — implement pricing sync here")

    # Example skeleton:
    # async with httpx.AsyncClient() as client:
    #     resp = await client.get("https://your-pricing-source/models.json")
    #     pricing = resp.json()
    #
    # async with AsyncSessionLocal() as session:
    #     for entry in pricing:
    #         await session.execute(text("""
    #             INSERT INTO model_pricing (model, input_usd, output_usd, updated_at)
    #             VALUES (:model, :input_usd, :output_usd, NOW())
    #             ON CONFLICT (model) DO UPDATE
    #                 SET input_usd  = EXCLUDED.input_usd,
    #                     output_usd = EXCLUDED.output_usd,
    #                     updated_at = NOW()
    #         """), entry)
    #     await session.commit()


# ---------------------------------------------------------------------------
# Scheduler bootstrap — called from main.py startup
# ---------------------------------------------------------------------------


def start_scheduler() -> None:
    """
    Register all jobs and start the scheduler.
    Called once from FastAPI's startup event in main.py.
    """
    # 1-minute threshold checks
    scheduler.add_job(
        job_check_thresholds,
        trigger=IntervalTrigger(minutes=1),
        id="check_thresholds",
        name="Check cost thresholds",
        max_instances=1,  # never overlap if the previous run is still going
        replace_existing=True,
        misfire_grace_time=30,  # seconds before a missed firing is skipped
    )

    # Hourly rollup
    scheduler.add_job(
        job_rollup_daily,
        trigger=IntervalTrigger(minutes=1),
        id="rollup_daily",
        name="Rollup raw events to daily table",
        max_instances=1,
        replace_existing=True,
        misfire_grace_time=300,
    )

    # Daily pricing sync at 02:00 UTC
    scheduler.add_job(
        job_sync_model_pricing,
        trigger=CronTrigger(hour=2, minute=0, timezone="UTC"),
        id="sync_model_pricing",
        name="Sync model pricing table",
        max_instances=1,
        replace_existing=True,
        misfire_grace_time=3600,
    )

    scheduler.start()
    logger.info(
        "[scheduler] started with jobs: %s",
        [job.id for job in scheduler.get_jobs()],
    )


def stop_scheduler() -> None:
    """Gracefully shut down — call from FastAPI shutdown event if needed."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("[scheduler] stopped")
