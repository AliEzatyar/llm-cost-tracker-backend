# ===========================================================================
# SERVICE LAYER
# ===========================================================================


from datetime import date, datetime, timedelta, timezone
import hashlib
import hmac
import uuid
import httpx
from sqlalchemy import text
from backend.database.models import get_project_by_id
from backend.configs import (
    ALERT_FROM_EMAIL,
    ALERT_WEBHOOK_URL,
    SMTP_HOST,
    SMTP_PASS,
    SMTP_PORT,
    SMTP_USER,
)
from ..sdk_api.schemas import (
    CostEventIn,
)
from sqlalchemy.ext.asyncio import AsyncSession
import logging

logger = logging.getLogger(__name__)


class CostService:
    """Handles raw event writes and cost queries."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def write_event(
        self, project_id: str, event: CostEventIn, cost_usd: float
    ) -> dict:
        event_id = str(uuid.uuid4())
        await self.session.execute(
            text("""
                INSERT INTO cost_events (
                    id, project_id, tenant_id,
                    provider, endpoint, model,
                    input_tokens, cached_tokens, output_tokens, total_tokens,
                    audio_input_tokens, audio_output_tokens,
                    duration_seconds,
                    character_count,
                    input_text_tokens, cached_input_text_tokens,
                    input_image_tokens, cached_input_image_tokens,
                    output_image_tokens,
                    image_count,
                    cost_usd, created_at
                ) VALUES (
                    :id, :project_id, :tenant_id,
                    :provider, :endpoint, :model,
                    :input_tokens, :cached_tokens, :output_tokens, :total_tokens,
                    :audio_input_tokens, :audio_output_tokens,
                    :duration_seconds,
                    :character_count,
                    :input_text_tokens, :cached_input_text_tokens,
                    :input_image_tokens, :cached_input_image_tokens,
                    :output_image_tokens,
                    :image_count,
                    :cost_usd, NOW()
                )
            """),
            {
                "id": event_id,
                "project_id": project_id,
                "cost_usd": cost_usd,
                **event.model_dump(),
            },
        )
        return {"id": event_id, "status": "accepted"}

    async def get_by_tenant(
        self,
        project_id: str,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[dict]:
        """
        Daily rollup for all tenants in a project.

        Returns one row per (tenant_id, date) with:
          • cost and call count from the pre-aggregated customer_cost_daily table
          • token breakdown summed live from cost_events, split by modality:
              text tokens  — input / cached / output / total
              audio tokens — audio_input / audio_output / duration_seconds
              speech       — character_count
              image tokens — input_text / cached_input_text /
                             input_image / cached_input_image / output_image
              image audit  — image_count
          • the tenant's configured spend limit from thresholds (if set)
        """
        filters = ["ccd.project_id = :project_id"]
        params: dict = {"project_id": project_id}

        if date_from:
            filters.append("ccd.date >= :date_from")
            params["date_from"] = date_from
        if date_to:
            filters.append("ccd.date <= :date_to")
            params["date_to"] = date_to

        where = " AND ".join(filters)
        result = await self.session.execute(
            text(f"""
                SELECT
                    ccd.tenant_id,
                    ccd.date,
                    ccd.total_cost_usd,
                    ccd.total_calls,
 
                    -- text tokens
                    COALESCE(SUM(ce.input_tokens),  0)                          AS total_input_tokens,
                    COALESCE(SUM(ce.cached_tokens), 0)                          AS total_cached_tokens,
                    COALESCE(SUM(ce.output_tokens), 0)                          AS total_output_tokens,
                    COALESCE(SUM(ce.total_tokens),  0)                          AS total_tokens,
 
                    -- audio tokens (gpt-4o-transcribe token mode, realtime)
                    COALESCE(SUM(ce.audio_input_tokens),  0)                    AS total_audio_input_tokens,
                    COALESCE(SUM(ce.audio_output_tokens), 0)                    AS total_audio_output_tokens,
 
                    -- audio flat billing (whisper-1)
                    COALESCE(SUM(ce.duration_seconds), 0)                       AS total_duration_seconds,
 
                    -- speech flat billing (tts-1)
                    COALESCE(SUM(ce.character_count), 0)                        AS total_character_count,
 
                    -- image tokens (gpt-image-*)
                    COALESCE(SUM(ce.input_text_tokens),         0)              AS total_input_text_tokens,
                    COALESCE(SUM(ce.cached_input_text_tokens),  0)              AS total_cached_input_text_tokens,
                    COALESCE(SUM(ce.input_image_tokens),        0)              AS total_input_image_tokens,
                    COALESCE(SUM(ce.cached_input_image_tokens), 0)              AS total_cached_input_image_tokens,
                    COALESCE(SUM(ce.output_image_tokens),       0)              AS total_output_image_tokens,
 
                    -- image audit
                    COALESCE(SUM(ce.image_count), 0)                            AS total_image_count,
 
                    -- spend limit
                    th.limit_usd
                FROM   customer_cost_daily ccd
                LEFT JOIN cost_events ce
                    ON  ce.project_id    = ccd.project_id
                    AND ce.tenant_id     = ccd.tenant_id
                    AND ce.created_at::date = ccd.date
                LEFT JOIN thresholds th
                    ON  th.project_id = ccd.project_id
                    AND th.tenant_id  = ccd.tenant_id
                WHERE  {where}
                GROUP  BY
                    ccd.tenant_id, ccd.date,
                    ccd.total_cost_usd, ccd.total_calls,
                    th.limit_usd
                ORDER  BY ccd.date DESC, ccd.total_cost_usd DESC
            """),
            params,
        )
        return [dict(r) for r in result.mappings()]

    async def get_customer_breakdown(
        self,
        project_id: str,
        tenant_id: str,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> dict:
        """
        Per-model breakdown for a single tenant from raw events.

        Each row in ``breakdown`` represents one model used by the tenant and
        carries the full token breakdown across all billing modalities, so the
        B2B owner can see exactly what drove cost for each model.
        """
        filters = [
            "project_id = :project_id",
            "tenant_id  = :tenant_id",
        ]
        params: dict = {"project_id": project_id, "tenant_id": tenant_id}

        if date_from:
            filters.append("created_at >= :date_from")
            params["date_from"] = datetime.combine(date_from, datetime.min.time())
        if date_to:
            filters.append("created_at < :date_to")
            params["date_to"] = datetime.combine(
                date_to + timedelta(days=1), datetime.min.time()
            )

        where = " AND ".join(filters)
        result = await self.session.execute(
            text(f"""
                SELECT
                    model,
                    endpoint,
 
                    -- text tokens
                    SUM(input_tokens)   AS input_tokens,
                    SUM(cached_tokens)  AS cached_tokens,
                    SUM(output_tokens)  AS output_tokens,
                    SUM(total_tokens)   AS total_tokens,
 
                    -- audio tokens
                    SUM(audio_input_tokens)  AS audio_input_tokens,
                    SUM(audio_output_tokens) AS audio_output_tokens,
 
                    -- audio flat
                    SUM(duration_seconds)    AS duration_seconds,
 
                    -- speech flat
                    SUM(character_count)     AS character_count,
 
                    -- image tokens
                    SUM(input_text_tokens)          AS input_text_tokens,
                    SUM(cached_input_text_tokens)   AS cached_input_text_tokens,
                    SUM(input_image_tokens)         AS input_image_tokens,
                    SUM(cached_input_image_tokens)  AS cached_input_image_tokens,
                    SUM(output_image_tokens)        AS output_image_tokens,
 
                    -- image audit
                    SUM(image_count)   AS image_count,
 
                    -- cost and call count
                    SUM(cost_usd)      AS total_cost_usd,
                    COUNT(*)           AS total_calls
                FROM  cost_events
                WHERE {where}
                GROUP BY model, endpoint
                ORDER BY total_cost_usd DESC
            """),
            params,
        )
        rows = [dict(r) for r in result.mappings()]

        total_cost = sum(float(r["total_cost_usd"]) for r in rows)
        total_input = sum(int(r["input_tokens"] or 0) for r in rows)
        total_cached = sum(int(r["cached_tokens"] or 0) for r in rows)
        total_output = sum(int(r["output_tokens"] or 0) for r in rows)
        total_audio_in = sum(int(r["audio_input_tokens"] or 0) for r in rows)
        total_audio_out = sum(int(r["audio_output_tokens"] or 0) for r in rows)
        total_img_out = sum(int(r["output_image_tokens"] or 0) for r in rows)
        total_images = sum(int(r["image_count"] or 0) for r in rows)

        return {
            "tenant_id": tenant_id,
            "total_cost_usd": total_cost,
            # text
            "total_input_tokens": total_input,
            "total_cached_tokens": total_cached,
            "total_output_tokens": total_output,
            "total_tokens": total_input + total_output,
            # audio
            "total_audio_input_tokens": total_audio_in,
            "total_audio_output_tokens": total_audio_out,
            # image
            "total_output_image_tokens": total_img_out,
            "total_images_generated": total_images,
            "breakdown": rows,
        }

    async def rollup_daily(self, project_id: str | None = None) -> int:
        """
        Upsert events from the last 24 hours (relative to job run time) into
        customer_cost_daily, bucketed by calendar date. Safe to run repeatedly
        (e.g. hourly) — re-aggregates and overwrites any existing rows for the
        dates touched by the window, so partial-day totals get corrected as
        more events land.
        """
        project_filter = "AND project_id = :project_id" if project_id else ""
        params: dict = {}
        if project_id:
            params["project_id"] = project_id

        result = await self.session.execute(
            text(f"""
                INSERT INTO customer_cost_daily
                    (id, project_id, tenant_id, date, total_cost_usd, total_calls)
                SELECT
                    gen_random_uuid(),
                    project_id,
                    tenant_id,
                    created_at::date   AS date,
                    SUM(cost_usd)      AS total_cost_usd,
                    COUNT(*)           AS total_calls
                FROM  cost_events
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                AND created_at <= NOW()
                {project_filter}
                GROUP BY project_id, tenant_id, created_at::date
                ON CONFLICT (project_id, tenant_id, date)
                DO UPDATE SET
                    total_cost_usd = EXCLUDED.total_cost_usd,
                    total_calls    = EXCLUDED.total_calls
            """),
            params,
        )
        return result.rowcount


class AlertService:
    """Threshold management and alert firing."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def check_thresholds(self, project_id: str | None = None) -> list[dict]:
        """
        Compare today's running cost against thresholds.
        Inserts an alert row and fires notifications for every breach found.
        Returns the list of newly fired alerts.
        """
        project_filter = "AND t.project_id = :project_id" if project_id else ""
        params: dict = {}
        if project_id:
            params["project_id"] = project_id

        result = await self.session.execute(
            text(f"""
                SELECT
                    t.project_id,
                    t.tenant_id,
                    t.limit_usd,
                    COALESCE(SUM(e.cost_usd), 0) AS current_cost
                FROM  thresholds t
                LEFT JOIN cost_events e
                    ON  e.project_id  = t.project_id
                    AND e.tenant_id = t.tenant_id
                    AND e.created_at >= DATE_TRUNC('month', NOW())
                {project_filter}
                GROUP BY t.project_id, t.tenant_id, t.limit_usd
                HAVING COALESCE(SUM(e.cost_usd), 0) > t.limit_usd
            """),
            params,
        )
        breaches = [dict(r) for r in result.mappings()]
        fired: list[dict] = []

        for breach in breaches:
            # Avoid re-alerting within the last hour
            recent = await self.session.execute(
                text("""
                    SELECT 1 FROM alerts
                    WHERE project_id  = :project_id
                      AND tenant_id = :tenant_id
                      AND triggered_at > NOW() - INTERVAL '1 hour'
                    LIMIT 1
                """),
                {
                    "project_id": breach["project_id"],
                    "tenant_id": breach["tenant_id"],
                },
            )
            if recent.first():
                continue

            alert_id = str(uuid.uuid4())
            await self.session.execute(
                text("""
                    INSERT INTO alerts
                        (id, project_id, tenant_id, triggered_at, cost_at_trigger)
                    VALUES
                        (:id, :project_id, :tenant_id, NOW(), :cost_at_trigger)
                """),
                {
                    "id": alert_id,
                    "project_id": breach["project_id"],
                    "tenant_id": breach["tenant_id"],
                    "cost_at_trigger": breach["current_cost"],
                },
            )
            fired.append(breach)

            # Fire side-effects (best-effort, don't let failures block the commit)
            try:
                project = await get_project_by_id(self.session, breach["project_id"])
                await self.send_email(dict(project), breach)
                await self.send_webhook(dict(project), breach)
            except Exception as exc:
                logger.warning("Alert notification failed: %s", exc)

        return fired

    async def send_email(self, project: dict, breach: dict) -> None:
        """Send a plain-text alert email via SMTP (skipped if SMTP_USER not set)."""
        if not SMTP_USER:
            logger.info(
                "[AlertService] SMTP not configured, skipping email for %s", breach
            )
            return

        import smtplib
        from email.mime.text import MIMEText

        subject = (
            f"[Cost Alert] Customer {breach['tenant_id']} "
            f"exceeded ${breach['limit_usd']:.2f}"
        )
        body = (
            f"Project: {project['name']}\n"
            f"Customer: {breach['tenant_id']}\n"
            f"Threshold: ${float(breach['limit_usd']):.4f}\n"
            f"Current cost this month: ${float(breach['current_cost']):.4f}\n"
        )
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = ALERT_FROM_EMAIL
        msg["To"] = ALERT_FROM_EMAIL  # replace with project owner email from DB

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(ALERT_FROM_EMAIL, [ALERT_FROM_EMAIL], msg.as_string())

    async def send_webhook(self, project: dict, breach: dict) -> None:
        """POST breach data to ALERT_WEBHOOK_URL if configured."""
        if not ALERT_WEBHOOK_URL:
            return
        payload = {
            "event": "cost_threshold_exceeded",
            "project_id": str(project["id"]),
            "project_name": project["name"],
            "tenant_id": breach["tenant_id"],
            "limit_usd": float(breach["limit_usd"]),
            "current_cost_usd": float(breach["current_cost"]),
            "timestamp": datetime.now(timezone.utc)().isoformat(),
        }
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(ALERT_WEBHOOK_URL, json=payload)
            resp.raise_for_status()
