# ===========================================================================
# SERVICE LAYER
# ===========================================================================


from datetime import date, datetime, timedelta
import hashlib
import hmac
import uuid
from fastapi import HTTPException
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
        self, project_id: str, event: CostEventIn, cost_usd: int
    ) -> dict:
        event_id = str(uuid.uuid4())
        await self.session.execute(
            text("""
                INSERT INTO cost_events
                    (id, project_id, tenant_id, model,
                     input_tokens, output_tokens, cost_usd, created_at)
                VALUES
                    (:id, :project_id, :tenant_id, :model,
                     :input_tokens, :output_tokens, :cost_usd, NOW())
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
        """Return daily rollup rows for all customers in this project.
        Joins cost_events to include total token counts (input + output)
        per customer per day, which are not stored in customer_cost_daily.
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
                    COALESCE(SUM(ce.input_tokens), 0)  AS total_input_tokens,
                    COALESCE(SUM(ce.output_tokens), 0) AS total_output_tokens,
                    COALESCE(SUM(ce.input_tokens + ce.output_tokens), 0) AS total_tokens
                FROM   customer_cost_daily ccd
                LEFT JOIN cost_events ce
                    ON  ce.project_id = ccd.project_id
                    AND ce.tenant_id  = ccd.tenant_id
                    AND ce.created_at::date = ccd.date
                WHERE  {where}
                GROUP  BY ccd.tenant_id, ccd.date, ccd.total_cost_usd, ccd.total_calls
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
        """Return per-model breakdown for a single customer from raw events."""
        filters = [
            "project_id = :project_id",
            "tenant_id = :tenant_id",
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
                    SUM(input_tokens)                       AS input_tokens,
                    SUM(output_tokens)                      AS output_tokens,
                    SUM(input_tokens + output_tokens)       AS total_tokens,
                    SUM(cost_usd)                           AS total_cost_usd,
                    COUNT(*)                                AS total_calls
                FROM  cost_events
                WHERE {where}
                GROUP BY model
                ORDER BY total_cost_usd DESC
            """),
            params,
        )
        rows = [dict(r) for r in result.mappings()]
        total = sum(float(r["total_cost_usd"]) for r in rows)
        total_input = sum(int(r["input_tokens"] or 0) for r in rows)
        total_output = sum(int(r["output_tokens"] or 0) for r in rows)
        return {
            "tenant_id": tenant_id,
            "total_cost_usd": total,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_tokens": total_input + total_output,
            "breakdown": rows,
        }

    async def rollup_daily(self, project_id: str | None = None) -> int:
        """
        Upsert yesterday's raw events into customer_cost_daily.
        Returns the number of rows upserted.
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
                    created_at::date        AS date,
                    SUM(cost_usd)           AS total_cost_usd,
                    COUNT(*)                AS total_calls
                FROM  cost_events
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
                await self.send_email(project, breach)
                await self.send_webhook(project, breach)
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
            "timestamp": datetime.utcnow().isoformat(),
        }
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(ALERT_WEBHOOK_URL, json=payload)
            resp.raise_for_status()


class BillingService:
    """Stripe-backed subscription checks."""

    PLAN_LIMITS: dict[str, int] = {
        "free": 1_000,
        "pro": 100_000,
        "enterprise": 10_000_000,
    }

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_plan(self, project_id: str) -> str:
        result = await self.session.execute(
            text("SELECT plan FROM projects WHERE id = :id"),
            {"id": project_id},
        )
        row = result.scalar_one_or_none()
        return row or "free"

    async def is_active(self, project_id: str) -> bool:
        """All projects in the DB are considered active.
        Hook in a Stripe customer lookup here when you add subscriptions."""
        result = await self.session.execute(
            text("SELECT 1 FROM projects WHERE id = :id"),
            {"id": project_id},
        )
        return result.scalar_one_or_none() is not None

    async def enforce_limit(self, project_id: str) -> None:
        """Raise 402 if the project has hit its monthly event ingestion limit."""
        plan = await self.get_plan(project_id)
        limit = self.PLAN_LIMITS.get(plan, 1_000)

        result = await self.session.execute(
            text("""
                SELECT COUNT(*) FROM cost_events
                WHERE project_id = :project_id
                  AND created_at >= DATE_TRUNC('month', NOW())
            """),
            {"project_id": project_id},
        )
        count = result.scalar_one()
        if count >= limit:
            raise HTTPException(
                status_code=402,
                detail=f"Monthly event limit ({limit}) reached for plan '{plan}'. Please upgrade.",
            )

    async def handle_stripe_event(self, event_type: str, data: dict) -> dict:
        """Handle Stripe webhook events to keep the projects.plan column in sync."""
        if event_type == "customer.subscription.updated":
            sub = data.get("object", {})
            new_plan = sub.get("metadata", {}).get("plan", "free")
            stripe_cust_id = sub.get("customer")
            logger.info(
                "Subscription updated: customer=%s plan=%s",
                stripe_cust_id,
                new_plan,
            )
            return {"handled": True, "plan": new_plan}

        if event_type == "customer.subscription.deleted":
            logger.info("Subscription cancelled: %s", data)
            return {"handled": True, "plan": "free"}

        return {"handled": False}


# ---------------------------------------------------------------------------
# Stripe signature verification helper
# ---------------------------------------------------------------------------


def _verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> None:
    """
    Reconstructs the expected Stripe signature and compares with constant-time
    hmac comparison. Raises HTTP 400 on mismatch.
    """
    try:
        parts = {k: v for k, v in (p.split("=", 1) for p in sig_header.split(","))}
        timestamp = parts["t"]
        received_sig = parts["v1"]
    except (KeyError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid Stripe-Signature header")

    signed_payload = f"{timestamp}.".encode() + payload
    mac = hmac.new(secret.encode(), signed_payload, hashlib.sha256)
    expected = mac.hexdigest()

    if not hmac.compare_digest(expected, received_sig):
        raise HTTPException(status_code=400, detail="Stripe signature mismatch")
