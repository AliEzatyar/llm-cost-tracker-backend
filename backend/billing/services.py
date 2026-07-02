"""
BillingService — orchestrates plan changes, usage checks, and limit enforcement.
All DB writes go through here; routes are thin.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.models import (
    BillingEvent,
    MonthlyUsageSnapshot,
    Project,
    Subscription,
    get_project_by_id,
)
from backend.billing.plans import PLANS, Plan, get_plan, get_plan_for_price_id
from backend.billing.stripe_client import (
    cancel_subscription,
    change_subscription_plan,
    create_checkout_session,
    create_billing_portal_session,
    create_customer,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subscription helpers
# ---------------------------------------------------------------------------


async def get_subscription(
    session: AsyncSession, project_id: str
) -> Optional[Subscription]:
    result = await session.execute(
        select(Subscription).where(Subscription.project_id == project_id)
    )
    return result.scalar_one_or_none()


async def get_or_create_subscription(
    session: AsyncSession,
    project: Project,
    user_email: str,
) -> Subscription:
    """
    Idempotent — returns existing subscription or bootstraps a free-tier one.
    `project` is the dict returned by get_project_by_id().
    """
    existing = await get_subscription(session, project.id)
    if existing:
        return existing

    # Create a Stripe customer for this project
    stripe_customer = create_customer(
        email=user_email,
        project_id=project.id,
        name=project.name,
    )

    sub = Subscription(
        project_id=project.id,
        stripe_customer_id=stripe_customer["id"],
        plan="free",
        status="active",
    )
    session.add(sub)
    await session.flush()
    return sub


# ---------------------------------------------------------------------------
# Checkout
# ---------------------------------------------------------------------------


async def create_upgrade_checkout(
    session: AsyncSession,
    project: Project,
    user_email: str,
    plan_key: str,
    success_url: str,
    cancel_url: str,
) -> str:
    """Returns a Stripe Checkout URL for the given plan."""
    plan: Plan = get_plan(plan_key)

    if plan.stripe_price_id_monthly is None:
        raise ValueError(f"Plan '{plan_key}' has no Stripe price ID configured.")

    sub = await get_or_create_subscription(session, project, user_email)

    session_obj = create_checkout_session(
        customer_id=sub.stripe_customer_id,
        price_id=plan.stripe_price_id_monthly,
        success_url=success_url,
        cancel_url=cancel_url,
        project_id=project.id,
    )
    await session.commit()
    return session_obj["url"]


async def create_portal_url(
    session: AsyncSession,
    project: dict,
    user_email: str,
    return_url: str,
) -> str:
    """Returns a Stripe billing-portal URL so users self-manage their sub."""
    sub = await get_or_create_subscription(session, project, user_email)
    portal = create_billing_portal_session(
        customer_id=sub.stripe_customer_id,
        return_url=return_url,
    )
    await session.commit()
    return portal["url"]


# ---------------------------------------------------------------------------
# Usage enforcement
# ---------------------------------------------------------------------------


async def get_current_usage_snapshot(
    session: AsyncSession,
    subscription_id,
    period_start: datetime,
) -> Optional[MonthlyUsageSnapshot]:
    result = await session.execute(
        select(MonthlyUsageSnapshot).where(
            MonthlyUsageSnapshot.subscription_id == subscription_id,
            MonthlyUsageSnapshot.period_start == period_start,
        )
    )
    return result.scalar_one_or_none()


async def sync_tracked_spend(
    session: AsyncSession,
    project_id: str,
    period_start: datetime,
    period_end: datetime,
) -> float:
    """
    Compute total tracked spend for this project in the current billing period
    from CustomerCostDaily, upsert into MonthlyUsageSnapshot, and return
    the total USD spent.

    For free-plan projects (no Subscription row): enforces the spend limit
    and grace period by updating the Project directly (block flag), since
    there is no subscription/snapshot to anchor to.
    """
    result = await session.execute(
        text("""
            SELECT COALESCE(SUM(total_cost_usd), 0)
            FROM   customer_cost_daily
            WHERE  project_id = :project_id
              AND  date >= :start
              AND  date <  :end
        """),
        {
            "project_id": project_id,
            "start": period_start.date(),
            "end": period_end.date(),
        },
    )
    total_usd: float = float(result.scalar_one())

    sub = await get_subscription(session, project_id)

    if not sub:
        # ----------------------------------------------------------------
        # Free plan — no Subscription or MonthlyUsageSnapshot row exists.
        # Enforce the limit by checking spend against the free plan cap
        # and blocking the project directly if grace period is exhausted.
        # ----------------------------------------------------------------
        free_plan = get_plan("free")
        limit = free_plan.tracked_spend_limit_usd
        print("no sub, free, limit", limit)
        if limit is not None and total_usd > limit:
            # Read grace metadata stored on the Project row itself.
            proj_result = await session.execute(
                text("SELECT * FROM projects WHERE id = :id"),
                {"id": project_id},
            )
            proj = proj_result.mappings().first()

            now = datetime.now(timezone.utc)
            grace_started = proj.get("grace_period_started_at") if proj else None

            if grace_started is None:
                print("grace started none")
                # First breach — record grace start
                await session.execute(
                    text("""
                        UPDATE projects
                        SET    grace_period_started_at = :now
                        WHERE  id = :id
                          AND  grace_period_started_at IS NULL
                    """),
                    {"now": now, "id": project_id},
                )
                print(
                    "project=%s (free) exceeded plan limit $%.2f (spent $%.2f)"
                    " — grace period started",
                    project_id,
                    limit,
                    total_usd,
                )
            else:
                print("grace elapsed")
                grace_elapsed = (now - grace_started).days
                if grace_elapsed >= free_plan.grace_period_days:
                    await session.execute(
                        text("UPDATE projects SET is_blocked = TRUE WHERE id = :id"),
                        {"id": project_id},
                    )
                    print(
                        "project=%s (free) hard-blocked after %d-day grace period",
                        project_id,
                        free_plan.grace_period_days,
                    )
        else:
            # Back under limit or never exceeded — clear any grace/block flags
            await session.execute(
                text("""
                    UPDATE projects
                    SET    grace_period_started_at = NULL,
                           is_blocked = FALSE
                    WHERE  id = :id
                """),
                {"id": project_id},
            )

        await session.commit()
        return total_usd

    # ----------------------------------------------------------------
    # Paid plan — upsert MonthlyUsageSnapshot and enforce limit there.
    # ----------------------------------------------------------------
    snapshot = await get_current_usage_snapshot(session, sub.id, period_start)
    plan = get_plan(sub.plan)

    now = datetime.now(timezone.utc)
    limit = plan.tracked_spend_limit_usd  # None = unlimited

    if snapshot is None:
        snapshot = MonthlyUsageSnapshot(
            subscription_id=sub.id,
            period_start=period_start,
            period_end=period_end,
            tracked_spend_usd=total_usd,
        )
        session.add(snapshot)
    else:
        snapshot.tracked_spend_usd = total_usd
        snapshot.updated_at = now

    # Enforce limit
    if limit is not None and total_usd > limit:
        if snapshot.grace_period_started_at is None:
            snapshot.grace_period_started_at = now
            print(
                "project=%s exceeded plan limit $%.2f (spent $%.2f) — grace period started",
                project_id,
                limit,
                total_usd,
            )

        grace_elapsed = (now - snapshot.grace_period_started_at).days
        if grace_elapsed >= plan.grace_period_days:
            snapshot.limit_exceeded = True
            project = await get_project_by_id(session, project_id)
            project.is_blocked = True
            print(
                "project=%s hard-blocked after %d-day grace period",
                project_id,
                plan.grace_period_days,
            )
    else:
        # Back under limit (e.g. after upgrade) — clear flags
        snapshot.limit_exceeded = False
        snapshot.grace_period_started_at = None

    await session.commit()
    return total_usd


async def _reset_usage_snapshot_for_period(
    session: AsyncSession,
    subscription_id,
    period_start: datetime,
    period_end: datetime,
) -> MonthlyUsageSnapshot:
    """
    Ensure a MonthlyUsageSnapshot row exists for this exact billing period,
    with tracked spend reset to 0 and no block/grace flags carried over from
    a previous plan or cycle.

    Called whenever a subscription's billing period restarts "now" (plan
    upgrade/downgrade — see stripe_client.change_subscription_plan). Without
    this, is_project_blocked()/the /subscription route would keep reading the
    *old* snapshot (matched by old period_start) until the next ingest-driven
    sync_tracked_spend() call happened to notice the period changed and
    created a fresh one — leaving stale spend/block state visible in the
    meantime.
    """
    snapshot = await get_current_usage_snapshot(session, subscription_id, period_start)
    if snapshot is None:
        snapshot = MonthlyUsageSnapshot(
            subscription_id=subscription_id,
            period_start=period_start,
            period_end=period_end,
            tracked_spend_usd=0.0,
        )
        session.add(snapshot)
    else:
        snapshot.tracked_spend_usd = 0.0
        snapshot.period_end = period_end
        snapshot.updated_at = datetime.now(timezone.utc)

    snapshot.limit_exceeded = False
    snapshot.grace_period_started_at = None
    return snapshot


async def is_project_blocked(session: AsyncSession, project_id: str) -> bool:
    """
    Fast check called on every ingest request.
    Returns True if the project is hard-blocked due to exceeding its plan limit.

    Free plan (no Subscription row): reads is_blocked directly from the Project row,
    set by sync_tracked_spend after the grace period is exhausted.

    Paid plans: reads limit_exceeded from the latest MonthlyUsageSnapshot.
    """
    sub = await get_subscription(session, project_id)

    if not sub:
        # Free plan — block state is tracked on the Project row itself.
        result = await session.execute(
            text("SELECT is_blocked FROM projects WHERE id = :id"),
            {"id": project_id},
        )
        row = result.mappings().first()
        return bool(row and row["is_blocked"])

    result = await session.execute(
        select(MonthlyUsageSnapshot)
        .where(MonthlyUsageSnapshot.subscription_id == sub.id)
        .order_by(MonthlyUsageSnapshot.period_start.desc())
        .limit(1)
    )
    snapshot = result.scalar_one_or_none()
    return bool(snapshot and snapshot.limit_exceeded)


# ---------------------------------------------------------------------------
# Plan change (upgrade / downgrade)
# ---------------------------------------------------------------------------


async def change_plan(
    session: AsyncSession,
    project_id: str,
    new_plan_key: str,
) -> Subscription:
    """
    Upgrade or downgrade between paid plans.
    For free → paid use create_upgrade_checkout() instead.
    For paid → free use request_cancel() instead.
    """
    if new_plan_key == "free":
        raise ValueError("To cancel to free, use request_cancel() instead.")

    plan = get_plan(new_plan_key)
    sub = await get_subscription(session, project_id)
    if not sub:
        raise ValueError(
            "No paid subscription found — use checkout to upgrade from free."
        )
    if not sub.stripe_subscription_id:
        raise ValueError("No Stripe subscription attached — use checkout to upgrade.")
    if not plan.stripe_price_id_monthly:
        raise ValueError(
            f"Plan '{new_plan_key}' has no Stripe price — cannot switch via API."
        )

    stripe_sub = change_subscription_plan(
        sub.stripe_subscription_id, plan.stripe_price_id_monthly
    )

    # Stripe's Basil API version (2025-03-31+) removed current_period_start/
    # current_period_end from the top-level Subscription object — they now
    # live on the subscription item instead. Mirror the fallback already
    # used in _on_subscription_updated() below, or this KeyErrors on any
    # account running Basil or later and the whole upgrade silently fails
    # to persist (Stripe's side still goes through — only our DB write
    # never happens).
    item = stripe_sub["items"]["data"][0]
    raw_start = stripe_sub.get("current_period_start") or item.get(
        "current_period_start"
    )
    raw_end = stripe_sub.get("current_period_end") or item.get("current_period_end")
    if raw_start is None or raw_end is None:
        raise RuntimeError(
            f"Stripe subscription {stripe_sub['id']} returned no "
            "current_period_start/end on either the subscription or its "
            "item — check the account's Stripe API version."
        )

    # Use the Stripe response to update all fields immediately so the DB is
    # fully in sync without waiting for the webhook to arrive.
    sub.plan = new_plan_key
    sub.stripe_price_id = item["price"]["id"]
    sub.cancel_at_period_end = stripe_sub["cancel_at_period_end"]
    sub.current_period_start = datetime.fromtimestamp(raw_start, tz=timezone.utc)
    sub.current_period_end = datetime.fromtimestamp(raw_end, tz=timezone.utc)
    sub.status = stripe_sub["status"]

    await session.execute(
        text("UPDATE projects SET plan = :plan WHERE id = :id"),
        {"plan": new_plan_key, "id": project_id},
    )

    # The Stripe call above reset the billing cycle to start now (see
    # stripe_client.change_subscription_plan), so sub.current_period_start/end
    # were just set to the new cycle a few lines up. Reset usage tracking to
    # that new period rather than clearing flags on whatever snapshot happens
    # to be "latest" — that snapshot still belongs to the old period and
    # carries the old plan's tracked spend.
    await _reset_usage_snapshot_for_period(
        session, sub.id, sub.current_period_start, sub.current_period_end
    )

    # Clear any block/grace-period flags carried over from the old plan.
    # check_plan_limits() (routers.py, run on every /ingest call) reads
    # projects.is_blocked directly — it does NOT consult the snapshot's
    # limit_exceeded flag above — so without this, a project that was
    # blocked on the old plan stays blocked after upgrading, even though
    # usage was just reset to 0 for the new period.
    await session.execute(
        text("""
            UPDATE projects
            SET    is_blocked               = FALSE,
                   grace_period_started_at  = NULL
            WHERE  id = :id
        """),
        {"id": project_id},
    )

    await session.commit()
    return sub


async def request_cancel(
    session: AsyncSession,
    project_id: str,
) -> Subscription:
    """Cancel at period end (no immediate loss of access)."""
    sub = await get_subscription(session, project_id)
    if not sub or not sub.stripe_subscription_id:
        raise ValueError("No active paid subscription to cancel.")

    cancel_subscription(sub.stripe_subscription_id, at_period_end=True)
    sub.cancel_at_period_end = True
    await session.commit()
    return sub


# ---------------------------------------------------------------------------
# Webhook processing
# ---------------------------------------------------------------------------


async def handle_stripe_webhook(
    session: AsyncSession,
    stripe_event_id: str,
    event_type: str,
    payload: dict,
) -> None:
    """
    Idempotent webhook handler.
    All supported event types are handled here; unknown ones are logged and skipped.
    """
    # Idempotency guard
    existing = await session.execute(
        select(BillingEvent).where(BillingEvent.stripe_event_id == stripe_event_id)
    )
    if existing.scalar_one_or_none():
        print("Duplicate webhook %s — skipping", stripe_event_id)
        return

    # Persist raw event first (for audit)
    project_id = _extract_project_id(event_type, payload)
    billing_event = BillingEvent(
        stripe_event_id=stripe_event_id,
        event_type=event_type,
        project_id=project_id,
        payload=str(payload),
    )
    session.add(billing_event)
    print("event type ....", event_type)
    try:
        if event_type == "checkout.session.completed":
            await _on_checkout_completed(session, payload)

        elif event_type in (
            "customer.subscription.updated",
            "customer.subscription.created",
        ):
            await _on_subscription_updated(session, payload)

        elif event_type == "customer.subscription.deleted":
            await _on_subscription_deleted(session, payload)

        elif event_type in (
            "invoice.paid",
            "invoice.payment_succeeded",
        ):
            await _on_invoice_paid(session, payload)
        elif event_type == "invoice.payment_failed":
            await _on_payment_failed(session, payload)

        else:
            print("Unhandled Stripe event type: %s", event_type)

        billing_event.processed = True
    except Exception:
        print("Error processing webhook %s (%s)", stripe_event_id, event_type)
        raise
    finally:
        await session.commit()


def _extract_project_id(event_type: str, payload: dict) -> Optional[str]:
    try:
        data = payload.get("data", {}).get("object", {})
        return data.get("metadata", {}).get("project_id") or data.get("lines", {}).get(
            "data", [{}]
        )[0].get("metadata", {}).get("project_id")
    except Exception:
        return None


async def _on_checkout_completed(session: AsyncSession, payload: dict) -> None:
    """
    Link the Stripe customer + subscription IDs to our Subscription row.
    Do NOT attempt to resolve the plan here — line_items is never populated
    in the webhook payload (it requires a separate API expand that doesn't
    apply to webhook objects). The plan is set correctly by the
    customer.subscription.created event that fires immediately after this,
    which calls _on_subscription_updated with the full subscription object.
    """
    obj = payload["data"]["object"]
    project_id = obj["metadata"].get("project_id")
    stripe_customer_id = obj["customer"]
    stripe_subscription_id = obj.get("subscription")

    if not project_id:
        logger.warning("checkout.session.completed missing project_id metadata")
        return

    result = await session.execute(
        select(Subscription).where(Subscription.project_id == project_id)
    )
    sub = result.scalar_one_or_none()
    if sub:
        sub.stripe_subscription_id = stripe_subscription_id
        sub.stripe_customer_id = stripe_customer_id
        sub.status = "active"
    else:
        session.add(
            Subscription(
                project_id=project_id,
                stripe_customer_id=stripe_customer_id,
                stripe_subscription_id=stripe_subscription_id,
                plan="free",  # will be updated by customer.subscription.created
                status="active",
            )
        )

    # Clear any free-plan block flags now that the user has paid.
    await session.execute(
        text("""
            UPDATE projects
            SET    is_blocked               = FALSE,
                   grace_period_started_at  = NULL
            WHERE  id = :id
        """),
        {"id": project_id},
    )


async def _on_subscription_updated(session: AsyncSession, payload: dict) -> None:
    print("\n=== SUBSCRIPTION UPDATED EVENT ===")

    obj = payload["data"]["object"]

    stripe_sub_id = obj["id"]
    stripe_customer_id = obj["customer"]  # always present on subscription objects
    status = obj["status"]

    items_data = obj.get("items", {}).get("data", [])
    if not items_data:
        print(f"No items in subscription object {stripe_sub_id}, skipping")
        return
    price_id = items_data[0]["price"]["id"]

    print(f"Subscription ID: {stripe_sub_id}")
    print(f"Status: {status}")
    print(f"Price ID: {price_id}")

    # current_period_start/end are top-level on the subscription object.
    # Fall back to the item level in case of API version differences.
    item = items_data[0]
    raw_start = obj.get("current_period_start") or item.get("current_period_start")
    raw_end = obj.get("current_period_end") or item.get("current_period_end")

    period_start = (
        datetime.fromtimestamp(raw_start, tz=timezone.utc) if raw_start else None
    )
    period_end = datetime.fromtimestamp(raw_end, tz=timezone.utc) if raw_end else None

    cancel_at_period_end = obj.get("cancel_at_period_end", False)

    print(f"Period Start: {period_start}")
    print(f"Period End: {period_end}")
    print(f"Cancel At Period End: {cancel_at_period_end}")

    plan = get_plan_for_price_id(price_id)
    plan_key = plan.key if plan else None

    print(f"Resolved Plan: {plan_key}")

    # Primary lookup: by stripe_subscription_id (normal path, post-checkout).
    result = await session.execute(
        select(Subscription).where(Subscription.stripe_subscription_id == stripe_sub_id)
    )
    sub = result.scalar_one_or_none()

    if not sub:
        # Race condition fallback: customer.subscription.created arrived before
        # checkout.session.completed, so stripe_subscription_id hasn't been stamped
        # on the row yet. The Subscription row always exists with a stripe_customer_id
        # (created by get_or_create_subscription before checkout), so look up by that.
        logger.warning(
            "subscription event %s: no row found for stripe_sub_id=%s — "
            "falling back to customer lookup (stripe_customer_id=%s)",
            stripe_sub_id,
            stripe_sub_id,
            stripe_customer_id,
        )
        result = await session.execute(
            select(Subscription).where(
                Subscription.stripe_customer_id == stripe_customer_id
            )
        )
        sub = result.scalar_one_or_none()

    if not sub:
        # Truly unknown customer — log and skip rather than crash.
        # checkout.session.completed will create the row; subsequent webhooks will
        # resolve correctly once stripe_subscription_id is stamped.
        logger.error(
            "subscription event %s: no Subscription row found for "
            "stripe_sub_id=%s or stripe_customer_id=%s — skipping",
            stripe_sub_id,
            stripe_sub_id,
            stripe_customer_id,
        )
        return

    # Stamp stripe_subscription_id now if we arrived via the customer fallback
    # (i.e. checkout.session.completed hasn't fired yet).
    if not sub.stripe_subscription_id:
        logger.info(
            "Stamping stripe_subscription_id=%s on subscription for customer=%s "
            "(race condition: subscription event arrived before checkout.session.completed)",
            stripe_sub_id,
            stripe_customer_id,
        )
        sub.stripe_subscription_id = stripe_sub_id

    print(f"Found DB subscription ID: {sub.id}")
    print(f"Current DB plan: {sub.plan}")

    # Always update these fields regardless of whether the plan changed.
    sub.status = status
    sub.stripe_price_id = price_id
    sub.current_period_start = period_start
    sub.current_period_end = period_end
    sub.cancel_at_period_end = cancel_at_period_end

    if plan_key and sub.plan != plan_key:
        print(f"Plan change detected: {sub.plan} -> {plan_key}")

        sub.plan = plan_key

        await session.execute(
            text("UPDATE projects SET plan = :plan WHERE id = :id"),
            {"plan": plan_key, "id": sub.project_id},
        )

        print(f"Updated project {sub.project_id} to plan {plan_key}")

        # period_start/period_end above are the freshly-reset cycle for this
        # plan change. Reset usage tracking to it (idempotent against the
        # same reset change_plan() already did synchronously — this branch is
        # what actually applies it if the plan was changed directly in the
        # Stripe dashboard rather than through our API).
        if period_start and period_end:
            await _reset_usage_snapshot_for_period(
                session, sub.id, period_start, period_end
            )
            # Same reasoning as change_plan(): check_plan_limits() reads
            # projects.is_blocked directly, not the snapshot, so it has to
            # be cleared here too or a project blocked on the old plan
            # stays blocked after a dashboard-initiated plan change.
            await session.execute(
                text("""
                    UPDATE projects
                    SET    is_blocked               = FALSE,
                           grace_period_started_at  = NULL
                    WHERE  id = :id
                """),
                {"id": sub.project_id},
            )
        else:
            print("No period_start/period_end on event — skipping usage reset")
    else:
        print("No plan change detected")

    print("=== SUBSCRIPTION UPDATE COMPLETE ===\n")


async def _on_subscription_deleted(session: AsyncSession, payload: dict) -> None:
    """Subscription canceled — downgrade to free."""
    obj = payload["data"]["object"]
    stripe_sub_id = obj["id"]

    result = await session.execute(
        select(Subscription).where(Subscription.stripe_subscription_id == stripe_sub_id)
    )
    sub = result.scalar_one_or_none()
    if not sub:
        return

    sub.status = "canceled"
    sub.plan = "free"
    sub.stripe_subscription_id = None
    sub.cancel_at_period_end = False

    await session.execute(
        text("UPDATE projects SET plan = 'free' WHERE id = :id"),
        {"id": sub.project_id},
    )

    # FIX 3: Clear snapshot flags on the way back to free — the next
    # sync_tracked_spend run will re-evaluate against the free plan limit.
    snapshot_result = await session.execute(
        select(MonthlyUsageSnapshot)
        .where(MonthlyUsageSnapshot.subscription_id == sub.id)
        .order_by(MonthlyUsageSnapshot.period_start.desc())
        .limit(1)
    )
    snapshot = snapshot_result.scalar_one_or_none()
    if snapshot is not None:
        snapshot.limit_exceeded = False
        snapshot.grace_period_started_at = None


async def _on_invoice_paid(session: AsyncSession, payload: dict) -> None:
    obj = payload["data"]["object"]
    stripe_sub_id = obj.get("subscription")
    if not stripe_sub_id:
        return

    result = await session.execute(
        select(Subscription).where(Subscription.stripe_subscription_id == stripe_sub_id)
    )
    sub = result.scalar_one_or_none()
    if not sub:
        return

    sub.status = "active"

    # FIX: clear limit_exceeded on the latest snapshot so projects aren't
    # blocked at the start of a new billing period until the worker runs.
    snapshot_result = await session.execute(
        select(MonthlyUsageSnapshot)
        .where(MonthlyUsageSnapshot.subscription_id == sub.id)
        .order_by(MonthlyUsageSnapshot.period_start.desc())
        .limit(1)
    )
    snapshot = snapshot_result.scalar_one_or_none()
    if snapshot is not None:
        snapshot.limit_exceeded = False
        snapshot.grace_period_started_at = None


async def _on_payment_failed(session: AsyncSession, payload: dict) -> None:
    obj = payload["data"]["object"]
    stripe_sub_id = obj.get("subscription")
    if not stripe_sub_id:
        return

    result = await session.execute(
        select(Subscription).where(Subscription.stripe_subscription_id == stripe_sub_id)
    )
    sub = result.scalar_one_or_none()
    if sub:
        sub.status = "past_due"
        print(
            "Payment failed for subscription %s (project=%s)",
            stripe_sub_id,
            sub.project_id,
        )
