"""
FastAPI router — mount this in your main app:

    from billing.router import router as billing_router
    app.include_router(billing_router)
"""

from datetime import datetime, timezone
import logging
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from backend.auth.services import get_current_user
from backend.database.database import get_db
from backend.database.models import (
    CustomerCostDaily,
    User,
    MonthlyUsageSnapshot,
    get_current_project,
    get_project_by_id,
)
from . import services
from .plans import PLANS, get_plan
from .schemas import (
    ChangePlanRequest,
    CheckoutRequest,
    CheckoutResponse,
    PlansResponse,
    PlanInfo,
    PortalRequest,
    PortalResponse,
    SubscriptionResponse,
)
from .stripe_client import construct_webhook_event
from .services import handle_stripe_webhook

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/billing", tags=["billing"])


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.get("/plans", response_model=PlansResponse)
async def list_plans():
    """Return all available plans — used to render your pricing page."""
    return PlansResponse(
        plans=[
            PlanInfo(
                key=p.key,
                display_name=p.display_name,
                tracked_spend_limit_usd=p.tracked_spend_limit_usd,
                monthly_price_usd=p.monthly_price_usd,
            )
            for p in PLANS.values()
        ]
    )


# routers.py — unify limit_reached and is_blocked from single source
@router.get("/subscription", response_model=SubscriptionResponse)
async def get_subscription(request: Request, db: AsyncSession = Depends(get_db)):
    project = await get_current_project(db, request)
    if not project:
        raise HTTPException(status_code=400, detail="No project selected.")

    sub = await services.get_subscription(db, project.id)
    if not sub:
        now = datetime.now(timezone.utc)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        result = await db.execute(
            select(func.coalesce(func.sum(CustomerCostDaily.total_cost_usd), 0))
            .where(CustomerCostDaily.project_id == project.id)
            .where(CustomerCostDaily.date >= month_start.date())
        )
        tracked_free_plan_spend = float(result.scalar())
        free_plan = get_plan("free")
        limit_usd = free_plan.tracked_spend_limit_usd

        # FIX: read is_blocked from DB (set by worker) rather than
        # recalculating — keeps router and worker in sync.
        is_blocked = await services.is_project_blocked(db, project.id)

        return SubscriptionResponse(
            plan="free",
            status="active",
            current_period_start=month_start,
            current_period_end=None,
            cancel_at_period_end=False,
            tracked_spend_usd=tracked_free_plan_spend,
            limit_usd=limit_usd,
            is_blocked=is_blocked,
            limit_reached=is_blocked,
            elapced_grace=0,
        )

    plan = get_plan(sub.plan)
    snapshot = await db.execute(
        select(MonthlyUsageSnapshot).where(
            MonthlyUsageSnapshot.subscription_id == sub.id,
            MonthlyUsageSnapshot.period_start == sub.current_period_start,
        )
    )
    snapshot = snapshot.scalar_one_or_none()
    now = datetime.now(timezone.utc)
    grace_elapsed = 0
    # FIX: derive both limit_reached and is_blocked from same snapshot read
    is_blocked = False
    limit_reached = False

    if snapshot is not None:
        limit_reached = bool(snapshot.limit_exceeded)
        if snapshot.grace_period_started_at is not None:
            grace_elapsed = (now - snapshot.grace_period_started_at).days
            if grace_elapsed >= plan.grace_period_days:
                limit_reached = True
                is_blocked = True
        if snapshot.limit_exceeded:
            is_blocked = True
    return SubscriptionResponse(
        plan=sub.plan,
        status=sub.status,
        current_period_start=sub.current_period_start,
        current_period_end=sub.current_period_end,
        cancel_at_period_end=sub.cancel_at_period_end,
        tracked_spend_usd=float(snapshot.tracked_spend_usd) if snapshot else 0.0,
        limit_usd=plan.tracked_spend_limit_usd,
        is_blocked=is_blocked,
        limit_reached=limit_reached,
        elapced_grace=grace_elapsed,
    )


@router.post("/checkout", response_model=CheckoutResponse)
async def create_checkout(
    request: Request,
    body: CheckoutRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Create a Stripe Checkout session for upgrading to a paid plan.
    Returns a URL to redirect the user to.
    """
    try:
        plan = get_plan(body.plan)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if plan.stripe_price_id_monthly is None:
        raise HTTPException(
            status_code=400,
            detail=f"Plan '{body.plan}' cannot be purchased via checkout.",
        )

    if body.project_id:
        project = await get_project_by_id(db, body.project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found.")
        if str(project.owner_id) != str(current_user.id):
            raise HTTPException(status_code=403, detail="Forbidden.")
    else:
        project = await get_current_project(db, request)
        if not project:
            raise HTTPException(status_code=400, detail="No project selected.")
    user_email = current_user.email
    try:
        result = await services.create_upgrade_checkout(
            session=db,
            project=project,
            user_email=user_email,
            plan_key=body.plan,
            success_url=body.success_url,
            cancel_url=body.cancel_url,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # result["checkout_url"] is set for free -> paid (redirect to Stripe).
    # result["plan_changed"] is True for paid -> paid (already applied in
    # place via change_plan() — nothing to redirect to, just refresh state).
    return CheckoutResponse(
        checkout_url=result["checkout_url"],
        plan_changed=result["plan_changed"],
    )


@router.post("/portal", response_model=PortalResponse)
async def billing_portal(
    request: Request,
    body: PortalRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Return a Stripe billing-portal URL.
    Users land here to manage payment methods, invoices, and cancellations.
    """
    project = await get_current_project(db, request)
    if not project:
        raise HTTPException(status_code=400, detail="No project selected.")

    user_email = current_user.email
    url = await services.create_portal_url(
        session=db,
        project=project,
        user_email=user_email,
        return_url=body.return_url,
    )
    return PortalResponse(portal_url=url)


@router.post("/change-plan", response_model=SubscriptionResponse)
async def change_plan(
    request: Request,
    body: ChangePlanRequest,
    db: AsyncSession = Depends(get_db),
):
    """Upgrade or downgrade between paid plans (proration applied immediately)."""
    project = await get_current_project(db, request)
    if not project:
        raise HTTPException(status_code=400, detail="No project selected.")

    try:
        sub = await services.change_plan(db, project.id, body.plan)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    plan = get_plan(sub.plan)
    return SubscriptionResponse(
        plan=sub.plan,
        status=sub.status,
        current_period_start=sub.current_period_start,
        current_period_end=sub.current_period_end,
        cancel_at_period_end=sub.cancel_at_period_end,
        limit_usd=plan.tracked_spend_limit_usd,
    )


@router.post("/cancel")
async def cancel_subscription(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Cancel at end of billing period — access continues until then."""
    project = await get_current_project(db, request)
    if not project:
        raise HTTPException(status_code=400, detail="No project selected.")

    try:
        await services.request_cancel(db, project.id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"detail": "Subscription will cancel at the end of the current period."}


# ---------------------------------------------------------------------------
# Stripe webhook — no auth, verified by signature
# ---------------------------------------------------------------------------


@router.post("/webhook", include_in_schema=False)
async def stripe_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
    stripe_signature: str = Header(None, alias="stripe-signature"),
):
    payload = await request.body()

    try:
        event = construct_webhook_event(payload, stripe_signature)
    except Exception as e:
        logger.warning("Webhook signature verification failed: %s", e)
        raise HTTPException(status_code=400, detail="Invalid signature")

    await handle_stripe_webhook(
        session=db,
        stripe_event_id=event["id"],
        event_type=event["type"],
        payload=event.to_dict(),
    )
    return {"received": True}
