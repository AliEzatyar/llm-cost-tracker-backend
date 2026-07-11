"""
Billing plans — five tiers based on tracked LLM spend per month.
These map 1-to-1 with Stripe Price IDs (set via env vars).
"""

from dataclasses import dataclass
from typing import Optional
import os


@dataclass(frozen=True)
class Plan:
    key: str  # stored in projects.plan
    display_name: str
    tracked_spend_limit_usd: Optional[float]  # None = unlimited
    monthly_price_usd: float
    stripe_price_id_monthly: Optional[str]  # None for free & enterprise
    stripe_price_id_annual: Optional[str]
    # Grace period in days before hard-blocking over-limit projects
    grace_period_days: int = 0


PLANS: dict[str, Plan] = {
    "free": Plan(
        key="free",
        display_name="Free",
        tracked_spend_limit_usd=100.0,
        monthly_price_usd=0.0,
        stripe_price_id_monthly=None,
        stripe_price_id_annual=None,
        grace_period_days=0,
    ),
    "starter": Plan(
        key="starter",
        display_name="Starter",
        tracked_spend_limit_usd=3_000.0,
        monthly_price_usd=29.0,
        stripe_price_id_monthly=os.getenv("STRIPE_PRICE_STARTER_MONTHLY"),
        stripe_price_id_annual=os.getenv("STRIPE_PRICE_STARTER_ANNUAL"),
    ),
    "growth": Plan(
        key="growth",
        display_name="Growth",
        tracked_spend_limit_usd=10_000.0,
        monthly_price_usd=99.0,
        stripe_price_id_monthly=os.getenv("STRIPE_PRICE_GROWTH_MONTHLY"),
        stripe_price_id_annual=os.getenv("STRIPE_PRICE_GROWTH_ANNUAL"),
    ),
    "scale": Plan(
        key="scale",
        display_name="Scale",
        tracked_spend_limit_usd=50_000.0,
        monthly_price_usd=299.0,
        stripe_price_id_monthly=os.getenv("STRIPE_PRICE_SCALE_MONTHLY"),
        stripe_price_id_annual=os.getenv("STRIPE_PRICE_SCALE_ANNUAL"),
    )
}


def get_plan(key: str) -> Plan:
    plan = PLANS.get(key)
    if not plan:
        raise ValueError(f"Unknown plan: {key!r}. Valid plans: {list(PLANS)}")
    return plan


def get_plan_for_price_id(price_id: str) -> Optional[Plan]:
    """Reverse-lookup: Stripe price ID → Plan (used in webhook handler)."""
    for plan in PLANS.values():
        if price_id in (plan.stripe_price_id_monthly, plan.stripe_price_id_annual):
            return plan
    return None
