"""
Pydantic v2 schemas for billing request/response bodies.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, HttpUrl

# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class CheckoutRequest(BaseModel):
    plan: str  # starter | growth | scale
    success_url: str
    cancel_url: str
    # Optional explicit project id. Used as a fallback when the caller can't
    # set X-Project-ID (e.g. it's already overridden by a wrapper fetch that
    # injects the *currently selected* project's header). If both are
    # provided, project_id takes precedence.
    project_id: Optional[str] = None


class ChangePlanRequest(BaseModel):
    plan: str  # any paid plan key


class PortalRequest(BaseModel):
    return_url: str


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class PlanInfo(BaseModel):
    key: str
    display_name: str
    tracked_spend_limit_usd: Optional[float]
    monthly_price_usd: float


class SubscriptionResponse(BaseModel):
    plan: str
    status: str
    current_period_start: Optional[datetime]
    current_period_end: Optional[datetime]
    cancel_at_period_end: bool
    tracked_spend_usd: Optional[float] = None
    limit_usd: Optional[float] = None
    is_blocked: bool = False
    limit_reached: bool = False
    elapced_grace: int = 0


class CheckoutResponse(BaseModel):
    checkout_url: str
    checkout_url: Optional[str] = None
    plan_changed: bool = False

class PortalResponse(BaseModel):
    portal_url: str


class PlansResponse(BaseModel):
    plans: list[PlanInfo]
