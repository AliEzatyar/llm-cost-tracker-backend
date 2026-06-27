from datetime import datetime

from pydantic import BaseModel, Field


class CreateProjectRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    # Plan is informational only at creation time — every new project starts
    # on "free" regardless of what's sent here. Upgrading to a paid plan
    # requires a Stripe Checkout session (POST /billing/checkout), since a
    # paid plan with no Stripe subscription would be an inconsistent state.


class CreateProjectResponse(BaseModel):
    id: str
    name: str
    plan: str
    api_key: str  # raw key — shown ONCE, never stored
    created_at: datetime


class UpdateProjectRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    # Plan is intentionally NOT editable here. Changing plans has billing
    # consequences (Stripe subscription create/upgrade/downgrade) and must
    # go through the /billing endpoints:
    #   free -> paid   : POST /billing/checkout
    #   paid -> paid   : POST /billing/change-plan
    #   paid -> free   : POST /billing/cancel
