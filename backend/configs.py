# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

import os
from typing import Any

from fastapi import Request
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address


DASHBOARD_ORIGIN: str = os.getenv("DASHBOARD_ORIGIN")
SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.sendgrid.net")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER: str = os.getenv("SMTP_USER", "")
SMTP_PASS: str = os.getenv("SMTP_PASS", "")
ALERT_FROM_EMAIL: str = os.getenv("ALERT_FROM_EMAIL", "alerts@yourapp.com")
STRIPE_WEBHOOK_SECRET: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")
ALERT_WEBHOOK_URL: str = os.getenv("ALERT_WEBHOOK_URL", "")  # optional external webhook


# ===========================================================================
# Pydantic schemas
# ===========================================================================


class CostEventIn(BaseModel):
    provider : str
    tenant_id: str
    model: str
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)


class CostEventBatch(BaseModel):
    events: list[CostEventIn]


class ThresholdIn(BaseModel):
    tenant_id: str
    limit_usd: float = Field(gt=0)


class StripeWebhookPayload(BaseModel):
    id: str
    type: str
    data: dict[str, Any]


def _get_project_id(request: Request) -> str:
    """Key function: use project_id injected by auth middleware, fall back to IP."""
    return getattr(request.state, "project_id", get_remote_address(request))


limiter = Limiter(key_func=_get_project_id)
