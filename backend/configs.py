# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

import os

from fastapi import Request
import redis.asyncio as redis
from fastapi import Request
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


def _get_project_id(request: Request) -> str:
    """Key function: use project_id injected by auth middleware, fall back to IP."""
    return getattr(request.state, "project_id", get_remote_address(request))


async def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis


limiter = Limiter(key_func=_get_project_id)
