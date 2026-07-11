"""
backend/emails/service.py — Generic transactional email sender (AWS SES via boto3).

This module is intentionally standalone (no imports from `auth`) so any part of
the backend can send emails: `from backend.emails.service import send_email`.

Usage:
    from backend.emails.service import send_email
    send_email(to="user@example.com", subject="Hi", html_body="<p>Hello</p>")

For a specific, recurring email (verification, password reset, receipts, ...),
add a small wrapper function below (see send_verification_email) so callers
never have to build the subject/body themselves.

Environment variables (set these in .env):
  AWS_ACCESS_KEY_ID       — IAM access key with ses:SendEmail permission
  AWS_SECRET_ACCESS_KEY   — corresponding secret key
  AWS_REGION              — region your SES identity lives in, e.g. "eu-west-1" (default: us-east-1)
  SES_SENDER_EMAIL        — a verified SES sender identity, e.g. "no-reply@yourapp.com"
  FRONTEND_URL            — base URL of your frontend, used to build links in emails, e.g. "https://app.yourapp.com"

Note: while your SES account is in the sandbox, you can only send TO addresses
that are also verified in SES. Request production access in the SES console
to send to arbitrary recipients.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
SES_SENDER_EMAIL: Optional[str] = os.getenv("SES_SENDER_EMAIL")
FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

_ses_client = None


def _get_client():
    """Lazily create a single boto3 SES client and reuse it across calls."""
    global _ses_client
    if _ses_client is None:
        _ses_client = boto3.client(
            "ses",
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
    return _ses_client


def send_email(
    to: str,
    subject: str,
    html_body: str,
    text_body: Optional[str] = None,
    sender: Optional[str] = None,
) -> bool:
    """
    Send a single transactional email via AWS SES.

    Returns True on success, False on failure. Never raises — a flaky email
    provider should not turn into a 500 for the caller's request. Callers that
    care whether the email actually went out (e.g. to warn an admin) can check
    the return value; callers that don't (most routes) can fire-and-forget via
    FastAPI's BackgroundTasks.
    """
    if not (sender or SES_SENDER_EMAIL):
        logger.error("SES_SENDER_EMAIL is not configured; cannot send email to %s", to)
        return False

    body: dict = {"Html": {"Data": html_body, "Charset": "UTF-8"}}
    if text_body:
        body["Text"] = {"Data": text_body, "Charset": "UTF-8"}

    try:
        _get_client().send_email(
            Source=sender or SES_SENDER_EMAIL,
            Destination={"ToAddresses": [to]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": body,
            },
        )
        logger.info("Sent email to %s (subject=%r)", to, subject)
        return True
    except (ClientError, BotoCoreError) as exc:
        logger.error("Failed to send email to %s: %s", to, exc)
        return False


# ---------------------------------------------------------------------------
# Templated emails — one small function per email "type" the app sends.
# Routers/services call these instead of touching boto3 or building HTML.
# ---------------------------------------------------------------------------


def send_verification_email(to: str, otp: str) -> bool:
    """Sent right after registration (or on resend): a 6-digit code the user
    types into the app to confirm their email address."""
    subject = "Your verification code"
    html_body = f"""
    <p>Use this code to verify your email address:</p>
    <p style="font-size:28px; font-weight:700; letter-spacing:6px; margin:16px 0;">{otp}</p>
    <p>This code expires in 10 minutes. If you didn't create an account, you can ignore this email.</p>
    """
    text_body = (
        f"Your verification code is: {otp}\n\n"
        "This code expires in 10 minutes. If you didn't create an account, you can ignore this email."
    )
    return send_email(to, subject, html_body, text_body)


def send_password_reset_email(to: str, token: str) -> bool:
    """Example second email type — copy this pattern for anything else you
    need to send (receipts, invites, digest emails, ...)."""
    link = f"{FRONTEND_URL}/reset-password?token={token}"
    subject = "Reset your password"
    html_body = f"""
    <p>We received a request to reset your password.</p>
    <p><a href="{link}">Reset my password</a></p>
    <p>If you didn't request this, you can safely ignore this email.</p>
    """
    text_body = (
        "We received a request to reset your password.\n\n"
        f"{link}\n\n"
        "If you didn't request this, you can safely ignore this email."
    )
    return send_email(to, subject, html_body, text_body)
