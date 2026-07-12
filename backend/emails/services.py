"""
backend/emails/service.py — Generic transactional email sender (Resend).

This module is intentionally standalone (no imports from `auth`) so any part of
the backend can send emails.

Environment variables:

RESEND_API_KEY      — Your Resend API key
EMAIL_FROM          — Verified sender address, e.g. "Token Limit <noreply@token-limit.com>"
FRONTEND_URL        — Frontend URL
"""

from __future__ import annotations

import logging
import os
from typing import Optional
from html import escape
from urllib.parse import quote

import resend

logger = logging.getLogger(__name__)

RESEND_API_KEY: Optional[str] = os.getenv("RESEND_API_KEY")
EMAIL_FROM: Optional[str] = os.getenv("EMAIL_FROM")
FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


def send_email(
    to: str,
    subject: str,
    html_body: str,
    text_body: Optional[str] = None,
    sender: Optional[str] = None,
) -> bool:
    """
    Send a transactional email via Resend.

    Returns True on success, False on failure.
    Never raises exceptions to callers.
    """

    from_email = sender or EMAIL_FROM

    if not RESEND_API_KEY:
        logger.error("RESEND_API_KEY is not configured.")
        return False

    if not from_email:
        logger.error("EMAIL_FROM is not configured.")
        return False

    params = {
        "from": from_email,
        "to": [to],
        "subject": subject,
        "html": html_body,
    }

    if text_body:
        params["text"] = text_body

    try:
        resend.Emails.send(params)
        logger.info("Email sent successfully to %s (%s)", to, subject)
        return True

    except Exception:
        logger.exception("Email delivery failed for %s", to)
        return False


def email_template(content: str) -> str:
    """
    Shared email layout.
    """

    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>Token Limit</title>

<style>
body {{
    margin:0;
    padding:0;
    background:#f6f8fb;
    font-family:Arial, Helvetica, sans-serif;
    color:#111827;
}}

.container {{
    max-width:600px;
    margin:40px auto;
    background:#ffffff;
    border-radius:12px;
    padding:32px;
    box-shadow:0 2px 8px rgba(0,0,0,0.05);
}}

.logo {{
    font-size:36px;
    font-weight:700;
    margin-bottom:24px;
}}

.footer {{
    margin-top:32px;
    color:#6b7280;
    font-size:13px;
    line-height:20px;
}}

.button {{
    display:inline-block;
    background:#111827;
    color:#ffffff !important;
    padding:12px 22px;
    border-radius:8px;
    text-decoration:none;
    font-weight:600;
}}

.code {{
    background:#f3f4f6;
    padding:16px;
    border-radius:10px;
    font-size:30px;
    font-weight:700;
    letter-spacing:8px;
    text-align:center;
    margin:24px 0;
}}
</style>

</head>

<body>

<div class="container">

<div class="logo">
TokenLimit
</div>

{content}

<div class="footer">
This email was sent by Token Limit.<br>
If you did not request this action, you can safely ignore this email.
</div>

</div>

</body>
</html>
"""


def send_verification_email(to: str, otp: str) -> bool:

    otp = escape(otp)

    subject = "Verify your Token Limit account"

    html_body = email_template(f"""
<p>
Welcome to Token Limit.
</p>

<p>
Use the verification code below to confirm your email address:
</p>

<div class="code">
{otp}
</div>

<p>
This code expires in <strong>10 minutes</strong>.
</p>
""")

    text_body = f"""
Welcome to Token Limit.

Your verification code is:

{otp}

This code expires in 10 minutes.

If you did not create an account, you can ignore this email.
"""

    return send_email(
        to=to,
        subject=subject,
        html_body=html_body,
        text_body=text_body.strip(),
    )


def send_password_reset_email(to: str, token: str) -> bool:

    safe_token = quote(token)

    link = f"{FRONTEND_URL}/reset-password?token={safe_token}"

    subject = "Reset your Token Limit password"

    html_body = email_template(f"""
<p>
We received a request to reset your Token Limit password.
</p>

<p>
Click the button below to choose a new password:
</p>

<p>
<a class="button" href="{link}">
Reset Password
</a>
</p>

<p>
This link expires according to your account security settings.
</p>
""")

    text_body = f"""
We received a request to reset your Token Limit password.

Reset your password here:

{link}

If you did not request this, you can safely ignore this email.
"""

    return send_email(
        to=to,
        subject=subject,
        html_body=html_body,
        text_body=text_body.strip(),
    )
