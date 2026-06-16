"""
Thin wrapper around the Stripe SDK.
All raw Stripe calls live here — services never import stripe directly.
"""

import os
import stripe

stripe.api_key = os.environ["STRIPE_SECRET_KEY"]
WEBHOOK_SECRET = os.environ["STRIPE_WEBHOOK_SECRET"]


# ---------------------------------------------------------------------------
# Customer
# ---------------------------------------------------------------------------


def create_customer(email: str, project_id: str, name: str) -> stripe.Customer:
    return stripe.Customer.create(
        email=email,
        name=name,
        metadata={"project_id": project_id},
    )


def get_customer(customer_id: str) -> stripe.Customer:
    return stripe.Customer.retrieve(customer_id)


# ---------------------------------------------------------------------------
# Checkout / billing portal
# ---------------------------------------------------------------------------


# stripe_client.py — expand line_items in checkout session creation
def create_checkout_session(
    customer_id: str,
    price_id: str,
    success_url: str,
    cancel_url: str,
    project_id: str,
) -> stripe.checkout.Session:
    return stripe.checkout.Session.create(
        customer=customer_id,
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"project_id": project_id},
        allow_promotion_codes=True,
        billing_address_collection="auto",
        expand=["line_items"],  
    )


def create_billing_portal_session(
    customer_id: str,
    return_url: str,
) -> stripe.billing_portal.Session:
    return stripe.billing_portal.Session.create(
        customer=customer_id, return_url=return_url
    )


# ---------------------------------------------------------------------------
# Subscription management
# ---------------------------------------------------------------------------


def cancel_subscription(
    subscription_id: str, at_period_end: bool = True
) -> stripe.Subscription:
    return stripe.Subscription.modify(
        subscription_id,
        cancel_at_period_end=at_period_end,
    )


def change_subscription_plan(
    subscription_id: str,
    new_price_id: str,
) -> stripe.Subscription:
    """Upgrade or downgrade — prorated immediately."""
    sub = stripe.Subscription.retrieve(subscription_id)
    return stripe.Subscription.modify(
        subscription_id,
        items=[{"id": sub["items"]["data"][0]["id"], "price": new_price_id}],
        proration_behavior="always_invoice",
    )


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------


def construct_webhook_event(payload: bytes, sig_header: str) -> stripe.Event:
    return stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
