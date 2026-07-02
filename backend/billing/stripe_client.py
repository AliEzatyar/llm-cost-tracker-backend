"""
Thin wrapper around the Stripe SDK.
All raw Stripe calls live here — services never import stripe directly.
"""

from dotenv import load_dotenv
import os
import stripe

load_dotenv
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
    """
    Upgrade or downgrade — resets the billing cycle to start now.

    billing_cycle_anchor="now" resets current_period_start/current_period_end
    so the new cycle runs from today for one full interval (Stripe handles
    28/29/30/31-day months correctly), and the next renewal lands exactly one
    month from today rather than staying anchored to the original signup date.

    proration_behavior="none" means no credit/charge is calculated against the
    old plan's remaining time — Stripe bills the full price of the new plan
    immediately on the customer's card, and nothing from the previous plan's
    cycle carries over or gets separately invoiced.
    """
    sub = stripe.Subscription.retrieve(subscription_id)
    return stripe.Subscription.modify(
        subscription_id,
        items=[{"id": sub["items"]["data"][0]["id"], "price": new_price_id}],
        proration_behavior="none",
        billing_cycle_anchor="now",
    )


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------


def construct_webhook_event(payload: bytes, sig_header: str) -> stripe.Event:
    return stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
