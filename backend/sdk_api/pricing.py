"""
pricing.py
----------
Versioned, provider-aware pricing registry.

All rates are stored as USD per 1,000,000 tokens (per-million).
Cache rates follow each provider's actual billing structure:

  OpenAI   — automatic caching, no write fee, read = 50% of input rate
  Anthropic — explicit cache_control, write = 25% extra, read = 10% of input rate
  Google    — explicit context caching, write = full rate, read = 25% of input rate

Add new models by appending entries to PRICING_TABLE.
The lookup always picks the entry with the latest `effective_from`
that is ≤ the call timestamp, so historical cost records stay accurate
when you update prices later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class ModelRates:
    provider: str
    model_id: str  # canonical model string as returned by the provider
    input_per_million: float  # USD / 1M input tokens
    output_per_million: float  # USD / 1M output tokens
    cache_write_per_million: float = 0.0  # USD / 1M tokens written to cache
    cache_read_per_million: float = 0.0  # USD / 1M tokens read from cache
    batch_discount: float = 0.5  # multiplier applied when is_batch=True (0.5 = 50% off)
    effective_from: datetime = field(
        default_factory=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc)
    )


# ---------------------------------------------------------------------------
# Pricing table — append new rows, never edit existing ones
# ---------------------------------------------------------------------------
PRICING_TABLE: list[ModelRates] = [
    # ── OpenAI ──────────────────────────────────────────────────────────────
    # cache_write = 0 (OpenAI caches automatically, no write charge)
    # cache_read  = 50% of input rate
    ModelRates(
        provider="openai",
        model_id="gpt-4o",
        input_per_million=2.50,
        output_per_million=10.00,
        cache_write_per_million=0.0,
        cache_read_per_million=1.25,
    ),
    ModelRates(
        provider="openai",
        model_id="gpt-4o-mini",
        input_per_million=0.15,
        output_per_million=0.60,
        cache_write_per_million=0.0,
        cache_read_per_million=0.075,
    ),
    ModelRates(
        provider="openai",
        model_id="gpt-4-turbo",
        input_per_million=10.00,
        output_per_million=30.00,
        cache_write_per_million=0.0,
        cache_read_per_million=5.00,
    ),
    ModelRates(
        provider="openai",
        model_id="gpt-4",
        input_per_million=30.00,
        output_per_million=60.00,
        cache_write_per_million=0.0,
        cache_read_per_million=0.0,
    ),
    ModelRates(
        provider="openai",
        model_id="gpt-3.5-turbo",
        input_per_million=0.50,
        output_per_million=1.50,
        cache_write_per_million=0.0,
        cache_read_per_million=0.0,
    ),
    ModelRates(
        provider="openai",
        model_id="o1",
        input_per_million=15.00,
        output_per_million=60.00,
        cache_write_per_million=0.0,
        cache_read_per_million=7.50,
    ),
    ModelRates(
        provider="openai",
        model_id="o1-mini",
        input_per_million=1.10,
        output_per_million=4.40,
        cache_write_per_million=0.0,
        cache_read_per_million=0.55,
    ),
    ModelRates(
        provider="openai",
        model_id="o3",
        input_per_million=10.00,
        output_per_million=40.00,
        cache_write_per_million=0.0,
        cache_read_per_million=2.50,
    ),
    ModelRates(
        provider="openai",
        model_id="o3-mini",
        input_per_million=1.10,
        output_per_million=4.40,
        cache_write_per_million=0.0,
        cache_read_per_million=0.55,
    ),
    ModelRates(
        provider="openai",
        model_id="o4-mini",
        input_per_million=1.10,
        output_per_million=4.40,
        cache_write_per_million=0.0,
        cache_read_per_million=0.275,
    ),
    # ── Anthropic ───────────────────────────────────────────────────────────
    # cache_write = 25% surcharge on input rate (1.25× input)
    # cache_read  = 10% of input rate (90% discount)
    ModelRates(
        provider="anthropic",
        model_id="claude-opus-4-8",
        input_per_million=15.00,
        output_per_million=75.00,
        cache_write_per_million=18.75,
        cache_read_per_million=1.50,
    ),
    ModelRates(
        provider="anthropic",
        model_id="claude-opus-4-7",
        input_per_million=15.00,
        output_per_million=75.00,
        cache_write_per_million=18.75,
        cache_read_per_million=1.50,
    ),
    ModelRates(
        provider="anthropic",
        model_id="claude-opus-4-6",
        input_per_million=15.00,
        output_per_million=75.00,
        cache_write_per_million=18.75,
        cache_read_per_million=1.50,
    ),
    ModelRates(
        provider="anthropic",
        model_id="claude-sonnet-4-6",
        input_per_million=3.00,
        output_per_million=15.00,
        cache_write_per_million=3.75,
        cache_read_per_million=0.30,
    ),
    ModelRates(
        provider="anthropic",
        model_id="claude-haiku-4-5-20251001",
        input_per_million=0.80,
        output_per_million=4.00,
        cache_write_per_million=1.00,
        cache_read_per_million=0.08,
    ),
    # Legacy models kept for historical cost accuracy
    ModelRates(
        provider="anthropic",
        model_id="claude-3-5-sonnet-20241022",
        input_per_million=3.00,
        output_per_million=15.00,
        cache_write_per_million=3.75,
        cache_read_per_million=0.30,
    ),
    ModelRates(
        provider="anthropic",
        model_id="claude-3-5-haiku-20241022",
        input_per_million=0.80,
        output_per_million=4.00,
        cache_write_per_million=1.00,
        cache_read_per_million=0.08,
    ),
    ModelRates(
        provider="anthropic",
        model_id="claude-3-opus-20240229",
        input_per_million=15.00,
        output_per_million=75.00,
        cache_write_per_million=18.75,
        cache_read_per_million=1.50,
    ),
    # ── Google ──────────────────────────────────────────────────────────────
    # cache_write = same as input rate (no surcharge)
    # cache_read  = 25% of input rate
    ModelRates(
        provider="google",
        model_id="gemini-2.5-pro",
        input_per_million=1.25,
        output_per_million=10.00,
        cache_write_per_million=1.25,
        cache_read_per_million=0.3125,
    ),
    ModelRates(
        provider="google",
        model_id="gemini-2.5-flash",
        input_per_million=0.15,
        output_per_million=0.60,
        cache_write_per_million=0.15,
        cache_read_per_million=0.0375,
    ),
    ModelRates(
        provider="google",
        model_id="gemini-1.5-pro",
        input_per_million=1.25,
        output_per_million=5.00,
        cache_write_per_million=1.25,
        cache_read_per_million=0.3125,
    ),
    ModelRates(
        provider="google",
        model_id="gemini-1.5-flash",
        input_per_million=0.075,
        output_per_million=0.30,
        cache_write_per_million=0.075,
        cache_read_per_million=0.01875,
    ),
    # ── Mistral ─────────────────────────────────────────────────────────────
    ModelRates(
        provider="mistral",
        model_id="mistral-large-latest",
        input_per_million=2.00,
        output_per_million=6.00,
    ),
    ModelRates(
        provider="mistral",
        model_id="mistral-small-latest",
        input_per_million=0.10,
        output_per_million=0.30,
    ),
    ModelRates(
        provider="mistral",
        model_id="codestral-latest",
        input_per_million=0.20,
        output_per_million=0.60,
    ),
    # ── Cohere ──────────────────────────────────────────────────────────────
    ModelRates(
        provider="cohere",
        model_id="command-r-plus",
        input_per_million=2.50,
        output_per_million=10.00,
    ),
    ModelRates(
        provider="cohere",
        model_id="command-r",
        input_per_million=0.15,
        output_per_million=0.60,
    ),
]


# ---------------------------------------------------------------------------
# Build a fast lookup index: (provider, model_id) → sorted list of ModelRates
# ---------------------------------------------------------------------------
from collections import defaultdict

_index: dict[tuple[str, str], list[ModelRates]] = defaultdict(list)
for _r in PRICING_TABLE:
    _index[(_r.provider.lower(), _r.model_id.lower())].append(_r)

# Sort each bucket newest-first for fast lookup
for _bucket in _index.values():
    _bucket.sort(key=lambda r: r.effective_from, reverse=True)


def get_rates(
    provider: str,
    model_id: str,
    at: Optional[datetime] = None,
) -> Optional[ModelRates]:
    """
    Return the ModelRates entry valid at `at` (defaults to now).
    Returns None if no matching entry is found.
    """
    if at is None:
        at = datetime.now(timezone.utc)

    bucket = _index.get((provider.lower(), model_id.lower()))
    if not bucket:
        # Try provider-only fuzzy match on model prefix
        # e.g. "gpt-4o-2024-11-20" should fall back to "gpt-4o"
        for (prov, mid), entries in _index.items():
            if prov == provider.lower() and model_id.lower().startswith(mid):
                bucket = entries
                break

    if not bucket:
        return None

    for rates in bucket:  # already sorted newest-first
        if rates.effective_from <= at:
            return rates

    return None


def infer_provider(model_id: str) -> Optional[str]:
    """
    Guess the provider from a model string when the caller doesn't supply one.
    Returns None if ambiguous.
    """
    m = model_id.lower()
    if m.startswith(("gpt-", "o1", "o3", "o4", "text-embedding", "dall-e")):
        return "openai"
    if m.startswith("claude"):
        return "anthropic"
    if m.startswith("gemini"):
        return "google"
    if m.startswith(("mistral", "codestral", "mixtral")):
        return "mistral"
    if m.startswith("command"):
        return "cohere"
    return None
