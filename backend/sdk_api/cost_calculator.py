"""
cost_calculator.py
------------------
Pure-function cost engine. No DB, no side-effects.
Accepts a raw usage dict (as returned by any provider SDK)
and returns a structured CostBreakdown.

DB-backed pricing
-----------------
Pass `db_rates` (a dict with the column values from the model_pricing table)
to override the static PRICING_TABLE for this call.  If `db_rates` is None
(or the field isn't found in the static table) the engine falls back to the
compiled-in PRICING_TABLE so existing behaviour is fully preserved.

db_rates expected keys (all numeric):
    input_per_million, output_per_million,
    cache_write_per_million, cache_read_per_million,
    batch_discount
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .pricing import ModelRates, get_rates, infer_provider


@dataclass
class CostBreakdown:
    # Component costs in USD
    input_cost: float
    output_cost: float
    cache_write_cost: float
    cache_read_cost: float
    total_cost: float

    # Metadata
    provider: str
    model_id: str
    rates_used: Optional[ModelRates]
    pricing_found: bool  # False = no rates found, cost = 0.0
    batch_applied: bool
    rates_source: str  # "db" | "static" | "none"

    def as_dict(self) -> dict:
        return {
            "total_cost_usd": round(self.total_cost, 10),
            "input_cost_usd": round(self.input_cost, 10),
            "output_cost_usd": round(self.output_cost, 10),
            "cache_write_cost_usd": round(self.cache_write_cost, 10),
            "cache_read_cost_usd": round(self.cache_read_cost, 10),
            "pricing_found": self.pricing_found,
            "batch_applied": self.batch_applied,
            "provider_resolved": self.provider,
            "model_resolved": self.model_id,
            "rates_source": self.rates_source,
        }


def _rates_from_db(db_rates: dict) -> ModelRates:
    """Wrap a DB row dict in a ModelRates dataclass for uniform downstream use."""
    return ModelRates(
        provider="db",
        model_id="db",
        input_per_million=float(db_rates["input_per_million"]),
        output_per_million=float(db_rates["output_per_million"]),
        cache_write_per_million=float(db_rates.get("cache_write_per_million", 0)),
        cache_read_per_million=float(db_rates.get("cache_read_per_million", 0)),
        batch_discount=float(db_rates.get("batch_discount", 0.5)),
    )


def calculate_cost(
    *,
    model: str,
    input_tokens: int,
    output_tokens: int,
    provider: Optional[str] = None,
    cached_tokens: int = 0,  # OpenAI: tokens served from cache (sub-field of input)
    cache_read_tokens: int = 0,  # Anthropic/Google: cache_read_input_tokens
    cache_write_tokens: int = 0,  # Anthropic/Google: cache_creation_input_tokens
    is_batch: bool = False,
    at: Optional[datetime] = None,
    db_rates: Optional[
        dict
    ] = None,  # live rates from model_pricing table; overrides static
) -> CostBreakdown:
    """
    Compute a full cost breakdown for a single LLM API call.

    Token field mapping by provider
    ────────────────────────────────
    OpenAI
      input_tokens   = usage.prompt_tokens
      cached_tokens  = usage.prompt_tokens_details.cached_tokens   (sub-field of input)
      output_tokens  = usage.completion_tokens

    Anthropic
      input_tokens        = usage.input_tokens
      cache_write_tokens  = usage.cache_creation_input_tokens
      cache_read_tokens   = usage.cache_read_input_tokens
      output_tokens       = usage.output_tokens

    Google
      input_tokens       = usageMetadata.promptTokenCount
      cache_read_tokens  = usageMetadata.cachedContentTokenCount
      output_tokens      = usageMetadata.candidatesTokenCount

    DB rates
    ────────
    When `db_rates` is supplied it takes priority over the static PRICING_TABLE.
    The static table is still consulted as a fallback when db_rates is None.
    """
    if at is None:
        at = datetime.now(timezone.utc)

    # ── 1. Resolve provider ──────────────────────────────────────────────────

    # ── 2. Determine rates source ─────────────────────────────────────────────
    #   Priority: db_rates → static pricing table → nothing (cost = 0)
    rates_source: str
    rates: Optional[ModelRates]

    if db_rates is not None:
        rates = _rates_from_db(db_rates)
        rates_source = "db"
    else:
        rates = get_rates(provider, model, at=at)
        rates_source = "static" if rates is not None else "none"

    if rates is None:
        return CostBreakdown(
            input_cost=0.0,
            output_cost=0.0,
            cache_write_cost=0.0,
            cache_read_cost=0.0,
            total_cost=0.0,
            provider=provider,
            model_id=model,
            rates_used=None,
            pricing_found=False,
            batch_applied=False,
            rates_source="none",
        )

    M = 1_000_000
    batch_mult = rates.batch_discount if is_batch else 1.0

    # ── 3. Resolve cache token fields by provider convention ──────────────────
    #
    # OpenAI: `cached_tokens` is a sub-field of input_tokens (already included).
    #   Fresh input = input_tokens - cached_tokens  (billed at full rate)
    #   Cached input = cached_tokens                (billed at cache_read rate)
    #   No separate cache_write billing.
    #
    # Anthropic / Google: cache_write and cache_read are separate token pools,
    #   NOT included in input_tokens.
    #
    if provider == "openai":
        fresh_input = max(0, input_tokens - cached_tokens)
        effective_cache_read = cached_tokens
        effective_cache_write = 0
    else:
        fresh_input = input_tokens
        effective_cache_read = cache_read_tokens
        effective_cache_write = cache_write_tokens

    # ── 4. Compute each component ────────────────────────────────────────────
    input_cost = (fresh_input / M) * rates.input_per_million * batch_mult
    output_cost = (output_tokens / M) * rates.output_per_million * batch_mult
    cache_write_cost = (effective_cache_write / M) * rates.cache_write_per_million
    cache_read_cost = (effective_cache_read / M) * rates.cache_read_per_million

    total = input_cost + output_cost + cache_write_cost + cache_read_cost

    return CostBreakdown(
        input_cost=input_cost,
        output_cost=output_cost,
        cache_write_cost=cache_write_cost,
        cache_read_cost=cache_read_cost,
        total_cost=total,
        provider=provider,
        model_id=model,
        rates_used=rates,
        pricing_found=True,
        batch_applied=is_batch,
        rates_source=rates_source,
    )
