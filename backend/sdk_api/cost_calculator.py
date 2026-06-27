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
    batch_discount,
    audio_billing,          speech_billing,         image_billing,
    audio_input_per_million, audio_output_per_million,
    image_input_per_million, image_output_per_million,
    per_minute_rate, per_1k_chars_rate, per_image_rate
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
    audio_cost: float  # per-minute OR per audio-token, depending on model
    speech_cost: float  # per-character OR per token (audio out), depending on model
    image_cost: float  # per-image OR per image-token, depending on model
    total_cost: float

    # Billing paths actually taken (for observability / debugging)
    audio_billing: str  # "per_minute" | "per_token"
    speech_billing: str  # "per_character" | "per_token"
    image_billing: str  # "per_image" | "per_token"

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
            "audio_cost_usd": round(self.audio_cost, 10),
            "speech_cost_usd": round(self.speech_cost, 10),
            "image_cost_usd": round(self.image_cost, 10),
            "audio_billing": self.audio_billing,
            "speech_billing": self.speech_billing,
            "image_billing": self.image_billing,
            "pricing_found": self.pricing_found,
            "batch_applied": self.batch_applied,
            "provider_resolved": self.provider,
            "model_resolved": self.model_id,
            "rates_source": self.rates_source,
        }


def _rates_from_db(db_rates: dict) -> ModelRates:
    """Wrap a DB row dict in a ModelRates dataclass for uniform downstream use."""

    def _f(key: str, default: float = 0.0) -> float:
        """Coalesce both missing-key and SQL NULL to `default` before casting."""
        v = db_rates.get(key)
        return float(v) if v is not None else default

    def _s(key: str, default: str) -> str:
        v = db_rates.get(key)
        return str(v) if v is not None else default

    return ModelRates(
        provider="db",
        model_id="db",
        input_per_million=_f("input_per_million"),
        output_per_million=_f("output_per_million"),
        cache_write_per_million=_f("cache_write_per_million"),
        cache_read_per_million=_f("cache_read_per_million"),
        batch_discount=_f("batch_discount", 0.5),
        # billing mode flags
        audio_billing=_s("audio_billing", "per_minute"),
        speech_billing=_s("speech_billing", "per_character"),
        # modality-specific token rates
        audio_input_per_million=_f("audio_input_per_million"),
        audio_output_per_million=_f("audio_output_per_million"),
        # image token rates (gpt-image-1 family — five dimensions, always per-token)
        image_text_input_per_million=_f("image_text_input_per_million"),
        image_cached_text_input_per_million=_f("image_cached_text_input_per_million"),
        image_input_per_million=_f("image_input_per_million"),
        image_cached_input_per_million=_f("image_cached_input_per_million"),
        image_output_per_million=_f("image_output_per_million"),
        # legacy flat rates
        per_minute_rate=_f("per_minute_rate"),
        per_1k_chars_rate=_f("per_1k_chars_rate"),
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
    # ── audio token fields (gpt-4o-transcribe per-token, gpt-4o-realtime) ────
    audio_input_tokens: int = 0,  # usage.input_token_details.audio_tokens
    audio_output_tokens: int = 0,  # usage.output_token_details.audio_tokens
    # ── audio flat field (whisper-1, gpt-4o-transcribe per-minute) ────────────
    duration_seconds: float = 0.0,
    # ── speech flat field (tts-1, tts-1-hd) ──────────────────────────────────
    character_count: int = 0,
    # ── image token fields (gpt-image-1, gpt-image-1-mini, gpt-image-1.5, gpt-image-2) ──
    input_text_tokens: int = 0,  # usage.input_tokens_details.text_tokens
    cached_input_text_tokens: int = 0,  # usage.input_tokens_details.cached_tokens
    input_image_tokens: int = 0,  # usage.input_tokens_details.image_tokens  (edit only)
    cached_input_image_tokens: int = 0,  # usage.input_tokens_details.cached_image_tokens
    output_image_tokens: int = 0,  # usage.output_tokens
    # ─────────────────────────────────────────────────────────────────────────
    is_batch: bool = False,
    at: Optional[datetime] = None,
    db_rates: Optional[dict] = None,  # live rates from model_pricing; overrides static
) -> CostBreakdown:
    """
    Compute a full cost breakdown for a single LLM API call.

    Token field mapping by provider
    ────────────────────────────────
    OpenAI chat/completions
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

    Audio billing — two paths, selected by rates.audio_billing
    ───────────────────────────────────────────────────────────
    "per_token"   gpt-4o-transcribe (token mode), gpt-4o-realtime
                    audio_input_tokens  = usage.input_token_details.audio_tokens
                    audio_output_tokens = usage.output_token_details.audio_tokens
                    cost = audio_input_tokens  * audio_input_per_million / M
                         + audio_output_tokens * audio_output_per_million / M

    "per_minute"  whisper-1, gpt-4o-transcribe (simple billing)
                    duration_seconds supplied by extractor
                    cost = duration_seconds / 60 * per_minute_rate

    Speech billing — two paths, selected by rates.speech_billing
    ─────────────────────────────────────────────────────────────
    "per_token"   gpt-4o-mini-tts
                    input_tokens        = text + instruction tokens (already in input_tokens)
                    audio_output_tokens = generated audio tokens
                    cost = input_tokens        * input_per_million / M
                         + audio_output_tokens * audio_output_per_million / M
                    (input_cost and audio_cost absorb these; speech_cost = 0)

    "per_character"  tts-1, tts-1-hd
                    character_count = len(kwargs["input"])
                    cost = character_count / 1000 * per_1k_chars_rate

    Image billing — token-only (gpt-image-* models)
    ─────────────────────────────────────────────────
    All gpt-image-* models are billed across five token dimensions:

      input_text_tokens         Text tokens in the prompt
                                  → usage.input_tokens_details.text_tokens
      cached_input_text_tokens  Prompt text tokens served from cache
                                  → usage.input_tokens_details.cached_tokens
      input_image_tokens        Image tokens from reference images (edit only)
                                  → usage.input_tokens_details.image_tokens
      cached_input_image_tokens Reference-image tokens served from cache
                                  → usage.input_tokens_details.cached_image_tokens
      output_image_tokens       Tokens for the generated image(s)
                                  → usage.output_tokens

    Cost formula:
      image_cost = input_text_tokens          * image_text_input_per_million / M
                 + cached_input_text_tokens   * image_cached_text_input_per_million / M
                 + input_image_tokens         * image_input_per_million / M
                 + cached_input_image_tokens  * image_cached_input_per_million / M
                 + output_image_tokens        * image_output_per_million / M

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
            audio_cost=0.0,
            speech_cost=0.0,
            image_cost=0.0,
            total_cost=0.0,
            audio_billing="per_minute",
            speech_billing="per_character",
            image_billing="per_token",
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

    # ── 4. Text token costs ──────────────────────────────────────────────────
    input_cost = (fresh_input / M) * rates.input_per_million * batch_mult
    output_cost = (output_tokens / M) * rates.output_per_million * batch_mult
    cache_write_cost = (effective_cache_write / M) * rates.cache_write_per_million
    cache_read_cost = (effective_cache_read / M) * rates.cache_read_per_million

    # ── 5. Audio cost ─────────────────────────────────────────────────────────
    _audio_billing = getattr(rates, "audio_billing", "per_minute")
    if _audio_billing == "per_token":
        audio_cost = (audio_input_tokens / M) * rates.audio_input_per_million + (
            audio_output_tokens / M
        ) * rates.audio_output_per_million
    else:  # "per_minute" — whisper-1, gpt-4o-transcribe simple billing
        audio_cost = (duration_seconds / 60.0) * rates.per_minute_rate

    # ── 6. Speech / TTS cost ─────────────────────────────────────────────────
    # "per_token" models (gpt-4o-mini-tts): text input tokens are already counted
    # in input_cost; audio output tokens are already counted in audio_cost above
    # (the extractor routes them through audio_output_tokens).  speech_cost = 0
    # so the total stays correct without double-counting.
    _speech_billing = getattr(rates, "speech_billing", "per_character")
    if _speech_billing == "per_token":
        speech_cost = 0.0  # absorbed into input_cost + audio_cost
    else:  # "per_character" — tts-1, tts-1-hd
        speech_cost = (character_count / 1_000.0) * rates.per_1k_chars_rate

    # ── 7. Image cost (gpt-image-* token billing) ────────────────────────────
    #
    # All supported image models use per-token billing across five dimensions.
    # Each dimension may carry its own rate in ModelRates:
    #
    #   image_text_input_per_million        rate for plain-text prompt tokens
    #   image_cached_text_input_per_million rate for cached text prompt tokens
    #   image_input_per_million             rate for reference-image tokens (edit)
    #   image_cached_input_per_million      rate for cached reference-image tokens
    #   image_output_per_million            rate for generated-image tokens
    #
    # Rates that are not set on a model default to 0.0, so unused dimensions
    # silently contribute nothing to the total.
    _image_billing = getattr(rates, "image_billing", "per_token")
    image_cost = (
        (input_text_tokens / M) * getattr(rates, "image_text_input_per_million", 0.0)
        + (cached_input_text_tokens / M)
        * getattr(rates, "image_cached_text_input_per_million", 0.0)
        + (input_image_tokens / M) * getattr(rates, "image_input_per_million", 0.0)
        + (cached_input_image_tokens / M)
        * getattr(rates, "image_cached_input_per_million", 0.0)
        + (output_image_tokens / M) * getattr(rates, "image_output_per_million", 0.0)
    )

    total = (
        input_cost
        + output_cost
        + cache_write_cost
        + cache_read_cost
        + audio_cost
        + speech_cost
        + image_cost
    )

    return CostBreakdown(
        input_cost=input_cost,
        output_cost=output_cost,
        cache_write_cost=cache_write_cost,
        cache_read_cost=cache_read_cost,
        audio_cost=audio_cost,
        speech_cost=speech_cost,
        image_cost=image_cost,
        total_cost=total,
        audio_billing=_audio_billing,
        speech_billing=_speech_billing,
        image_billing=_image_billing,
        provider=provider,
        model_id=model,
        rates_used=rates,
        pricing_found=True,
        batch_applied=is_batch,
        rates_source=rates_source,
    )
