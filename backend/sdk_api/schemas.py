# ===========================================================================
# Pydantic schemas
# ===========================================================================


from pydantic import BaseModel
from typing import Any
from pydantic import BaseModel, Field

class CostEventIn(BaseModel):
    """
    Inbound event payload produced by the SDK patch and posted to the ingest
    endpoint.  Every field defaults to its zero value so callers only need to
    populate the fields that are non-zero for a given endpoint / billing mode.

    Billing mode guide
    ──────────────────
    Chat / Completions / Responses / Embeddings
        provider, endpoint, model
        input_tokens, cached_tokens, output_tokens, total_tokens

    Audio — whisper-1 (per-minute)
        + duration_seconds

    Audio — gpt-4o-transcribe token mode, gpt-4o-realtime
        + audio_input_tokens, audio_output_tokens

    Speech — tts-1, tts-1-hd (per-character)
        + character_count

    Speech — gpt-4o-mini-tts (per-token)
        input_tokens and audio_output_tokens carry the billing; no extra field

    Image — gpt-image-1 / 1-mini / 1.5 / 2 (per-token, five dimensions)
        + input_text_tokens, cached_input_text_tokens
        + input_image_tokens, cached_input_image_tokens  (edit only)
        + output_image_tokens
        + image_count  (audit only, not used for billing)
    """

    # ── call metadata ─────────────────────────────────────────────────────────
    provider: str
    endpoint: str
    tenant_id: str
    model: str

    # ── text tokens ───────────────────────────────────────────────────────────
    input_tokens: int = Field(default=0, ge=0)
    cached_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)

    # ── audio tokens (gpt-4o-transcribe token mode, gpt-4o-realtime) ─────────
    audio_input_tokens: int = Field(default=0, ge=0)
    audio_output_tokens: int = Field(default=0, ge=0)

    # ── audio flat billing (whisper-1) ────────────────────────────────────────
    duration_seconds: float = Field(default=0.0, ge=0.0)

    # ── speech flat billing (tts-1, tts-1-hd) ────────────────────────────────
    character_count: int = Field(default=0, ge=0)

    # ── image tokens (gpt-image-* five billing dimensions) ───────────────────
    input_text_tokens: int = Field(default=0, ge=0)
    cached_input_text_tokens: int = Field(default=0, ge=0)
    input_image_tokens: int = Field(default=0, ge=0)
    cached_input_image_tokens: int = Field(default=0, ge=0)
    output_image_tokens: int = Field(default=0, ge=0)

    # ── image audit ───────────────────────────────────────────────────────────
    image_count: int = Field(default=0, ge=0)


class CostEventBatch(BaseModel):
    events: list[CostEventIn]


class ThresholdIn(BaseModel):
    tenant_id: str
    limit_usd: float = Field(gt=0)
    project_id: str

