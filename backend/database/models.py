# common models here to prevent circular imports
from datetime import datetime, timezone
import hashlib
from typing import Optional
import uuid
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import (
    TIMESTAMP,
    Column,
    Index,
    String,
    Text,
    Numeric,
    UUID,
    Integer,
    DateTime,
    Date,
    ForeignKey,
    UniqueConstraint,
    Boolean,
    func,
    select,
    text,
)

SYSTEM_DEFINED_MODEL = "SYSTEM"
USER_DEFINED_MODEL = "USER"

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Project and query helpers (used by services in main.py)
# ---------------------------------------------------------------------------
class Project(Base):
    """
    One row per SaaS customer / API key holder.
    api_key_hash stores sha256(raw_api_key) — never the raw key.
    """

    __tablename__ = "projects"

    id: str = Column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: str = Column(Text, nullable=False)
    api_key_hash: str = Column(Text, nullable=False, unique=True, index=True)
    plan: str = Column(
        Text, nullable=False, default="free"
    )  # see backend/billing/plans.py PLANS keys: free | starter | growth | scale | enterprise
    created_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc)
    )

    # relationships
    cost_events = relationship("CostEvent", back_populates="project", lazy="noload")
    cost_daily = relationship(
        "CustomerCostDaily", back_populates="project", lazy="noload"
    )
    thresholds = relationship("Threshold", back_populates="project", lazy="noload")
    alerts = relationship("Alert", back_populates="project", lazy="noload")
    owner_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    owner: Mapped[Optional["User"]] = relationship("User", back_populates="projects")
    grace_period_started_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    is_blocked: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)

    @staticmethod
    def hash_key(raw_key: str) -> str:
        return hashlib.sha256(raw_key.encode()).hexdigest()


async def get_project_by_api_key(
    session: AsyncSession, raw_key: str
) -> "Project | None":
    key_hash = Project.hash_key(raw_key)
    result = await session.execute(
        text("SELECT * FROM projects WHERE api_key_hash = :h"),
        {"h": key_hash},
    )
    row = result.mappings().first()
    return dict(row) if row else None


async def get_current_project(
    session: AsyncSession, request: Request
) -> "Project | None":
    project_id = request.headers.get("X-Project-ID")
    result = await session.execute(
        select(Project).where(
            Project.id == project_id,
        )
    )
    return result.scalar_one_or_none()


async def get_project_by_id(session: AsyncSession, project_id: str) -> "Project | None":
    result = await session.execute(
        select(Project).where(
            Project.id == project_id,
        )
    )
    return result.scalar_one_or_none()


async def get_free_plan_project(
    session: AsyncSession, owner_id: str
) -> "Project | None":
    result = await session.execute(
        select(Project).where(Project.owner_id == owner_id, Project.plan == "free")
    )
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Cost models
# ---------------------------------------------------------------------------
class CostEvent(Base):
    """
    Raw ingest table — every token-usage event written here immediately.
    Rolled up hourly into CustomerCostDaily.

    Column groups
    ─────────────
    Core            provider, endpoint, model, cost_usd, created_at
    Text tokens     input_tokens, cached_tokens, output_tokens, total_tokens
                    (OpenAI: cached_tokens is a sub-field of input_tokens,
                     i.e. fresh = input_tokens − cached_tokens)
    Audio tokens    audio_input_tokens, audio_output_tokens
                    (gpt-4o-transcribe token mode, gpt-4o-realtime)
    Audio flat      duration_seconds   (whisper-1 per-minute billing)
    Speech flat     character_count    (tts-1 / tts-1-hd per-character billing)
    Image tokens    input_text_tokens, cached_input_text_tokens,
                    input_image_tokens, cached_input_image_tokens,
                    output_image_tokens
                    (gpt-image-1 / 1-mini / 1.5 / 2 — five billing dimensions)
    Image audit     image_count        (number of images generated, for audit)
    """

    __tablename__ = "cost_events"

    # ── identity ──────────────────────────────────────────────────────────────
    id: str = Column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    project_id: str = Column(
        UUID(as_uuid=False),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tenant_id: str = Column(Text, nullable=True, index=True)

    # ── call metadata ─────────────────────────────────────────────────────────
    provider: str = Column(Text, nullable=True)  # "openai", "anthropic", …
    endpoint: str = Column(
        Text, nullable=True
    )  # "chat.completions", "images.generate", …
    model: str = Column(Text, nullable=True)

    # ── text token columns ────────────────────────────────────────────────────
    input_tokens: int = Column(Integer, nullable=True, default=0)
    cached_tokens: int = Column(Integer, nullable=True, default=0)
    output_tokens: int = Column(Integer, nullable=True, default=0)
    total_tokens: int = Column(Integer, nullable=True, default=0)

    # ── audio token columns (gpt-4o-transcribe token mode, gpt-4o-realtime) ──
    audio_input_tokens: int = Column(Integer, nullable=True, default=0)
    audio_output_tokens: int = Column(Integer, nullable=True, default=0)

    # ── audio flat billing (whisper-1) ────────────────────────────────────────
    duration_seconds: float = Column(Numeric(10, 4), nullable=True, default=0)

    # ── speech flat billing (tts-1, tts-1-hd) ────────────────────────────────
    character_count: int = Column(Integer, nullable=True, default=0)

    # ── image token columns (gpt-image-1, gpt-image-1-mini, gpt-image-1.5, gpt-image-2) ──
    input_text_tokens: int = Column(Integer, nullable=True, default=0)
    cached_input_text_tokens: int = Column(Integer, nullable=True, default=0)
    input_image_tokens: int = Column(Integer, nullable=True, default=0)
    cached_input_image_tokens: int = Column(Integer, nullable=True, default=0)
    output_image_tokens: int = Column(Integer, nullable=True, default=0)

    # ── image audit ───────────────────────────────────────────────────────────
    image_count: int = Column(Integer, nullable=True, default=0)

    # ── cost ──────────────────────────────────────────────────────────────────
    cost_usd: float = Column(Numeric(12, 8), nullable=True)

    # ── timestamp ─────────────────────────────────────────────────────────────
    created_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc), index=True
    )

    project = relationship("Project", back_populates="cost_events")


class CustomerCostDaily(Base):
    """
    Hourly rollup — pre-aggregated per (project, customer, date).
    Used for fast dashboard queries; built by the background worker.
    """

    __tablename__ = "customer_cost_daily"

    id: str = Column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    project_id: str = Column(
        UUID(as_uuid=False),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tenant_id: str = Column(Text, nullable=False, index=True)
    date: Date = Column(Date, nullable=False, index=True)
    total_cost_usd: float = Column(Numeric(12, 6), nullable=False, default=0)
    total_calls: int = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint("project_id", "tenant_id", "date", name="uq_cost_daily_pcd"),
    )

    project = relationship("Project", back_populates="cost_daily")


# ---------------------------------------------------------------------------
# threshold and alert models
# ---------------------------------------------------------------------------
class Threshold(Base):
    """
    Per-customer spend cap.  AlertService checks these every minute.
    """

    PER_DAY = "per_day"
    PER_MONTH = "per_month"
    __tablename__ = "thresholds"

    id: str = Column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    project_id: str = Column(
        UUID(as_uuid=False),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tenant_id: str = Column(Text, nullable=False)
    limit_usd: float = Column(Numeric(12, 4), nullable=False)
    created_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc)
    )
    updated_at: datetime = Column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
    )
    frequency: str = Column(Text, nullable=True, default=PER_MONTH)
    effective_date: datetime = Column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
        nullable=True,
    )
    __table_args__ = (
        UniqueConstraint("project_id", "tenant_id", name="uq_threshold_pc"),
    )

    project = relationship("Project", back_populates="thresholds")
    def to_dict(self) -> dict:
        """Converts the Threshold model instance into a dictionary."""
        return {
            "id": self.id,
            "project_id": self.project_id,
            "tenant_id": self.tenant_id,
            "limit_usd": float(self.limit_usd) if self.limit_usd is not None else None,
            "frequency": self.frequency,
            "effective_date": self.effective_date.isoformat() if self.effective_date else None,
        }


class Alert(Base):
    """
    Fired when a customer's cumulative cost exceeds their threshold.
    One row per breach event (not deduplicated — use triggered_at for that).
    """

    __tablename__ = "alerts"

    id: str = Column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    project_id: str = Column(
        UUID(as_uuid=False),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tenant_id: str = Column(Text, nullable=False, index=True)
    triggered_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc), index=True
    )
    cost_at_trigger: float = Column(Numeric(12, 6), nullable=False)

    project = relationship("Project", back_populates="alerts")


# ---------------------------------------------------------------------------
# Auth models
# ---------------------------------------------------------------------------
class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(
        String(320),  # RFC 5321 max email length
        nullable=False,
        unique=True,
        index=True,
    )
    hashed_password: Mapped[str] = mapped_column(Text, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # 2. Enforce timezone=True here too!
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    #  FIXED: Added (timezone=True)
    last_login: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    # A user can own many projects
    projects: Mapped[list["Project"]] = relationship(
        "Project",
        back_populates="owner",
        cascade="all, delete-orphan",
    )
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)
    otp_hash = Column(String, nullable=True)
    otp_expires_at  = Column(DateTime(timezone=True), nullable=True)

# ---------------------------------------------------------------------------
# LLM models pricing
# ---------------------------------------------------------------------------
class ModelPricing(Base):
    """
    DB-backed pricing registry — mirrors pricing.py's ModelRates dataclass.

    Rates are stored as USD per 1,000,000 tokens (per-million), matching the
    convention used throughout the cost_calculator.

    Price history is preserved by inserting NEW rows with a later effective_from
    rather than updating existing ones. The cost engine always picks the row
    with the latest effective_from that is <= NOW().

    Cache rate conventions (same as pricing.py):
        OpenAI     — cache_write=0 (automatic), cache_read=50% of input
        Anthropic  — cache_write=125% of input, cache_read=10% of input
        Google     — cache_write=input rate,    cache_read=25% of input

    Billing mode flags
    ──────────────────
    audio_billing:
        "per_minute"  — whisper-1, gpt-4o-transcribe (simple)  → per_minute_rate
        "per_token"   — gpt-4o-transcribe (token mode), gpt-4o-realtime
                        → audio_input_per_million / audio_output_per_million

    speech_billing:
        "per_character" — tts-1, tts-1-hd  → per_1k_chars_rate
        "per_token"     — gpt-4o-mini-tts  → input_per_million (text in) +
                          audio_output_per_million (audio out); speech_cost = 0,
                          cost absorbed by input_cost + audio_cost

    Image pricing (gpt-image-1, gpt-image-1-mini, gpt-image-1.5, gpt-image-2)
    is always token-based across five dimensions — there is no flat
    per-image billing mode in the cost engine:
        image_text_input_per_million         plain text prompt tokens
        image_cached_text_input_per_million  cached text prompt tokens
        image_input_per_million              reference-image tokens (edit)
        image_cached_input_per_million       cached reference-image tokens
        image_output_per_million             generated-image tokens
    Unused dimensions for a given model are simply left at 0.
    """

    __tablename__ = "model_pricing"

    id: str = Column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    provider: str = Column(Text, nullable=False)  # openai | anthropic | google …
    model_id: str = Column(Text, nullable=False)  # canonical model string

    # ── Text token rates (all models) ─────────────────────────────────────────
    input_per_million: float = Column(Numeric(14, 6), nullable=False)
    output_per_million: float = Column(Numeric(14, 6), nullable=False)
    cache_write_per_million: float = Column(Numeric(14, 6), nullable=True, default=0)
    cache_read_per_million: float = Column(Numeric(14, 6), nullable=True, default=0)

    # ── Billing mode flags ────────────────────────────────────────────────────
    audio_billing: str = Column(
        Text, nullable=True, default="per_minute"
    )  # "per_minute" | "per_token"
    speech_billing: str = Column(
        Text, nullable=True, default="per_character"
    )  # "per_character" | "per_token"

    # ── Modality-specific token rates ─────────────────────────────────────────
    # Audio: gpt-4o-transcribe (token mode), gpt-4o-realtime, gpt-4o-mini-tts
    audio_input_per_million: float = Column(Numeric(14, 6), nullable=True, default=0)
    audio_output_per_million: float = Column(Numeric(14, 6), nullable=True, default=0)

    # Images: gpt-image-1 family — five token dimensions, always per-token
    image_text_input_per_million: float = Column(
        Numeric(14, 6), nullable=True, default=0
    )
    image_cached_text_input_per_million: float = Column(
        Numeric(14, 6), nullable=True, default=0
    )
    image_input_per_million: float = Column(Numeric(14, 6), nullable=True, default=0)
    image_cached_input_per_million: float = Column(
        Numeric(14, 6), nullable=True, default=0
    )
    image_output_per_million: float = Column(Numeric(14, 6), nullable=True, default=0)

    # ── Legacy flat rates ─────────────────────────────────────────────────────
    # Audio transcription / translation: USD per minute (whisper-1, gpt-4o-transcribe simple)
    per_minute_rate: float = Column(Numeric(14, 6), nullable=True, default=0)
    # TTS: USD per 1 000 characters (tts-1, tts-1-hd)
    per_1k_chars_rate: float = Column(Numeric(14, 6), nullable=True, default=0)

    # Multiplier applied when is_batch=True (0.5 = 50% off)
    batch_discount: float = Column(Numeric(5, 4), nullable=True, default=0.5)

    # A later effective_from = new price period; old rows stay for audit history
    effective_from: datetime = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    created_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc)
    )
    updated_at: datetime = Column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
    )
    model_type = Column(
        String(100),  # <-- FIX: Define the explicit database type here
        nullable=False,
        default=SYSTEM_DEFINED_MODEL,
    )
    author_id : str = Column(
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
    ) 

    __table_args__ = (
        # USER pricing
        UniqueConstraint(
            "provider",
            "model_id",
            "model_type",
            "author_id",
            name="uq_model_pricing_user",
        ),
        # SYSTEM pricing
        Index(
            "uq_model_pricing_system",
            "provider",
            "model_id",
            unique=True,
            postgresql_where=(model_type == "SYSTEM"),
        ),
    )
    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "provider": self.provider,
            "model_id": self.model_id,
            # text token rates
            "input_per_million": float(self.input_per_million),
            "output_per_million": float(self.output_per_million),
            "cache_write_per_million": float(self.cache_write_per_million),
            "cache_read_per_million": float(self.cache_read_per_million),
            # billing mode flags
            "audio_billing": self.audio_billing,
            "speech_billing": self.speech_billing,
            # modality-specific token rates
            "audio_input_per_million": float(self.audio_input_per_million),
            "audio_output_per_million": float(self.audio_output_per_million),
            "image_text_input_per_million": float(self.image_text_input_per_million),
            "image_cached_text_input_per_million": float(
                self.image_cached_text_input_per_million
            ),
            "image_input_per_million": float(self.image_input_per_million),
            "image_cached_input_per_million": float(
                self.image_cached_input_per_million
            ),
            "image_output_per_million": float(self.image_output_per_million),
            # legacy flat rates
            "per_minute_rate": float(self.per_minute_rate),
            "per_1k_chars_rate": float(self.per_1k_chars_rate),
            "batch_discount": float(self.batch_discount),
            "model_type": str(self.model_type),
            "author_id" : str(self.author_id),
            "effective_from": (
                self.effective_from.isoformat() if self.effective_from else None
            ),
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


# ---------------------------------------------------------------------------
# Pricing query helper
# ---------------------------------------------------------------------------
async def get_model_pricing(
    session: AsyncSession,
    provider: str,
    model_id: str,
) -> "dict | None":
    """
    Return the most recent pricing row for (provider, model_id) whose
    effective_from is <= NOW(). Returns None when not found — callers
    should fall back to the static pricing.py PRICING_TABLE.
    """
    result = await session.execute(
        text("""
            SELECT
                input_per_million,
                output_per_million,
                cache_write_per_million,
                cache_read_per_million,
                audio_billing,
                speech_billing,
                image_billing,
                audio_input_per_million,
                audio_output_per_million,
                image_input_per_million,
                image_output_per_million,
                per_minute_rate,
                per_1k_chars_rate,
                per_image_rate,
                batch_discount
            FROM model_pricing
            WHERE  provider        = :provider
              AND  model_id        = :model_id
              AND  effective_from <= NOW()
            ORDER  BY effective_from DESC
            LIMIT  1
        """),
        {"provider": provider, "model_id": model_id},
    )
    row = result.mappings().first()
    return dict(row) if row else None


class RefreshToken(Base):
    """
    Stored refresh tokens — lets us invalidate sessions on logout.
    One row per active session; delete the row to revoke.
    """

    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    # 1. Enforce timezone=True here!
    expires_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    # 2. Enforce timezone=True here too!
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    revoked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    user: Mapped["User"] = relationship("User")


# --------------------------------------------------------------------------
# Billing and subscription
# --------------------------------------------------------------------------


class Subscription(Base):
    """
    One active subscription per project.
    Stripe customer + subscription data cached here so we avoid
    round-tripping to Stripe on every request.
    """

    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,  # one subscription per project
        index=True,
    )

    # -------------- Stripe identifiers
    stripe_customer_id: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True, index=True
    )
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, unique=True, index=True
    )
    stripe_price_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Plan mirrored from projects.plan for fast reads
    plan: Mapped[str] = mapped_column(Text, nullable=False, default="free")

    # Subscription lifecycle
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="active",
        # active | past_due | canceled | trialing | incomplete
    )
    current_period_start: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    current_period_end: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    cancel_at_period_end: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    usage_records: Mapped[list["MonthlyUsageSnapshot"]] = relationship(
        "MonthlyUsageSnapshot", back_populates="subscription", lazy="noload"
    )


class MonthlyUsageSnapshot(Base):
    """
    Rolling monthly tracked-spend total per subscription.
    Reset at the start of each billing period.
    Used to enforce plan limits without hitting CustomerCostDaily every request.
    """

    __tablename__ = "monthly_usage_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # Billing period this snapshot covers
    period_start: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    period_end: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    tracked_spend_usd: Mapped[float] = mapped_column(
        Numeric(14, 6), nullable=False, default=0.0
    )
    # Set True when spend > plan limit (after grace period → hard block)
    limit_exceeded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    grace_period_started_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "subscription_id", "period_start", name="uq_usage_snapshot_sub_period"
        ),
    )

    subscription: Mapped["Subscription"] = relationship(
        "Subscription", back_populates="usage_records"
    )


class BillingEvent(Base):
    """
    Immutable audit log — every Stripe webhook event stored here.
    Idempotency key = stripe_event_id.
    """

    __tablename__ = "billing_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    stripe_event_id: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True, index=True
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)  # e.g. invoice.paid
    project_id: Mapped[Optional[str]] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("projects.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    payload: Mapped[str] = mapped_column(Text, nullable=False)  # raw JSON
    processed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
