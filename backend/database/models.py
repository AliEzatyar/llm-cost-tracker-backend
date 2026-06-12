# common models here to prevent circular imports
from datetime import datetime, timezone
import hashlib
from typing import Optional
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import (
    TIMESTAMP,
    Column,
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
    text,
)


class Base(DeclarativeBase):
    pass


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
    plan: str = Column(Text, nullable=False, default="free")  # free | pro | enterprise
    created_at: datetime = Column(DateTime(timezone=True), default=datetime.utcnow)

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

    @staticmethod
    def hash_key(raw_key: str) -> str:
        return hashlib.sha256(raw_key.encode()).hexdigest()
# ---------------------------------------------------------------------------
# Lightweight query helpers (used by services in main.py)
# ---------------------------------------------------------------------------


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


async def get_project_by_id(session: AsyncSession, project_id: str) -> "Project | None":
    result = await session.execute(
        text("SELECT * FROM projects WHERE id = :id"),
        {"id": project_id},
    )
    row = result.mappings().first()
    return dict(row) if row else None


async def get_project_by_owner_id(session: AsyncSession, owner_id: str) -> "Project | None":
    result = await session.execute(
        text("SELECT * FROM projects WHERE owner_id = :o"),
        {"o": owner_id},
    )
    row = result.mappings().first()
    return dict(row) if row else None


class CostEvent(Base):
    """
    Raw ingest table — every token-usage event written here immediately.
    Rolled up hourly into CustomerCostDaily.
    """

    __tablename__ = "cost_events"

    id: str = Column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    project_id: str = Column(
        UUID(as_uuid=False),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tenant_id: str = Column(
        Text, nullable=False, index=True
    )  # your end-user / tenant ID
    model: str = Column(Text, nullable=False)
    input_tokens: int = Column(Integer, nullable=False, default=0)
    output_tokens: int = Column(Integer, nullable=False, default=0)
    cost_usd: float = Column(Numeric(12, 8), nullable=False)
    created_at: datetime = Column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
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


class Threshold(Base):
    """
    Per-customer spend cap.  AlertService checks these every minute.
    """

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
    created_at: datetime = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: datetime = Column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("project_id", "tenant_id", name="uq_threshold_pc"),
    )

    project = relationship("Project", back_populates="thresholds")


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
        DateTime(timezone=True), default=datetime.utcnow, index=True
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
