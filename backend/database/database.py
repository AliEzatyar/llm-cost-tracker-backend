"""
database.py — Layer 4: Storage
PostgreSQL schema definitions, async connection pool, and helpers.

Dependencies:
    pip install asyncpg sqlalchemy[asyncio] alembic python-dotenv
"""

import os
from typing import AsyncGenerator
from dotenv import load_dotenv

from .models import Base
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/costtracker",
)

engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    # Sized for: N_workers * (pool_size + max_overflow) + worker-process pool
    # must stay under Postgres max_connections. On a t3a.small running
    # Postgres + Redis + app together, each idle connection also costs real
    # RAM. With 2-4 Uvicorn workers + the separate cost_event_worker.py
    # process, worst case is now ~(4*8)+8 = 40, comfortably under the
    # default max_connections=100. Only raise this after confirming
    # connections are actually being held for their real query duration
    # (see ingest_cost_event's short-lived rate_db session) — a bigger
    # pool does not fix connections held open too long, only delays when
    # you notice.
    pool_size=10,
    max_overflow=20,
    pool_timeout=10,  # fail fast with a clear TimeoutError instead of hanging
    pool_pre_ping=True,
    echo=bool(os.getenv("SQL_ECHO", "")),
)
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields a DB session and closes it afterward."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------


async def create_all_tables() -> None:
    """
    Creates all tables that don't yet exist.
    Call once at application startup (or use Alembic for migrations).
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_all_tables() -> None:
    """Tear down — useful in tests."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
