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
    pool_size=10,
    max_overflow=20,
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

