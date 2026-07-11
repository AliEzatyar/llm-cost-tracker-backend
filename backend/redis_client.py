# app/redis_client.py
import os
import redis.asyncio as redis
import logging


logger = logging.getLogger(__name__)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/10")
def create_redis_client() -> redis.Redis:
    return redis.from_url(
        REDIS_URL,
        decode_responses=True,
        max_connections=100,  # up from 50 — see note below
        socket_connect_timeout=2,
        socket_timeout=2,
        health_check_interval=30,
    )


async def _cache_get(redis_client: redis.Redis, key: str) -> str | None:
    """Treat any Redis failure as a cache miss rather than failing the request."""
    try:
        return await redis_client.get(key)
    except redis.RedisError:
        logger.warning(
            "redis get failed for key=%s, falling back to db", key, exc_info=True
        )
        return None


async def _cache_set(redis_client: redis.Redis, key: str, value, **kwargs) -> None:
    """Best-effort cache write — a failure here shouldn't fail a request that
    already has a correct answer from Postgres."""
    try:
        await redis_client.set(key, value, **kwargs)
    except redis.RedisError:
        logger.warning("redis set failed for key=%s", key, exc_info=True)
