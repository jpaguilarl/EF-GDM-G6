import redis.asyncio as aioredis


class RedisClient:
    """Async Redis client (redis-py asyncio). Singleton per FastAPI app."""

    def __init__(self, url: str, ttl_hours: int = 48):
        self._url = url
        self._ttl = ttl_hours * 3600
        self._redis: aioredis.Redis | None = None

    async def connect(self):
        self._redis = aioredis.Redis.from_url(self._url, decode_responses=True)

    async def close(self):
        if self._redis:
            await self._redis.aclose()

    @property
    def redis(self) -> aioredis.Redis:
        assert self._redis, "Redis not connected"
        return self._redis
