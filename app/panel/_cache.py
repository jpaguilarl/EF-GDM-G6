import time
from functools import wraps


def ttl_cache(ttl_seconds: int = 300, maxsize: int = 128):
    """Simple TTL cache decorator — no external dependencies."""
    cache: dict = {}
    timestamps: dict = {}

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(sorted(kwargs.items()))
            now = time.monotonic()
            if key in cache and now - timestamps.get(key, 0) < ttl_seconds:
                return cache[key]
            result = func(*args, **kwargs)
            if len(cache) >= maxsize:
                oldest = min(timestamps, key=timestamps.get)
                del cache[oldest]
                del timestamps[oldest]
            cache[key] = result
            timestamps[key] = now
            return result
        return wrapper
    return decorator
