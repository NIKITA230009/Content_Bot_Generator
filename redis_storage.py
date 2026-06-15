import json
import redis.asyncio as aioredis
from config import config

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis


# ── Raw queue ────────────────────────────────────────────


async def push_to_raw_queue(post_id: int):
    r = await get_redis()
    await r.lpush("queue:raw", str(post_id))


async def push_to_ready_queue(content_id: int):
    r = await get_redis()
    await r.lpush("queue:ready", str(content_id))


async def brpoplpush_from_queue(queue: str, processing_queue: str, timeout: int) -> str | None:
    r = await get_redis()
    return await r.brpoplpush(queue, processing_queue, timeout)


async def remove_from_processing(queue: str, item: str):
    r = await get_redis()
    await r.lrem(queue, 0, item)


async def get_processing_items(queue: str) -> list[str]:
    r = await get_redis()
    return await r.lrange(queue, 0, -1)


async def return_to_queue(queue: str, item: str):
    r = await get_redis()
    await r.rpush(queue, item)


async def processing_queue_len(queue: str) -> int:
    r = await get_redis()
    return await r.llen(queue)


# ── Media group aggregation ──────────────────────────────


async def add_media_group_part(group_id: str, part_data: dict, timeout: int) -> bool:
    r = await get_redis()
    key = f"media_group:{group_id}"
    await r.rpush(key, json.dumps(part_data, ensure_ascii=False))
    was_first = await r.llen(key) == 1
    if was_first:
        await r.expire(key, timeout + 10)
    return was_first


async def get_media_group(group_id: str) -> list[dict]:
    r = await get_redis()
    raw = await r.lrange(f"media_group:{group_id}", 0, -1)
    await r.delete(f"media_group:{group_id}")
    return [json.loads(x) for x in raw]


# ── Locks ────────────────────────────────────────────────


async def acquire_lock(name: str, ttl: int) -> bool:
    r = await get_redis()
    return bool(await r.set(f"lock:{name}", "1", nx=True, ex=ttl))


async def release_lock(name: str):
    r = await get_redis()
    await r.delete(f"lock:{name}")


# ── In-progress guard ────────────────────────────────────


async def is_in_progress(post_id: int) -> bool:
    r = await get_redis()
    return bool(await r.exists(f"in_progress:{post_id}"))


async def mark_in_progress(post_id: int, ttl: int = 300):
    r = await get_redis()
    await r.setex(f"in_progress:{post_id}", ttl, "1")


async def clear_in_progress(post_id: int):
    r = await get_redis()
    await r.delete(f"in_progress:{post_id}")


# ── Publish intervals ────────────────────────────────────


async def get_last_publish_time(channel_id: int) -> float:
    r = await get_redis()
    val = await r.get(f"last_publish:{channel_id}")
    return float(val) if val else 0.0


async def set_last_publish_time(channel_id: int, timestamp: float):
    r = await get_redis()
    await r.set(f"last_publish:{channel_id}", str(timestamp))
