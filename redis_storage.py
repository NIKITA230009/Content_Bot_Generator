import json
import redis.asyncio as aioredis
from config import config

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis


# ── Streams ────────────────────────────────────────────────


async def push_to_raw_stream(post_id: int):
    r = await get_redis()
    await r.xadd("stream:raw", {"item": str(post_id)})


async def push_to_ready_stream(content_id: int):
    r = await get_redis()
    await r.xadd("stream:ready", {"item": str(content_id)})


async def ensure_stream_group(stream: str, group: str):
    r = await get_redis()
    try:
        await r.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


async def push_to_dead_letter(stream: str, item: str, error: str, msg_id: str):
    r = await get_redis()
    await r.xadd(f"{stream}:dead", {
        "item": item,
        "error": error,
        "original_msg_id": msg_id,
    })


async def increment_retry(stream: str, msg_id: str, ttl: int = 86400) -> int:
    r = await get_redis()
    key = f"retry:{stream}:{msg_id}"
    count = await r.incr(key)
    await r.expire(key, ttl)
    return count


async def reset_retry(stream: str, msg_id: str):
    r = await get_redis()
    await r.delete(f"retry:{stream}:{msg_id}")


# ── Media group aggregation with redis lists ──────────────────────────────


async def add_media_group_part(group_id: str, part_data: dict, timeout: int) -> bool:
    r = await get_redis()
    key = f"media_group:{group_id}"
    await r.rpush(key, json.dumps(part_data, ensure_ascii=False))  # type: ignore[arg-type]
    was_first = await r.llen(key) == 1  # type: ignore[arg-type]
    if was_first:
        await r.expire(key, timeout + 10)
    return was_first


async def get_media_group(group_id: str) -> list[dict]:
    r = await get_redis()
    raw = await r.lrange(f"media_group:{group_id}", 0, -1)  # type: ignore[arg-type]
    await r.delete(f"media_group:{group_id}")
    return [json.loads(x) for x in raw]


# ── Publish intervals ────────────────────────────────────


async def get_last_publish_time(channel_id: int) -> float:
    r = await get_redis()
    val = await r.get(f"last_publish:{channel_id}")
    return float(val) if val else 0.0


async def set_last_publish_time(channel_id: int, timestamp: float):
    r = await get_redis()
    await r.set(f"last_publish:{channel_id}", str(timestamp))


# ── Tokens ────────────────────────────────────────────────

_TOKEN_TTL = 86400


async def get_tokens(user_id: str) -> int:
    r = await get_redis()
    val = await r.get(f"tokens:{user_id}")
    return int(val) if val else 0


async def add_tokens(user_id: str, delta: int) -> None:
    r = await get_redis()
    await r.incrby(f"tokens:{user_id}", delta)
    await r.expire(f"tokens:{user_id}", _TOKEN_TTL)


async def delete_tokens(user_id: str) -> None:
    r = await get_redis()
    await r.delete(f"tokens:{user_id}")
