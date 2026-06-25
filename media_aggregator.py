import asyncio
import structlog

from redis_storage import add_media_group_part, get_media_group

logger = structlog.get_logger() 

_media_futures: dict[str, asyncio.Future] = {}

async def aggregate_media_message(msg_data: dict, timeout: int) -> dict | None:
    if not msg_data.get("media_group_id"):
        logger.info("single_post_no_aggregation", message_id=msg_data["message_id"])
        return msg_data

    group_id = msg_data["media_group_id"]
    part = {
        "message_id": msg_data["message_id"],
        "text": msg_data.get("text", ""),
        "media": msg_data.get("media", []),
    }
    is_first = await add_media_group_part(group_id, part, timeout)

    if is_first:
        fut = asyncio.get_running_loop().create_future()
        _media_futures[group_id] = fut
        asyncio.create_task(_aggregation_timer(group_id, timeout))
        logger.info("media_group_started", group_id=group_id, timeout=timeout)
        return await fut

    return None

async def _aggregation_timer(group_id: str, timeout: int):
    try:
        await asyncio.sleep(timeout)
        parts = await get_media_group(group_id)
        fut = _media_futures.get(group_id)
        if fut is None or fut.done():
            return
        if parts:
            merged = _merge_parts(group_id, parts)
            fut.set_result(merged)
            logger.info("media_group_assembled", group_id=group_id, parts=len(parts))
        else:
            fut.set_result(None)
    except Exception as e:
        fut = _media_futures.get(group_id)
        if fut and not fut.done():
            fut.set_exception(e)
    finally:
        _media_futures.pop(group_id, None)


def _merge_parts(group_id: str, parts: list[dict]) -> dict:
    all_media = []
    text = ""
    for p in parts:
        all_media.extend(p.get("media", []))
        if p.get("text"):
            text = p["text"] or text
    return {
        "message_id": parts[0]["message_id"],
        "media_group_id": group_id,
        "text": text,
        "media": all_media,
    }
