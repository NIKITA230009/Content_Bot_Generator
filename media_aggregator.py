import asyncio
import structlog
from aiogram.types import Message

from redis_storage import add_media_group_part, get_media_group

logger = structlog.get_logger()

_media_futures: dict[str, asyncio.Future] = {}


async def aggregate_media_message(msg: Message, timeout: int) -> dict | None:
    if not msg.media_group_id:
        return _build_post_dict(msg)

    group_id = msg.media_group_id
    part = _extract_part(msg)
    is_first = await add_media_group_part(group_id, part, timeout)

    if is_first:
        fut = asyncio.get_event_loop().create_future()
        _media_futures[group_id] = fut
        asyncio.create_task(_aggregation_timer(group_id, timeout, fut))
        logger.info("media_group_started", group_id=group_id, timeout=timeout)
        return await fut

    existing = _media_futures.get(group_id)
    if existing and not existing.done():
        return await existing

    return None


async def _aggregation_timer(group_id: str, timeout: int, fut: asyncio.Future):
    try:
        await asyncio.sleep(timeout)
        parts = await get_media_group(group_id)
        if parts:
            merged = _merge_parts(parts)
            fut.set_result(merged)
            logger.info("media_group_assembled", group_id=group_id, parts=len(parts))
        else:
            fut.set_result(None)
    except Exception as e:
        if not fut.done():
            fut.set_exception(e)
    finally:
        _media_futures.pop(group_id, None)


def _extract_part(msg: Message) -> dict:
    media = []
    if msg.photo:
        media.append({"type": "photo", "file_id": msg.photo[-1].file_id})
    if msg.video:
        media.append({"type": "video", "file_id": msg.video.file_id})
    return {
        "message_id": msg.message_id,
        "text": msg.caption or "",
        "media": media,
    }


def _build_post_dict(msg: Message) -> dict:
    media = []
    if msg.photo:
        media.append({"type": "photo", "file_id": msg.photo[-1].file_id})
    if msg.video:
        media.append({"type": "video", "file_id": msg.video.file_id})
    return {
        "message_id": msg.message_id,
        "media_group_id": None,
        "text": msg.caption or "",
        "media": media,
    }


def _merge_parts(parts: list[dict]) -> dict:
    all_media = []
    text = ""
    for p in parts:
        all_media.extend(p.get("media", []))
        if p.get("text"):
            text = p["text"] or text
    return {
        "message_id": parts[0]["message_id"],
        "media_group_id": parts[0].get("media_group_id"),
        "text": text,
        "media": all_media,
    }
