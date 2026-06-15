import asyncio
import time
import structlog
from aiogram import Bot
from aiogram.types import InputMediaPhoto, InputMediaVideo
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import config
from db import get_generated_content, log_publication, is_already_published
from redis_storage import (
    get_last_publish_time, set_last_publish_time,
    remove_from_processing,
)
from reliable_queue import reliable_worker

logger = structlog.get_logger()

_bot: Bot | None = None


def init_bot(token: str):
    global _bot
    _bot = Bot(token=token, default=DefaultBotProperties(parse_mode=None))
    return _bot


async def process_content(content_id_str: str):
    content_id = int(content_id_str)
    content = await get_generated_content(content_id)
    if not content:
        logger.warning("generated_content_not_found", content_id=content_id)
        return

    source_id = content["source_channel_id"]
    target_ids = config.SOURCE_TARGET_MAP.get(source_id, [])
    if not target_ids:
        logger.warning("no_target_channels", source_id=source_id)
        return

    for target_id in target_ids:
        if await is_already_published(content_id, target_id):
            logger.info("already_published", content_id=content_id, target_id=target_id)
            continue

        last_time = await get_last_publish_time(target_id)
        interval = config.PUBLISH_INTERVALS.get(target_id, 300)
        wait = interval - (time.time() - last_time)
        if wait > 0:
            logger.info("waiting_interval", target_id=target_id, seconds=round(wait))
            await asyncio.sleep(wait)

        try:
            msg = await _send_to_channel(target_id, content)
            msg_id = msg[0].message_id if isinstance(msg, list) else msg.message_id
            await log_publication(content_id, target_id, msg_id, True, None)
            await set_last_publish_time(target_id, time.time())
            logger.info("published", content_id=content_id, target_id=target_id)
        except Exception as e:
            await log_publication(content_id, target_id, None, False, str(e))
            logger.error("publish_error", content_id=content_id, target_id=target_id, error=str(e))

    await remove_from_processing("queue:ready:processing", content_id_str)


async def _send_to_channel(channel_id: int, content: dict):
    bot = _bot
    if not bot:
        raise RuntimeError("Bot not initialized")

    media = content.get("media") or []
    text = content.get("rewritten_text") or ""

    if media:
        group = []
        for i, m in enumerate(media):
            if m["type"] == "photo":
                inp = InputMediaPhoto(media=m["file_id"])
            else:
                inp = InputMediaVideo(media=m["file_id"])
            if i == 0:
                inp.caption = text
            group.append(inp)
        return await bot.send_media_group(channel_id, group)

    return await bot.send_message(channel_id, text)


async def run_publisher_worker():
    await reliable_worker("queue:ready", process_content)
