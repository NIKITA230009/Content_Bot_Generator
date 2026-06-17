import asyncio
import time
import structlog
from aiogram import Bot
from aiogram.types import InputMediaPhoto, InputMediaVideo
from aiogram.client.default import DefaultBotProperties
from config import config
from db import GeneratedContent, get_generated_content, log_publication, is_already_published
from redis_storage import get_last_publish_time, set_last_publish_time
from stream_worker import stream_worker

logger = structlog.get_logger()

_bot: Bot | None = None


async def process_content(content_id_str: str):
    content_id = int(content_id_str)
    content = await get_generated_content(content_id)
    if not content:
        logger.warning("generated_content_not_found", content_id=content_id)
        return

    source_id = content.raw_post.source_channel_id
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
            msg_id = await _send_to_channel(target_id, content)
            await log_publication(content_id, target_id, msg_id, True, None)
            await set_last_publish_time(target_id, time.time())
            logger.info("published", content_id=content_id, target_id=target_id)
        except Exception as e:
            await log_publication(content_id, target_id, None, False, str(e))
            logger.error("publish_error", content_id=content_id, target_id=target_id, error=str(e))

async def _send_to_channel(channel_id: int, content: GeneratedContent) -> int:
    global _bot
    if _bot is None:
        _bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
    bot = _bot

    media = content.raw_post.media or []
    text = content.rewritten_text or ""

    if media:
        group: list[InputMediaPhoto | InputMediaVideo] = []
        for i, m in enumerate(media):
            if m["type"] == "photo":
                inp = InputMediaPhoto(media=m["file_id"])
            else:
                inp = InputMediaVideo(media=m["file_id"])
            if i == 0:
                inp.caption = text  # type: ignore[arg-type]
            group.append(inp)
        msgs = await bot.send_media_group(channel_id, group)  # type: ignore[arg-type]
        return msgs[0].message_id

    msg = await bot.send_message(channel_id, text)  # type: ignore[arg-type]
    return msg.message_id


async def run_publisher_worker():
    await stream_worker("stream:ready", "publishers", process_content)
