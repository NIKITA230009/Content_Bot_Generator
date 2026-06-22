import asyncio
import base64
import structlog
from telethon import TelegramClient, events

from config import config
from db import save_raw_post
from redis_storage import add_tokens, push_to_raw_stream
from media_aggregator import aggregate_media_message

logger = structlog.get_logger()

_client: TelegramClient | None = None
_source_cache: set[int] = set()
_source_cache_lock = asyncio.Lock()


def get_client() -> TelegramClient | None:
    return _client


async def refresh_source_cache():
    from db import get_all_bot_sources

    db_sources = await get_all_bot_sources()
    new_cache = set(config.SOURCE_TARGET_MAP.keys())
    for s in db_sources:
        if s.channel_id:
            new_cache.add(s.channel_id)
    async with _source_cache_lock:
        global _source_cache
        _source_cache = new_cache
    logger.info("source_cache_refreshed", count=len(_source_cache))


async def _process_message(msg, chat_id: int, aggregate: bool = True) -> bool:
    if not msg.text and not msg.photo and not msg.video:
        return False

    media = []
    if msg.photo:
        data = await msg.download_media(file=bytes)
        media.append({"type": "photo", "file_bytes64": base64.b64encode(data).decode()})
    if msg.video:
        data = await msg.download_media(file=bytes)
        media.append({"type": "video", "file_bytes64": base64.b64encode(data).decode()})

    msg_data = {
        "message_id": msg.id,
        "media_group_id": str(msg.grouped_id) if msg.grouped_id else None,
        "text": msg.text or "",
        "media": media,
    }

    if aggregate:
        post = await aggregate_media_message(msg_data, config.MEDIA_AGGREGATION_TIMEOUT)
    else:
        post = msg_data

    if post is None:
        return False

    post_id = await save_raw_post(
        source_channel_id=chat_id,
        message_id=post["message_id"],
        text=post.get("text", ""),
        media_group_id=post.get("media_group_id"),
        media=post.get("media", []),
    )
    if post_id:
        await push_to_raw_stream(post_id)
        await add_tokens(str(chat_id), 1)
        return True
    return False


async def fetch_historical_messages(channel_id: int, limit: int = 10):
    """Подтягивает последние N сообщений из канала и пускает в пайплайн."""
    global _client
    if _client is None:
        raise RuntimeError("Telethon client not started")

    messages = await _client.get_messages(channel_id, limit=limit)
    for msg in reversed(messages):
        await _process_message(msg, channel_id, aggregate=False)
        await asyncio.sleep(0.5)

    logger.info("backfill_complete", channel_id=channel_id, count=len(messages))


async def run_telethon_listener():
    global _client

    _client = TelegramClient(
        "user_session",
        config.TELETHON_API_ID,
        config.TELETHON_API_HASH,
        device_model="Desktop",
        system_version="macOS",
        app_version="1.0",
        lang_code="ru",
        system_lang_code="ru-RU",
    )

    await _client.start(phone=config.TELETHON_PHONE) # type: ignore
    await refresh_source_cache()

    @_client.on(events.NewMessage()) # type: ignore
    async def handler(event):
        async with _source_cache_lock:
            if event.chat_id not in _source_cache:
                return
        ok = await _process_message(event.message, event.chat_id)
        if ok:
            logger.info("telethon_raw_post_saved", message_id=event.message.id)
        else:
            logger.info("telethon_raw_post_skipped", message_id=event.message.id)

    await _client.run_until_disconnected() # type: ignore
    
    
