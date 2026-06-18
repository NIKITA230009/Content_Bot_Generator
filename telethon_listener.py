import base64
import structlog
from telethon import TelegramClient, events

from config import config
from db import save_raw_post
from redis_storage import add_tokens, push_to_raw_stream
from media_aggregator import aggregate_media_message

logger = structlog.get_logger()


async def run_telethon_listener():
    source_ids = list(config.SOURCE_TARGET_MAP.keys())

    client = TelegramClient(
        "user_session",
        config.TELETHON_API_ID,
        config.TELETHON_API_HASH,
        device_model="Desktop",
        system_version="macOS",
        app_version="1.0",
        lang_code="ru",
        system_lang_code="ru-RU",
    )

    await client.start(phone=config.TELETHON_PHONE)
    logger.info("telethon_started", channels=source_ids)

    @client.on(events.NewMessage(chats=source_ids))
    async def handler(event):
        msg = event.message
        logger.info("telethon_message_received", chat_id=event.chat_id, message_id=msg.id)

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

        post = await aggregate_media_message(msg_data, config.MEDIA_AGGREGATION_TIMEOUT)
        if post is None:
            return

        post_id = await save_raw_post(
            source_channel_id=event.chat_id,
            message_id=post["message_id"],
            text=post.get("text", ""),
            media_group_id=post.get("media_group_id"),
            media=post.get("media", []),
        )
        if post_id:
            await push_to_raw_stream(post_id)
            await add_tokens(str(event.chat_id), 1)
            logger.info("telethon_raw_post_saved", post_id=post_id)
        else:
            logger.info("telethon_raw_post_duplicate", message_id=post["message_id"])

    await client.run_until_disconnected()
