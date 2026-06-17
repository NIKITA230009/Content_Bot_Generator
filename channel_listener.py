import structlog
from aiogram import Router, F
from aiogram.types import Message

from config import config
from db import save_raw_post
from redis_storage import add_tokens, push_to_raw_stream
from media_aggregator import aggregate_media_message

logger = structlog.get_logger()
router = Router()


@router.channel_post(F.chat.id.in_(config.SOURCE_TARGET_MAP.keys()))
async def handle_channel_post(msg: Message):
    post = await aggregate_media_message(msg, config.MEDIA_AGGREGATION_TIMEOUT)
    if post is None:
        return

    post_id = await save_raw_post(
        source_channel_id=msg.chat.id,
        message_id=post["message_id"],
        text=post.get("text", ""),
        media_group_id=post.get("media_group_id"),
        media=post.get("media", []),
    )
    if post_id:
        await push_to_raw_stream(post_id)
        logger.info("raw_post_saved", post_id=post_id, channel_id=msg.chat.id)
        await add_tokens(str(msg.chat.id),1)
    else:
        logger.info("raw_post_duplicate", message_id=post["message_id"])
