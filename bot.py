import asyncio
import logging

import structlog
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties

from config import config
from db import init_db
from channel_listener import router
from content_generator import run_generator_worker
from publisher import run_publisher_worker

logger = structlog.get_logger()


async def main():
    if not config.BOT_TOKEN:
        logger.error("BOT_TOKEN not set")
        return

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))

    dp = Dispatcher()
    dp.include_router(router)

    await init_db()
    logger.info("database_initialized")

    logger.info(
        "content_bot_starting",
        sources=list(config.SOURCE_TARGET_MAP.keys()),
        targets=list(config.PUBLISH_INTERVALS.keys()),
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(dp.start_polling(bot))
        tg.create_task(run_generator_worker())
        tg.create_task(run_publisher_worker())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(main())
