import asyncio
import structlog

from config import config
from db import init_db
from telethon_listener import run_telethon_listener
from content_generator import run_generator_worker
from publisher import run_publisher_worker

logger = structlog.get_logger()


async def main():
    if not config.BOT_TOKEN:
        logger.error("BOT_TOKEN not set")
        return

    await init_db()
    logger.info("database_initialized")

    logger.info(
        "content_bot_starting",
        sources=list(config.SOURCE_TARGET_MAP.keys()),
        targets=list(config.PUBLISH_INTERVALS.keys()),
    )

    async with asyncio.TaskGroup() as tg:
        async def _wrap(name, coro):
            logger.info("task_running", name=name)
            try:
                await coro
            except Exception as e:
                logger.exception("task_crashed", name=name, error=str(e))
                raise

        logger.info("task_started", name="telethon")
        tg.create_task(_wrap("telethon", run_telethon_listener()))
        logger.info("task_started", name="generator")
        tg.create_task(_wrap("generator", run_generator_worker()))
        logger.info("task_started", name="publisher")
        tg.create_task(_wrap("publisher", run_publisher_worker()))


if __name__ == "__main__":
    asyncio.run(main())
