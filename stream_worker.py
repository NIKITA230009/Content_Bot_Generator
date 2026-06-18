import asyncio
import os
import socket
import structlog

from redis import asyncio as aioredis

from config import config
from redis_storage import (
    get_redis, reset_redis, ensure_stream_group, push_to_dead_letter,
    increment_retry, reset_retry,
)

logger = structlog.get_logger()


async def stream_worker(
    stream: str,
    group: str,
    process_func,
    poll_timeout: int = 30,
):
    logger.info("worker_setup_start", stream=stream, group=group)
    r = await get_redis()
    logger.info("worker_redis_ok", stream=stream)
    consumer = f"{socket.gethostname()}-{os.getpid()}"
    await ensure_stream_group(stream, group)
    logger.info("worker_group_ok", stream=stream, group=group)

    check_backlog = True

    async def _heartbeat():
        while True:
            await asyncio.sleep(10)
            logger.info("worker_busy", stream=stream, item=item)

    while True:
        logger.info("worker_loop_iteration", stream=stream, group=group)
        item = None
        msg_id = None
        try:
            myid = "0" if check_backlog else ">"
            result = await r.xreadgroup(
                group, consumer, {stream: myid},
                count=5,
            )

            if not result:
                if check_backlog:
                    check_backlog = False
                await asyncio.sleep(3)
                continue

            _, messages = result[0]
            if not messages:
                if check_backlog:
                    check_backlog = False
                await asyncio.sleep(3)
                continue

            for msg_id, msg_data in messages:
                item = msg_data.get("item")
                logger.info("stream_item_received", stream=stream, item=item, msg_id=msg_id)

                heartbeat = asyncio.create_task(_heartbeat())
                try:
                    await process_func(item)
                finally:
                    heartbeat.cancel()

                await r.xack(stream, group, msg_id)
                await reset_retry(stream, item)
                check_backlog = False
                logger.info("worker_done_item", stream=stream, item=item)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("worker_error", stream=stream, item=item, error=str(e))
            if item and msg_id:
                retries = await increment_retry(stream, item)
                await r.xack(stream, group, msg_id)
                if retries >= config.MAX_RETRIES:
                    await push_to_dead_letter(stream, item, str(e), msg_id)
                    await reset_retry(stream, item)
                    logger.warning("item_to_dead_letter", stream=stream, item=item)
                else:
                    await r.xadd(stream, {"item": item})
                    logger.info("item_requeued", stream=stream, item=item, retry=retries)
            await asyncio.sleep(5)
            await reset_redis()
            r = await get_redis()


async def _claim_stuck(
    stream: str, group: str, consumer: str,
    process_func, r: aioredis.Redis, min_idle_ms: int = 300000,
):
    cursor = "0-0"
    while cursor:
        result = await r.xautoclaim(stream, group, consumer, min_idle_ms, cursor, count=10)
        cursor = result[0]
        for msg_id, msg_data in result[1]:
            item = msg_data.get("item")
            try:
                await process_func(item)
                await r.xack(stream, group, msg_id)
                await reset_retry(stream, item)
                logger.info("claimed_and_processed", stream=stream, item=item, msg_id=msg_id)
            except Exception as e:
                retries = await increment_retry(stream, item)
                await r.xack(stream, group, msg_id)
                if retries >= config.MAX_RETRIES:
                    await push_to_dead_letter(stream, item, str(e), msg_id)
                    await reset_retry(stream, item)
                    logger.warning("dead_letter_claimed", stream=stream, item=item)
                else:
                    await r.xadd(stream, {"item": item})
                    logger.info("claimed_and_requeued", stream=stream, item=item, retry=retries)
