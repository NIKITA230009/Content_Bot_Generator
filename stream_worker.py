import asyncio
import os
import socket
import structlog

from config import config
from redis_storage import (
    get_redis, ensure_stream_group, push_to_dead_letter,
    increment_retry, reset_retry,
)

logger = structlog.get_logger()


async def stream_worker(
    stream: str,
    group: str,
    process_func,
    poll_timeout: int = 30,
):
    r = await get_redis()
    consumer = f"{socket.gethostname()}-{os.getpid()}"
    await ensure_stream_group(stream, group)

    check_backlog = True

    while True:
        item = None
        msg_id = None
        try:
            myid = "0" if check_backlog else ">"
            result = await r.xreadgroup(
                group, consumer, {stream: myid},
                count=1, block=poll_timeout * 1000,
            )

            if not result:
                if check_backlog:
                    check_backlog = False
                    continue
                await _claim_stuck(stream, group, consumer, process_func, r)
                continue

            _, messages = result[0]
            msg_id, msg_data = messages[0]
            item = msg_data.get("item")

            await process_func(item)
            await r.xack(stream, group, msg_id)
            await reset_retry(stream, msg_id)
            check_backlog = False
            logger.info("worker_done_item", stream=stream, item=item)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("worker_error", stream=stream, item=item, error=str(e))
            if item and msg_id:
                retries = await increment_retry(stream, msg_id)
                await r.xack(stream, group, msg_id)
                if retries >= config.MAX_RETRIES:
                    await push_to_dead_letter(stream, item, str(e), msg_id)
                    await reset_retry(stream, msg_id)
                    logger.warning("item_to_dead_letter", stream=stream, item=item)
                else:
                    await r.xadd(stream, {"item": item})
                    logger.info("item_requeued", stream=stream, item=item, retry=retries)
            await asyncio.sleep(5)


async def _claim_stuck(stream, group, consumer, process_func, r, min_idle_ms=300000):
    cursor = "0-0"
    while cursor:
        result = await r.xautoclaim(stream, group, consumer, min_idle_ms, cursor, count=10)
        cursor = result[0]
        for msg_id, msg_data in result[1]:
            item = msg_data.get("item")
            try:
                await process_func(item)
                await r.xack(stream, group, msg_id)
                await reset_retry(stream, msg_id)
                logger.info("claimed_and_processed", stream=stream, item=item, msg_id=msg_id)
            except Exception as e:
                retries = await increment_retry(stream, msg_id)
                await r.xack(stream, group, msg_id)
                if retries >= config.MAX_RETRIES:
                    await push_to_dead_letter(stream, item, str(e), msg_id)
                    await reset_retry(stream, msg_id)
                    logger.warning("dead_letter_claimed", stream=stream, item=item)
                else:
                    await r.xadd(stream, {"item": item})
                    logger.info("claimed_and_requeued", stream=stream, item=item, retry=retries)
