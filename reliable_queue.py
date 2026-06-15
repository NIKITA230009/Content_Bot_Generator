import asyncio
import structlog

from redis_storage import (
    brpoplpush_from_queue,
    remove_from_processing,
)

logger = structlog.get_logger()


async def reliable_worker(
    queue: str,
    process_func,
    poll_timeout: int = 30,
):
    processing_queue = f"{queue}:processing"
    while True:
        item: str | None = None
        try:
            item = await brpoplpush_from_queue(queue, processing_queue, timeout=poll_timeout)
            if item is None:
                continue
            logger.info("worker_got_item", queue=queue, item=item)
            await process_func(item)
            await remove_from_processing(processing_queue, item)
            logger.info("worker_done_item", queue=queue, item=item)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("worker_error", queue=queue, item=item, error=str(e))
            if item:
                await remove_from_processing(processing_queue, item)
            await asyncio.sleep(5)
