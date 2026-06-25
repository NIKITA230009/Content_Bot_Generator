import asyncio
import base64
import httpx
import structlog
from google.cloud import vision

logger = structlog.get_logger()

_vision_sem = asyncio.Semaphore(2)
_http_client: httpx.AsyncClient | None = None


def _get_http() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=15.0)
    return _http_client


def _find_similar_sync(image_data: bytes) -> str | None:
    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=image_data)
    response = client.web_detection(image=image)
    if response.web_detection.visually_similar_images:
        return response.web_detection.visually_similar_images[0].url
    return None


async def find_similar_images(media: list) -> list:
    if not media:
        return media
    result = []
    for item in media:
        if item.get("type") != "photo":
            result.append(item)
            continue
        original_b64 = item.get("file_bytes64")
        if not original_b64:
            result.append(item)
            continue
        similar_url = None
        try:
            async with _vision_sem:
                logger.info("vision_web_detection_start")
                similar_url = await asyncio.to_thread(
                    _find_similar_sync, base64.b64decode(original_b64))
        except Exception as e:
            logger.exception("vision_web_detection_failed", error=str(e))
        if similar_url:
            try:
                http = _get_http()
                resp = await http.get(similar_url)
                if resp.status_code == 200:
                    result.append({
                        "type": "photo",
                        "file_bytes64": base64.b64encode(resp.content).decode()
                    })
                    logger.info("image_search_replaced", url=similar_url)
                    continue
            except Exception as e:
                logger.warning("image_search_download_failed",
                               url=similar_url, error=str(e))
        result.append(item)
        logger.info("image_search_keep_original")
    return result
