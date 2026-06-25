import asyncio
import functools
import re
import structlog
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_google_genai import ChatGoogleGenerativeAI, Modality

from config import config
from db import get_raw_post_by_id, save_generated_content, mark_raw_post_processed, get_bot_source_by_channel_id, update_raw_post_regenerated_media
from redis_storage import push_to_raw_stream, push_to_ready_stream, push_to_moderation_stream
from stream_worker import stream_worker
from image_search import find_similar_images

logger = structlog.get_logger()

# ── LLM setup (copied from support bot pattern) ─────────

model = ChatOpenAI(
    model=config.MODEL_NAME,
    temperature=config.MODEL_TEMPERATURE,
    max_tokens=config.MODEL_MAX_TOKENS, # type: ignore
    max_retries=config.LLM_MAX_RETRIES,
    api_key=config.OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
    default_headers={
        "HTTP-Referer": "https://aretora.ru",
        "X-Title": "Content Repost Bot",
    },
)

_llm_sem = asyncio.Semaphore(3)

agent = create_agent(model=model)

image_model = ChatGoogleGenerativeAI(
    model="gemini-3.1-flash-image-preview",
    response_modalities=[Modality.IMAGE],
    vertexai=True,
    project=config.VERTEX_PROJECT_ID,
)

image_agent = create_agent(model=image_model)

image_sem = asyncio.Semaphore(2)

_RE_MARKDOWN = [
    # (r'\*\*(.+?)\*\*', r'\1'),  — **жирный** оставляем для рендеринга в канале
    (r'\*(.+?)\*', r'\1'),
    (r'```(.+?)```', r'\1'),
    (r'`(.+?)`', r'\1'),
    (r'(?m)^#+\s*', ''),
    (r'(?m)^>\s*', ''),
    (r'!?\[(.+?)\]\(.+?\)', r'\1'),
    (r'(?m)^[-*+]\s+', ''),
    (r'_{2,}', ''),
    (r'~~(.+?)~~', r'\1'),
]

_REWRITE_PROMPT = """Ты — редактор Telegram-канала ARETORA VPN. Перепиши входящий текст так, чтобы он звучал живо, разнообразно и в едином фирменном стиле.

Главное правило:
Ты не придумываешь новости с нуля. Берёшь факты из исходного текста и излагаешь их от лица ARETORA VPN.

1. ЗАМЕНА БРЕНДА И ССЫЛОК
Любое упоминание другого VPN-сервиса, бота или приложения (No Propaganda, Happ, v2rayNG и т.п.) замени на «ARETORA VPN».
Ссылки на ботов, сайты и кнопки удаляй. Вместо ссылки на бот используй @aretora_vpn_bot в тексте.

2. ЯЗЫК И ТОН
Обращайся к читателю строго на «ты» (никакого «вы»).
Короткие, рубленые предложения. Убирай канцелярит и вводные слова.
Стиль — уверенный, энергичный, как разговор с другом.
Активный залог.

3. СТРУКТУРА — свободная, не привязывайся к одному шаблону
Каждый пост может быть оформлен по-разному. Вот возможные форматы (но не ограничивайся ими):

— УГРОЗА: крючок → факты → чем опасно → как ARETORA VPN решает
— НОВОСТЬ: суть события → почему это важно → позиция ARETORA VPN
— СРАВНЕНИЕ: было (проблема) → стало (решение с ARETORA VPN)
— ЛАЙФХАК: короткий совет / фича / инструкция
— КОРОТКИЙ: 2-3 предложения, ёмко и в точку

Не используй одни и те же подзаголовки в каждом посте. Разные темы — разный формат.

4. ЭМОДЗИ — уместно и без фанатизма
В typical news post: 🔥 🛡️ 🔍 ✅ 🌟 ⚡ 🚀 💡 ⚠️ 📉 📈 👀 🎯 💣
Можно использовать и другие, если они усиливают смысл. Не ставь эмодзи просто для красоты. Максимум 2-3 за пост.

5. ФИНАЛЬНЫЙ СЛОГАН
Один из вариантов (можно варьировать):
• «Будь умнее системы. Оставайся под защитой с ARETORA VPN»
• «C ARETORA VPN ты всегда на шаг впереди»
• «С ARETORA VPN твои данные всегда в безопасности»
Можно придумать свой — главное, чтобы звучало в том же духе и упоминало ARETORA VPN.

Если уместно — можно добавить «Попробуй ARETORA VPN бесплатно — @aretora_vpn_bot»

6. ФИЧИ ARETORA VPN (выбирай подходящие к контексту, не пиши все подряд)
• Умная маскировка — скрывает сам факт использования VPN
• Авто-обход — банки, Госуслуги и карты работают напрямую
• Невидимость — сервисы видят реальный IP
• Промежуточные узлы — трафик маскируется под мессенджер
• Без открытых портов — исключён перехват ключей
• Передовой протокол с обфускацией
• Стабильность и скорость
• Удобство — не нужно включать/выключать VPN для разных приложений

7. ЧЕГО ДЕЛАТЬ НЕЛЬЗЯ
• Оставлять старый бренд или ссылку
• Переходить на «вы»
• Делать сухие технические описания
• Заканчивать пост без упоминания ARETORA VPN

Длина финального текста — не более 1024 символов. Текст должен быть полным и законченным.

Исходный текст:

{text}"""

def _strip_markdown(text: str) -> str:
    for pattern, repl in _RE_MARKDOWN:
        text = re.sub(pattern, repl, text)
    return text.strip()

def clean_output(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        if hasattr(result, "answer"):
            result.answer = _strip_markdown(result.answer)
        return result
    return wrapper

def _count_tokens(msg: AIMessage) -> int:
    if msg.usage_metadata:
        return msg.usage_metadata.get("output_tokens", 0)
    return len(msg.content) // 4 + len(msg.content.split())

@clean_output
async def ask_for_rewrite(text: str, prompt: str | None = None) -> str:
    if not text.strip():
        return text

    system_text = prompt if prompt else _REWRITE_PROMPT
    messages = [
        SystemMessage(content=system_text.format(text=text)),
        HumanMessage(content=text),
    ]
    try:
        async with _llm_sem:
            logger.info("llm_invoke_start", text_len=len(text.strip()))
            result = await asyncio.wait_for(
                agent.ainvoke({"messages": messages}),
                timeout=60.0,
            )
            last_msg = result["messages"][-1]
            content = last_msg.content.strip()

            if len(content) > 1024:
                logger.warning("llm_exceeded_limit", length=len(content))
                messages.append(last_msg)
                messages.append(HumanMessage(content=f"Сократи этот текст до 1024 символов, сохранив все ключевые факты и смысл. Сейчас {len(content)} символов, нужно не более 1024."))
                async with _llm_sem:
                    result = await asyncio.wait_for(
                        agent.ainvoke({"messages": messages}),
                        timeout=60.0,
                    )
                    content = result["messages"][-1].content.strip()

            logger.info("llm_response", post_id=0, length=len(content), preview=content[:200], msg_type=type(last_msg).__name__)
            logger.info("llm_token_usage", tokens=_count_tokens(last_msg))
            return content
    except Exception as e:
        logger.exception("agent_invoke_failed", error=str(e))
        return text

@clean_output
async def rewrite_with_custom_prompt(text: str, custom_prompt: str) -> str:
    if not text.strip():
        return text
    messages = [
        SystemMessage(content=custom_prompt),
        HumanMessage(content=text),
    ]
    try:
        async with _llm_sem:
            result = await asyncio.wait_for(
                agent.ainvoke({"messages": messages}),
                timeout=60.0,
            )
            last_msg = result["messages"][-1]
            content = last_msg.content.strip()

            if len(content) > 1024:
                logger.warning("custom_llm_exceeded_limit", length=len(content))
                messages.append(last_msg)
                messages.append(HumanMessage(content=f"Сократи этот текст до 1024 символов, сохранив все ключевые факты и смысл. Сейчас {len(content)} символов, нужно не более 1024."))
                result = await asyncio.wait_for(
                    agent.ainvoke({"messages": messages}),
                    timeout=60.0,
                )
                content = result["messages"][-1].content.strip()

            logger.info("custom_rewrite_response", length=len(content), preview=content[:200], msg_type=type(last_msg).__name__)
            logger.info("custom_rewrite_token_usage", tokens=_count_tokens(last_msg))
            return content
    except Exception as e:
        logger.exception("custom_rewrite_failed", error=str(e))
        return text


async def ask_for_regenerate_media(media: list, prompt: str) -> list:
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
        messages = [
            SystemMessage(content=prompt),
            HumanMessage(
                content=[
                    {"type": "text", "text": "Отредактируй это изображение согласно описанию стиля."},
                    {
                        "type": "image",
                        "base64": original_b64,
                        "mime_type": "image/jpeg",
                    },
                ]
            ),
        ]
        try:
            for attempt in range(2):
                async with image_sem:
                    logger.info("image_llm_invoke_start")
                    agent_output = await asyncio.wait_for(
                        image_agent.ainvoke({"messages": messages}),
                        timeout=120.0,
                    )
                response = agent_output["messages"][-1]
                logger.info("image_response_raw", content=str(response.content)[:500])
                if response.content:
                    break
                logger.warning("image_empty_retry", attempt=attempt)
                await asyncio.sleep(5)
            new_b64 = None
            blocks = response.content if isinstance(response.content, list) else [response.content] # type: ignore
            for block in blocks:
                if isinstance(block, dict):
                    if block.get("image_url"):
                        image_url = block["image_url"]
                        if isinstance(image_url, dict):
                            url = image_url.get("url", "")
                        else:
                            url = str(image_url)
                        if "," in url:
                            new_b64 = url.split(",", 1)[1]
                            break
                    if block.get("inline_data"):
                        data = block["inline_data"]
                        new_b64 = data.get("data") or data.get("base64")
                        if new_b64:
                            break
            if new_b64:
                result.append({"type": "photo", "file_bytes64": new_b64})
                logger.info("image_regen_success", item_idx=len(result))
            else:
                logger.warning("image_regen_no_image_in_response")
                result.append(item)
        except Exception as e:
            logger.exception("image_regeneration_failed", error=str(e))
            result.append(item)
    return result


# ── Worker logic ─────────────────────────────────────────


async def process_post(post_id_str: str):
    post_id = int(post_id_str)
    logger.info("processing_post", post_id=post_id)

    raw = await get_raw_post_by_id(post_id)
    if not raw:
        logger.warning("raw_post_not_found", post_id=post_id)
        return

    logger.info("raw_post_text", post_id=post_id, text_length=len(raw.text or ""), text_preview=(raw.text or "")[:100]) # type: ignore
    source = await get_bot_source_by_channel_id(raw.source_channel_id)  # type: ignore
    prompt = source.system_prompt if source else None  # type: ignore
    rewritten = await ask_for_rewrite(raw.text or "", prompt=prompt)  # type: ignore[arg-type]
    logger.info("rewritten_text", post_id=post_id, length=len(rewritten), preview=rewritten[:120])
    gen_id = await save_generated_content(raw.id, rewritten, config.MODEL_NAME)  # type: ignore[arg-type]
    await mark_raw_post_processed(raw.id, gen_id)  # type: ignore[arg-type]
    await push_to_moderation_stream(gen_id)
    logger.info("content_generated", post_id=post_id, gen_id=gen_id)


async def run_generator_worker():
    await stream_worker("stream:raw", "generators", process_post)


async def process_media(post_id_str: str):
    post_id = int(post_id_str)
    logger.info("processing_media_post", post_id=post_id)

    raw = await get_raw_post_by_id(post_id)
    if not raw:
        logger.warning("raw_post_not_found", post_id=post_id)
        return

    source = await get_bot_source_by_channel_id(raw.source_channel_id)  # type: ignore
    if not source:
        await push_to_raw_stream(post_id)
        return

    media = raw.media
    prompt = None
    any_work_done = False

    # Step 1: поиск похожих картинок (Vision API)
    if source.image_search_enabled:
        searched = await find_similar_images(media)
        if searched != media:
            any_work_done = True
            media = searched
            logger.info("media_replaced_by_search", post_id=post_id)

    # Step 2: выбор стилевого промпта (round-robin)
    if source.image_style_prompts:
        prompts = source.image_style_prompts
        prompt = prompts[raw.id % len(prompts)]  # type: ignore[arg-type]
        logger.info("image_style_round_robin", post_id=post_id, selected=raw.id % len(prompts), total=len(prompts), preview=prompt[:60])
    elif source.image_style_prompt:
        prompt = source.image_style_prompt

    # Step 3: Gemini-регенерация
    if prompt:
        logger.info("media_regeneration_start", post_id=post_id, style_preview=prompt[:80])
        media = await ask_for_regenerate_media(media, prompt=prompt)
        any_work_done = True

    if any_work_done:
        await update_raw_post_regenerated_media(post_id, media)

    await push_to_raw_stream(post_id)
    logger.info("media_content_generated", post_id=post_id, regenerated=any_work_done)


async def run_media_worker():
    await stream_worker("stream:media", "media_processor", process_media)
