import asyncio
import structlog
from langchain.agents import create_agent
from langchain.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, ToolMessage

from config import config
from db import get_raw_post_by_id, save_generated_content, mark_raw_post_processed
from redis_storage import (
    is_in_progress, mark_in_progress, clear_in_progress,
    push_to_ready_queue,
)
from reliable_queue import reliable_worker

logger = structlog.get_logger()

# ── LLM setup (copied from support bot pattern) ─────────

model = ChatOpenAI(
    model=config.MODEL_NAME,
    temperature=config.MODEL_TEMPERATURE,
    max_tokens=config.MODEL_MAX_TOKENS,
    max_retries=config.LLM_MAX_RETRIES,
    api_key=config.OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
    default_headers={
        "HTTP-Referer": "https://aretora.ru",
        "X-Title": "Content Repost Bot",
    },
)

_llm_sem = asyncio.Semaphore(3)


@tool(return_direct=True)
def approve_content(text: str) -> str:
    """Return the final rewritten text for approval."""
    return f"__APPROVE__:{text}"


agent = create_agent(model=model, tools=[approve_content])

_REWRITE_PROMPT = """Ты — копирайтер Telegram-канала. Перепиши текст ниже так, чтобы он:
1. Сохранял все факты, цифры, ссылки без изменений
2. Был живым, понятным, без воды
3. Сохранял эмодзи
4. Без markdown-разметки, без звёздочек, без обратных кавычек

Исходный текст:

{text}

Перепиши и вызови approve_content с финальным текстом."""


async def ask_for_rewrite(text: str) -> str:
    if not text.strip():
        return text

    messages = [
        SystemMessage(content=_REWRITE_PROMPT.format(text=text)),
        HumanMessage(content=text),
    ]
    async with _llm_sem:
        result = await agent.ainvoke({"messages": messages})
    last = result["messages"][-1]
    if isinstance(last, ToolMessage):
        _, content = last.content.split(":", 1)
        return content.strip()
    return last.content.strip()


# ── Worker logic ─────────────────────────────────────────


async def process_post(post_id_str: str):
    post_id = int(post_id_str)

    if await is_in_progress(post_id):
        logger.info("post_already_in_progress", post_id=post_id)
        return

    await mark_in_progress(post_id)
    try:
        raw = await get_raw_post_by_id(post_id)
        if not raw:
            logger.warning("raw_post_not_found", post_id=post_id)
            return

        rewritten = await ask_for_rewrite(raw.get("text") or "")
        gen_id = await save_generated_content(raw["id"], rewritten, config.MODEL_NAME)
        await mark_raw_post_processed(raw["id"], gen_id)
        await push_to_ready_queue(gen_id)
        logger.info("content_generated", post_id=post_id, gen_id=gen_id)

    except Exception as e:
        logger.error("process_post_error", post_id=post_id, error=str(e))
    finally:
        await clear_in_progress(post_id)


async def run_generator_worker():
    await reliable_worker("queue:raw", process_post)
