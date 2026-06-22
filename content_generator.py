import asyncio
import functools
import re
import structlog
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

from config import config
from db import get_raw_post_by_id, save_generated_content, mark_raw_post_processed
from redis_storage import push_to_ready_stream, push_to_moderation_stream
from stream_worker import stream_worker

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

_RE_MARKDOWN = [
    (r'\*\*(.+?)\*\*', r'\1'),
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

_REWRITE_PROMPT = """Ты — копирайтер Telegram-канала. Перепиши текст ниже так, чтобы он:
1. Сохранял все факты, цифры, ссылки без изменений
2. Был живым, понятным, без воды
3. Сохранял эмодзи
4. Без markdown-разметки, без звёздочек, без обратных кавычек
5. Без ссылок на каналы, без упоминаний источников, без рекламы — публикуется как оригинальный контент вашего канала

Исходный текст:

{text}

Перепиши и отправь финальный текст."""

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
async def ask_for_rewrite(text: str) -> str:
    if not text.strip():
        return text

    messages = [
        SystemMessage(content=_REWRITE_PROMPT.format(text=text)),
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
            logger.info("custom_rewrite_response", length=len(content), preview=content[:200], msg_type=type(last_msg).__name__)
            logger.info("custom_rewrite_token_usage", tokens=_count_tokens(last_msg))
            return content
    except Exception as e:
        logger.exception("custom_rewrite_failed", error=str(e))
        return text


# ── Worker logic ─────────────────────────────────────────


async def process_post(post_id_str: str):
    post_id = int(post_id_str)
    logger.info("processing_post", post_id=post_id)

    raw = await get_raw_post_by_id(post_id)
    if not raw:
        logger.warning("raw_post_not_found", post_id=post_id)
        return

    logger.info("raw_post_text", post_id=post_id, text_length=len(raw.text or ""), text_preview=(raw.text or "")[:100]) # type: ignore
    rewritten = await ask_for_rewrite(raw.text or "")  # type: ignore[arg-type]
    logger.info("rewritten_text", post_id=post_id, length=len(rewritten), preview=rewritten[:120])
    gen_id = await save_generated_content(raw.id, rewritten, config.MODEL_NAME)  # type: ignore[arg-type]
    await mark_raw_post_processed(raw.id, gen_id)  # type: ignore[arg-type]
    await push_to_moderation_stream(gen_id)
    logger.info("content_generated", post_id=post_id, gen_id=gen_id)


async def run_generator_worker():
    await stream_worker("stream:raw", "generators", process_post)
