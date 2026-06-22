import structlog

from aiogram import Bot, Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties

from config import config
from db import (
    get_generated_content, get_raw_text_by_content_id,
    update_generated_text, mark_generated_skipped,
)
from redis_storage import push_to_ready_stream
from content_generator import rewrite_with_custom_prompt
from stream_worker import stream_worker

logger = structlog.get_logger()


# ── FSM States ──

class ModerationStates(StatesGroup):
    waiting_for_custom_prompt = State()
    waiting_for_edit_text = State()


# ── Keyboard builders ──

def moderation_kb(content_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"publish:{content_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{content_id}"),
        ],
        [
            InlineKeyboardButton(text="🔄 Свой промпт", callback_data=f"prompt:{content_id}"),
            InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"skip:{content_id}"),
        ],
    ])


def status_kb(label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data="disabled")]
    ])


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])


# ── Router ──

router = Router()


# ── Send moderation card to channel ──

async def send_moderation_card(bot: Bot, content_id: int):
    gc = await get_generated_content(content_id)
    if not gc:
        logger.warning("moderation_content_not_found", content_id=content_id)
        return
    if gc.skipped: # type: ignore
        return

    raw_text = await get_raw_text_by_content_id(content_id)
    rewritten = gc.rewritten_text

    text = (
        f"\u2501\u2501\u2501 ОРИГИНАЛ \u2501\u2501\u2501\n{raw_text or '(нет текста)'}\n\n"
        f"\u2501\u2501\u2501 ПЕРЕПИСАНО \u2501\u2501\u2501\n{rewritten}"
    )

    await bot.send_message(
        config.MODERATION_CHANNEL_ID,
        text,
        reply_markup=moderation_kb(content_id),
    )


# ── Callback handlers ──

@router.callback_query(F.data.startswith("publish:"))
async def on_publish(cq: CallbackQuery):
    content_id = int(cq.data.split(":")[1]) # type: ignore
    await push_to_ready_stream(content_id)
    await cq.message.edit_reply_markup(reply_markup=status_kb("\u2705 Опубликовано")) # type: ignore
    await cq.answer("Пост отправлен в публикацию")


@router.callback_query(F.data.startswith("edit:"))
async def on_edit(cq: CallbackQuery, state: FSMContext):
    content_id = int(cq.data.split(":")[1]) # type: ignore
    await state.set_state(ModerationStates.waiting_for_edit_text)
    await state.update_data(
        content_id=content_id,
        channel_chat_id=cq.message.chat.id, # type: ignore
        channel_msg_id=cq.message.message_id,    # type: ignore
    )
    gc = await get_generated_content(content_id)
    current_text = gc.rewritten_text if gc else "(текст не найден)"
    changed_msg = await cq.message.answer(f"Ответьте на это сообщение с исправленным текстом:\n\n{current_text}",reply_markup=cancel_kb()) # type: ignore
    await state.update_data(changed_msg_id=changed_msg.message_id)
    await cq.answer()


@router.callback_query(F.data.startswith("prompt:"))
async def on_prompt(cq: CallbackQuery, state: FSMContext):
    content_id = int(cq.data.split(":")[1])# type: ignore
    await state.set_state(ModerationStates.waiting_for_custom_prompt)
    await state.update_data(
        content_id=content_id,
        channel_chat_id=cq.message.chat.id,# type: ignore
        channel_msg_id=cq.message.message_id,# type: ignore
    )
    prompt_msg = await cq.message.answer("Отправьте ответным сообщением ваш промпт для переработки", reply_markup=cancel_kb())# type: ignore
    await state.update_data(prompt_message_id=prompt_msg.message_id)
    await cq.answer()

@router.callback_query(F.data.startswith("skip:"))
async def on_skip(cq: CallbackQuery):
    content_id = int(cq.data.split(":")[1])# type: ignore
    await mark_generated_skipped(content_id)
    await cq.message.edit_reply_markup(reply_markup=status_kb("\u23ed Пропущено"))# type: ignore
    await cq.answer("Пост пропущен")


@router.callback_query(F.data == "cancel")
async def on_cancel(cq: CallbackQuery, state: FSMContext):
    await cq.message.delete()# type: ignore
    await cq.answer("Действие отменено")


# ── FSM message handlers ──

@router.message(ModerationStates.waiting_for_custom_prompt)
async def on_custom_prompt(msg: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    content_id = data["content_id"]
    raw_text = await get_raw_text_by_content_id(content_id)
    if not raw_text:
        await msg.answer("Исходный текст не найден")
        await state.clear()
        return

    processing_msg = await msg.answer("\U0001f504 Перерабатываю...")
    new_text = await rewrite_with_custom_prompt(raw_text, msg.text)# type: ignore
    await update_generated_text(content_id, new_text)
    await state.clear()

    await bot.edit_message_text(
        f"\u2501\u2501\u2501 ПЕРЕПИСАНО (с вашим промптом) \u2501\u2501\u2501\n{new_text}",
        chat_id=data["channel_chat_id"],
        message_id=data["channel_msg_id"],
        reply_markup=moderation_kb(content_id),
    )
    try:
        await bot.delete_message(chat_id=msg.chat.id, message_id=data["prompt_message_id"])
        await bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
        await bot.delete_message(chat_id=msg.chat.id, message_id=processing_msg.message_id)
    except Exception as e:
        logger.exception("Failed to delete messages", error=str(e))


@router.message(ModerationStates.waiting_for_edit_text)
async def on_edit_text(msg: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    content_id = data["content_id"]
    new_text = msg.text or ""
    await update_generated_text(content_id, new_text)
    await state.clear()

    await bot.edit_message_text(
        f"\u2501\u2501\u2501 ОТРЕДАКТИРОВАНО \u2501\u2501\u2501\n{new_text}",
        chat_id=data["channel_chat_id"],
        message_id=data["channel_msg_id"],
        reply_markup=moderation_kb(content_id),
    )
    try:
        await bot.delete_message(chat_id=msg.chat.id, message_id=data["changed_msg_id"])
        await bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
    except Exception as e:
        logger.exception("Failed to delete messages", error=str(e))


# ── Moderation worker ──

async def moderation_worker():
    if not config.MODERATION_CHANNEL_ID:
        logger.warning("MODERATION_CHANNEL_ID not set, moderation disabled")
        return

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))

    async def _send_card(content_id_str: str):
        await send_moderation_card(bot, int(content_id_str))

    await stream_worker("stream:moderation", "moderators", _send_card)
