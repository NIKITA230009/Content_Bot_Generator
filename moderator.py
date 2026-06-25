import base64
import structlog

from aiogram import Bot, Router, F
from aiogram.types import (
    CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message,
    BufferedInputFile, InputMediaPhoto, InputMediaVideo,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties

from config import config
from db import (
    get_generated_content, get_raw_text_by_content_id,
    update_generated_text, mark_generated_skipped, update_raw_post_media,
    update_raw_post_regenerated_media, get_bot_source_by_channel_id,
)
from redis_storage import push_to_ready_stream
from content_generator import rewrite_with_custom_prompt, ask_for_regenerate_media
from stream_worker import stream_worker

logger = structlog.get_logger()


# ── FSM States ──

class ModerationStates(StatesGroup):
    waiting_for_custom_prompt = State()
    waiting_for_edit_text = State()
    waiting_for_media = State()
    waiting_for_media_style = State()


# ── Keyboard builders ──

def moderation_kb(content_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"publish:{content_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{content_id}"),
        ],
        [
            InlineKeyboardButton(text="🔄 Свой промпт", callback_data=f"prompt:{content_id}"),
            InlineKeyboardButton(text="🖼 Заменить медиа", callback_data=f"remedia:{content_id}"),
        ],
        [
            InlineKeyboardButton(text="🔄 Regen (оригинал)", callback_data=f"regenmedia:orig:{content_id}"),
            InlineKeyboardButton(text="🔄 Regen (regen)", callback_data=f"regenmedia:regen:{content_id}"),
        ],
        [
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

    media_msg = None
    media = gc.raw_post.media or []
    if media:
        try:
            group = []
            for i, m in enumerate(media):
                if "file_bytes64" not in m:
                    continue
                raw = base64.b64decode(m["file_bytes64"])
                buf = BufferedInputFile(raw, filename=f"media{i}")
                cap = f"\U0001f4ce \u041C\u0435\u0434\u0438\u0430 \u043A \u043F\u043E\u0441\u0442\u0443 #{content_id} ({i+1}/{len(media)})"
                if m["type"] == "photo":
                    group.append(InputMediaPhoto(media=buf, caption=cap))
                else:
                    group.append(InputMediaVideo(media=buf, caption=cap))
            if len(group) == 1:
                msg = await bot.send_photo(
                    config.MODERATION_CHANNEL_ID, group[0].media,
                    caption=group[0].caption,
                )
                media_msg = msg
            elif len(group) > 1:
                msgs = await bot.send_media_group(config.MODERATION_CHANNEL_ID, group)
                media_msg = msgs[0]
        except Exception as e:
            logger.warning("media_send_failed", content_id=content_id, error=str(e))

    if gc.raw_post.regenerated_media:
        try:
            regen = gc.raw_post.regenerated_media
            group_regen = []
            for i, m in enumerate(regen):
                if "file_bytes64" not in m:
                    continue
                raw = base64.b64decode(m["file_bytes64"])
                buf = BufferedInputFile(raw, filename=f"regen{i}")
                cap = f"\U0001f3a8 \u041F\u0435\u0440\u0435\u0433\u0435\u043D\u0435\u0440\u0438\u0440\u043E\u0432\u0430\u043D\u043E #{content_id} ({i+1}/{len(regen)})"
                if m["type"] == "photo":
                    group_regen.append(InputMediaPhoto(media=buf, caption=cap))
                else:
                    group_regen.append(InputMediaVideo(media=buf, caption=cap))
            if len(group_regen) == 1:
                await bot.send_photo(
                    config.MODERATION_CHANNEL_ID, group_regen[0].media,
                    caption=group_regen[0].caption,
                )
            elif len(group_regen) > 1:
                await bot.send_media_group(config.MODERATION_CHANNEL_ID, group_regen)
        except Exception as e:
            logger.warning("regen_media_send_failed", content_id=content_id, error=str(e))

    text = (
        f"\u2501\u2501\u2501 ОРИГИНАЛ \u2501\u2501\u2501\n{raw_text or '(нет текста)'}\n\n"
        f"\u2501\u2501\u2501 ПЕРЕПИСАНО \u2501\u2501\u2501\n{rewritten}"
    )

    kwargs: dict = {}
    if media_msg:
        kwargs["reply_to_message_id"] = media_msg.message_id

    await bot.send_message(
        config.MODERATION_CHANNEL_ID,
        text,
        reply_markup=moderation_kb(content_id),
        **kwargs,
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


@router.callback_query(F.data.startswith("remedia:"))
async def on_remedia(cq: CallbackQuery, state: FSMContext):
    content_id = int(cq.data.split(":")[1]) # type: ignore
    await state.set_state(ModerationStates.waiting_for_media)
    await state.update_data(
        content_id=content_id,
        channel_chat_id=cq.message.chat.id, # type: ignore
        channel_msg_id=cq.message.message_id, # type: ignore
    )
    await cq.message.answer("Отправьте ответным сообщением новое фото или видео", reply_markup=cancel_kb()) # type: ignore
    await cq.answer()


@router.callback_query(F.data.startswith("regenmedia:"))
async def on_regen_media(cq: CallbackQuery, state: FSMContext):
    parts = cq.data.split(":")
    source_type = parts[1]  # "orig" или "regen"
    content_id = int(parts[2])
    await state.set_state(ModerationStates.waiting_for_media_style)
    await state.update_data(content_id=content_id, regen_source=source_type)
    label = "из оригинала" if source_type == "orig" else "из перегенерированного"
    await cq.message.answer(f"Напишите промпт для перегенерации ({label})", reply_markup=cancel_kb())
    await cq.answer()


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


@router.message(ModerationStates.waiting_for_media)
async def on_replace_media(msg: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    content_id = data["content_id"]

    if not msg.photo and not msg.video:
        await msg.answer("Пожалуйста, отправьте фото или видео")
        return

    raw = await bot.download(msg.photo[-1] if msg.photo else msg.video)  # type: ignore
    file_bytes = raw.read()
    encoded = base64.b64encode(file_bytes).decode()
    mtype = "photo" if msg.photo else "video"

    gc = await get_generated_content(content_id)
    if not gc:
        await msg.answer("Пост не найден")
        await state.clear()
        return

    new_media = [{"file_bytes64": encoded, "type": mtype}]
    await update_raw_post_media(gc.raw_post.id, new_media)
    await state.clear()

    # send new media into moderation group
    buf = BufferedInputFile(file_bytes, filename=f"media.{mtype}")
    cap = f"\U0001f4ce \u041C\u0435\u0434\u0438\u0430 \u043A \u043F\u043E\u0441\u0442\u0443 #{content_id} (\u0437\u0430\u043C\u0435\u043D\u0435\u043D\u043E)"
    if mtype == "photo":
        await bot.send_photo(config.MODERATION_CHANNEL_ID, buf, caption=cap)
    else:
        await bot.send_video(config.MODERATION_CHANNEL_ID, buf, caption=cap)

    try:
        await bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
    except Exception as e:
        logger.exception("Failed to delete message", error=str(e))


@router.message(ModerationStates.waiting_for_media_style)
async def on_media_style_prompt(msg: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    content_id = data["content_id"]
    gc = await get_generated_content(content_id)
    if not gc or not gc.raw_post:
        await msg.answer("Пост не найден")
        await state.clear()
        return

    regen_source = data.get("regen_source", "orig")
    source_media = gc.raw_post.media
    if regen_source == "regen" and gc.raw_post.regenerated_media:
        source_media = gc.raw_post.regenerated_media

    # combined = msg.text
    # source = await get_bot_source_by_channel_id(gc.raw_post.source_channel_id)
    # if source:
    #     if source.image_style_prompts:
    #         prompts = source.image_style_prompts
    #         style_prompt = prompts[gc.raw_post.id % len(prompts)]
    #         combined = f"{style_prompt}\n\nДополнительное требование пользователя: {msg.text}"
    #     elif source.image_style_prompt:
    #         combined = f"{source.image_style_prompt}\n\nДополнительное требование пользователя: {msg.text}"

    processing = await msg.answer("\U0001f504 \u0413\u0435\u043D\u0435\u0440\u0438\u0440\u0443\u044E...")
    new_media = await ask_for_regenerate_media(source_media, msg.text)
    await update_raw_post_regenerated_media(gc.raw_post.id, new_media)
    await state.clear()

    if new_media:
        for i, m in enumerate(new_media):
            if "file_bytes64" not in m:
                continue
            raw = base64.b64decode(m["file_bytes64"])
            buf = BufferedInputFile(raw, filename=f"regen{i}")
            cap = f"\U0001f3a8 \u041F\u0435\u0440\u0435\u0433\u0435\u043D\u0435\u0440\u0438\u0440\u043E\u0432\u0430\u043D\u043E #{content_id} ({i+1}/{len(new_media)})"
            if m["type"] == "photo":
                await bot.send_photo(config.MODERATION_CHANNEL_ID, buf, caption=cap)
            else:
                await bot.send_video(config.MODERATION_CHANNEL_ID, buf, caption=cap)

    try:
        await bot.delete_message(chat_id=msg.chat.id, message_id=processing.message_id)
        await bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
    except Exception:
        pass


# ── Moderation worker ──

async def moderation_worker():
    if not config.MODERATION_CHANNEL_ID:
        logger.warning("MODERATION_CHANNEL_ID not set, moderation disabled")
        return

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))

    async def _send_card(content_id_str: str):
        await send_moderation_card(bot, int(content_id_str))

    await stream_worker("stream:moderation", "moderators", _send_card)
