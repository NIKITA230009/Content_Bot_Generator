import asyncio
import json
import structlog

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import config
from telethon_listener import fetch_historical_messages, refresh_source_cache, get_client
from db import (
    add_bot_source, remove_bot_source, get_all_bot_sources,
    get_bot_source_by_username, resolve_bot_source, get_bot_source_by_channel_id,
    update_bot_source_targets,
)

logger = structlog.get_logger()

router = Router()

_backfill_tasks: set[asyncio.Task] = set()


class BackfillStates(StatesGroup):
    waiting_for_source = State()
    waiting_for_limit = State()


def _is_admin(user_id: int) -> bool:
    return config.ADMIN_CHAT_ID > 0 and user_id == config.ADMIN_CHAT_ID


async def _build_source_list() -> str:
    lines = ["<b>Источники из конфига (.env):</b>"]
    for ch_id, targets in config.SOURCE_TARGET_MAP.items():
        lines.append(f"  {ch_id} → {targets}")
    db_sources = await get_all_bot_sources()
    if db_sources:
        lines.append("")
        lines.append("<b>Источники из БД (бот-команды):</b>")
        for s in db_sources:
            targets = json.loads(s.target_channel_ids)
            resolved = f" (id: {s.channel_id}, {s.title})" if s.channel_id else " (не разрешён)"
            lines.append(f"  {s.username}{resolved} → {targets}  [{s.publish_interval}s]")
    return "\n".join(lines) if lines else "Нет источников"


# ── /backfill ──────────────────────────────────────────────


@router.message(Command("backfill"))
async def cmd_backfill(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    sources = config.SOURCE_TARGET_MAP
    if not sources:
        await message.answer("Нет настроенных источников")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Канал {ch_id}", callback_data=f"bf_source:{ch_id}")]
        for ch_id in sources
    ])
    await state.set_state(BackfillStates.waiting_for_source)
    await message.answer("Выберите канал-источник:", reply_markup=kb)


@router.callback_query(BackfillStates.waiting_for_source, F.data.startswith("bf_source:"))
async def on_source_selected(cq: CallbackQuery, state: FSMContext):
    channel_id = int(cq.data.split(":")[1])
    await state.update_data(channel_id=channel_id)
    await state.set_state(BackfillStates.waiting_for_limit)
    await cq.message.edit_text("Сколько постов загрузить? (1-200)")
    await cq.answer()


@router.message(BackfillStates.waiting_for_limit, F.text)
async def on_limit_received(message: Message, state: FSMContext):
    try:
        limit = int(message.text)
        if limit < 1 or limit > 200:
            raise ValueError
    except ValueError:
        await message.answer("Введите число от 1 до 200")
        return

    data = await state.get_data()
    channel_id = data["channel_id"]
    await state.clear()

    status_msg = await message.answer(f"Загружаю {limit} постов из канала {channel_id}...")
    task = asyncio.create_task(_run_backfill(status_msg, channel_id, limit))
    _backfill_tasks.add(task)
    task.add_done_callback(_backfill_tasks.discard)


async def _run_backfill(status_msg: Message, channel_id: int, limit: int):
    try:
        await fetch_historical_messages(channel_id, limit)
        await status_msg.edit_text("Загрузка завершена")
    except Exception as e:
        logger.exception("backfill_failed", channel_id=channel_id, error=str(e))
        await status_msg.edit_text(f"Ошибка при загрузке: {e}")


# ── /add_source ────────────────────────────────────────────


@router.message(Command("add_source"))
async def cmd_add_source(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Укажите @username или ссылку на канал\nПример: /add_source @channel_name")
        return

    username = args[1].strip()
    if username.startswith("https://t.me/"):
        username = username.replace("https://t.me/", "@")
    if not username.startswith("@"):
        username = f"@{username}"

    existing = await get_bot_source_by_username(username)
    if existing:
        await message.answer(f"Источник {username} уже добавлен")
        return

    obj = await add_bot_source(username)
    if obj is None:
        await message.answer("Ошибка при добавлении (возможно, дубликат)")
        return

    client = get_client()
    if client is None:
        await message.answer(f"Источник {username} добавлен, но клиент Telethon не запущен — channel_id не разрешён")
        return

    try:
        entity = await client.get_entity(username)
        channel_id = entity.id
        title = getattr(entity, "title", None) or str(entity.id)
        await resolve_bot_source(username, channel_id, title)
        await refresh_source_cache()
        await message.answer(f"Источник {username} добавлен (ID: {channel_id}, «{title}»)")
    except Exception as e:
        logger.exception("resolve_entity_failed", username=username, error=str(e))
        await message.answer(f"Источник {username} сохранён, но не удалось разрешить ID: {e}. Проверьте, что пользователь подписан на канал.")


# ── /remove_source ─────────────────────────────────────────


@router.message(Command("remove_source"))
async def cmd_remove_source(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Укажите @username для удаления\nПример: /remove_source @channel_name")
        return

    identifier = args[1].strip()
    if identifier.startswith("https://t.me/"):
        identifier = identifier.replace("https://t.me/", "@")
    if not identifier.startswith("@"):
        identifier = f"@{identifier}"

    ok = await remove_bot_source(identifier)
    if ok:
        await refresh_source_cache()
        await message.answer(f"Источник {identifier} удалён")
    else:
        await message.answer(f"Источник {identifier} не найден")


# ── /sources ───────────────────────────────────────────────


@router.message(Command("sources"))
async def cmd_sources(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    text = await _build_source_list()
    await message.answer(text, parse_mode="HTML")


# ── /set_targets ───────────────────────────────────────────


@router.message(Command("set_targets"))
async def cmd_set_targets(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.answer("Укажите @username и target ID\nПример: /set_targets @channel -100111 -100222")
        return

    username = args[1]
    if not username.startswith("@"):
        username = f"@{username}"

    targets = []
    for raw in args[2:]:
        try:
            targets.append(int(raw))
        except ValueError:
            await message.answer(f"Некорректный ID: {raw}")
            return

    source = await get_bot_source_by_username(username)
    if source is None:
        await message.answer(f"Источник {username} не найден. Сначала добавьте через /add_source")
        return
    if not source.channel_id:
        await message.answer(f"У источника {username} ещё не разрешён channel_id. Попробуйте позже")
        return

    await update_bot_source_targets(source.channel_id, targets)
    await message.answer(f"Target-каналы для {username} обновлены: {targets}")
