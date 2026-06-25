import asyncio
import json
import structlog

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import config
from telethon_listener import fetch_historical_messages, refresh_source_cache, get_client
from db import (
    add_bot_source, remove_bot_source, get_all_bot_sources,
    get_bot_source_by_username, resolve_bot_source, get_bot_source_by_channel_id,
    update_bot_source_targets, update_source_system_prompt,
    update_source_image_style, update_source_image_styles, add_source_image_style,
    update_source_image_search,
)
from ui import admin_kb

logger = structlog.get_logger()

router = Router()

_backfill_tasks: set[asyncio.Task] = set()


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Меню админа:", reply_markup=admin_kb())


class BackfillStates(StatesGroup):
    waiting_for_source = State()
    waiting_for_limit = State()


def _is_admin(user_id: int) -> bool:
    return bool(config.ADMIN_IDS) and user_id in config.ADMIN_IDS


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
            has_prompt = " [промпт: есть]" if s.system_prompt else ""
            has_image = ""
            if s.image_style_prompts:
                has_image = f" [image: {len(s.image_style_prompts)} стилей]"
            elif s.image_style_prompt:
                has_image = " [image: есть]"
            search_flag = " [search: вкл]" if s.image_search_enabled else ""
            lines.append(f"  {s.username}{resolved} → {targets}  [{s.publish_interval}s]{has_prompt}{has_image}{search_flag}")
    return "\n".join(lines) if lines else "Нет источников"


# ── /backfill ──────────────────────────────────────────────


@router.message(Command("backfill"))
async def cmd_backfill(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    sources = dict(config.SOURCE_TARGET_MAP)
    db_sources = await get_all_bot_sources()
    source_names = {}
    for s in db_sources:
        if s.channel_id:
            sources.setdefault(s.channel_id, json.loads(s.target_channel_ids))
            source_names[s.channel_id] = s.title or s.username or str(s.channel_id)

    if not sources:
        await message.answer("Нет настроенных источников")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=source_names.get(ch_id, f"Канал {ch_id}"),
            callback_data=f"bf_source:{ch_id}",
        )]
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
        await message.answer("Укажите @username и target-каналы\nПример: /set_targets @channel @target1 @target2")
        return

    username = args[1]
    if not username.startswith("@"):
        username = f"@{username}"

    client = get_client()
    targets = []
    for raw in args[2:]:
        try:
            if raw.startswith("@") or raw.startswith("https://"):
                if client is None:
                    await message.answer("Telethon не запущен — используйте числовые ID")
                    return
                if raw.startswith("https://t.me/"):
                    raw = raw.replace("https://t.me/", "@")
                entity = await client.get_entity(raw)
                tid = entity.id
                if hasattr(entity, "broadcast") or hasattr(entity, "megagroup"):
                    tid = int(f"-100{abs(tid)}")
                targets.append(tid)
            else:
                targets.append(int(raw))
        except Exception as e:
            await message.answer(f"Некорректный target: {raw} — {e}")
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


# ── /set_prompt ────────────────────────────────────────────


@router.message(Command("set_prompt"))
async def cmd_set_prompt(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Укажите @channel_name и текст промпта\nПример: /set_prompt @channel Ты — редактор новостей о криптовалютах...")
        return

    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"

    source = await get_bot_source_by_username(channel_name)
    if source is None or not source.channel_id:
        await message.answer(f"Источник {channel_name} не найден или не разрешён channel_id. Сначала добавьте через /add_source")
        return

    prompt_text = args[2]
    await update_source_system_prompt(source.channel_id, prompt_text)
    await message.answer(f"Системный промпт для {channel_name} обновлён (длина: {len(prompt_text)} символов)")


# ── /clear_prompt ──────────────────────────────────────────


@router.message(Command("clear_prompt"))
async def cmd_clear_prompt(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Укажите @channel_name\nПример: /clear_prompt @channel")
        return

    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"

    source = await get_bot_source_by_username(channel_name)
    if source is None or not source.channel_id:
        await message.answer(f"Источник {channel_name} не найден")
        return

    await update_source_system_prompt(source.channel_id, None)
    await message.answer(f"Системный промпт для {channel_name} сброшен (будет использоваться стандартный)")


# ── /set_image_style ─────────────────────────────────────


@router.message(Command("set_image_style"))
async def cmd_set_image_style(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Укажите @channel_name и описание стиля\nПример: /set_image_style @channel Наложи логотип, тёмная гамма")
        return
    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"
    source = await get_bot_source_by_username(channel_name)
    if source is None or not source.channel_id:
        await message.answer(f"Источник {channel_name} не найден")
        return
    prompt_text = args[2]
    await update_source_image_style(source.channel_id, prompt_text)
    await message.answer(f"Стиль изображений для {channel_name} обновлён")


# ── /clear_image_style ───────────────────────────────────


@router.message(Command("clear_image_style"))
async def cmd_clear_image_style(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Укажите @channel_name\nПример: /clear_image_style @channel")
        return
    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"
    source = await get_bot_source_by_username(channel_name)
    if source is None or not source.channel_id:
        await message.answer(f"Источник {channel_name} не найден")
        return
    await update_source_image_style(source.channel_id, None)
    await message.answer(f"Стиль изображений для {channel_name} сброшен")


# ── /set_image_styles ────────────────────────────────────


@router.message(Command("set_image_styles"))
async def cmd_set_image_styles(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Укажите @channel_name и стили через |||\nПример: /set_image_styles @channel тёмная тема ||| светлая тема ||| неон")
        return
    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"
    source = await get_bot_source_by_username(channel_name)
    if source is None or not source.channel_id:
        await message.answer(f"Источник {channel_name} не найден")
        return
    prompts = [p.strip() for p in args[2].split("|||") if p.strip()]
    if len(prompts) < 2:
        await message.answer("Нужно минимум 2 промпта, разделённых |||")
        return
    await update_source_image_styles(source.channel_id, prompts)
    await message.answer(f"Установлено {len(prompts)} стилей для {channel_name}, будут чередоваться по кругу")


# ── /add_image_style ─────────────────────────────────────


@router.message(Command("add_image_style"))
async def cmd_add_image_style(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Укажите @channel_name и промпт\nПример: /add_image_style @channel ретро-стиль")
        return
    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"
    source = await get_bot_source_by_username(channel_name)
    if source is None or not source.channel_id:
        await message.answer(f"Источник {channel_name} не найден")
        return
    await add_source_image_style(source.channel_id, args[2])
    await message.answer(f"Промпт добавлен в список для {channel_name}")


# ── /set_image_search ────────────────────────────────────


@router.message(Command("set_image_search"))
async def cmd_set_image_search(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer(
            "Укажите @channel_name и on/off\n"
            "Пример: /set_image_search @channel on"
        )
        return
    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"
    source = await get_bot_source_by_username(channel_name)
    if source is None or not source.channel_id:
        await message.answer(f"Источник {channel_name} не найден")
        return
    enabled = args[2].lower() in ("on", "yes", "1", "true")
    await update_source_image_search(source.channel_id, enabled)
    state = "включён" if enabled else "выключен"
    await message.answer(f"Поиск картинок для {channel_name} {state}")


# ── /source_info ──────────────────────────────────────────


@router.message(Command("source_info"))
async def cmd_source_info(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Нет доступа")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Укажите @channel_name\nПример: /source_info @channel")
        return
    channel_name = args[1]
    if not channel_name.startswith("@"):
        channel_name = f"@{channel_name}"
    source = await get_bot_source_by_username(channel_name)
    if source is None:
        await message.answer(f"Источник {channel_name} не найден")
        return

    lines = [
        f"<b>Источник:</b> {source.username}",
        f"<b>ID:</b> {source.channel_id or 'не разрешён'}",
        f"<b>Title:</b> {source.title or '-'}",
        f"<b>Targets:</b> {source.target_channel_ids}",
        f"<b>Interval:</b> {source.publish_interval}s",
    ]
    if source.system_prompt:
        lines.append("")
        lines.append("<b>System prompt:</b> ↓ в файле ниже")
    lines.append(f"<b>Search images:</b> {'вкл' if source.image_search_enabled else 'выкл'}")
    if source.image_style_prompts:
        lines.append("")
        lines.append(f"<b>Image styles ({len(source.image_style_prompts)} шт.):</b> ↓ в файле ниже")
    elif source.image_style_prompt:
        lines.append("")
        lines.append("<b>Image style prompt:</b> ↓ в файле ниже")
    await message.answer("\n".join(lines), parse_mode="HTML")

    if source.system_prompt:
        await message.answer_document(
            BufferedInputFile(source.system_prompt.encode(), filename=f"system_prompt_{source.username}.txt")
        )
    if source.image_style_prompts:
        content = "\n\n---\n\n".join(f"[{i+1}] {p}" for i, p in enumerate(source.image_style_prompts))
        await message.answer_document(
            BufferedInputFile(content.encode(), filename=f"image_styles_{source.username}.txt")
        )
    elif source.image_style_prompt:
        await message.answer_document(
            BufferedInputFile(source.image_style_prompt.encode(), filename=f"image_style_{source.username}.txt")
        )
