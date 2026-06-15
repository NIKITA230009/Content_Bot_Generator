# Анализ кодовой базы TLG_content_bot

## 📋 Общая структура

Проект представляет собой Telegram-бота для репоста контента из каналов-источников в каналы-цели с переписыванием через LLM (OpenRouter).

**11 файлов, ~821 строка кода.**

---

## ✅ Что уже реализовано

### 1. Конфигурация (`config.py`)
- Загрузка настроек из `.env` через `pydantic-settings`
- Парсинг JSON-маппинга источников/целей и интервалов публикации
- Настройки БД, Redis, LLM-модели

### 2. Точка входа (`bot.py`)
- Запуск aiogram polling
- Инициализация БД, генератора и воркера публикации через `asyncio.gather`

### 3. Прослушивание каналов (`channel_listener.py`)
- Фильтрация постов только от указанных каналов-источников
- Сохранение сырого поста в БД
- Отправка ID поста в Redis-очередь `queue:raw`

### 4. Агрегация медиа-групп (`media_aggregator.py`)
- Сбор всех сообщений из одной медиа-группы (альбома)
- Таймаут ожидания (настраивается через `MEDIA_AGGREGATION_TIMEOUT`)
- Склейка caption и всех медиа-файлов в один пост
- Использование asyncio.Future для ожидания

### 5. Генерация контента (`content_generator.py`)
- Запрос к LLM через OpenRouter (deepseek/deepseek-chat)
- Переписывание текста с сохранением фактов, ссылок, эмодзи
- Семафор на 3 одновременных запроса к LLM
- Очередь `queue:raw` → обработка → `queue:ready`

### 6. Публикация (`publisher.py`)
- Отправка в целевые каналы (текст + медиа-группы)
- Интервалы между публикациями (`PUBLISH_INTERVALS`)
- Логирование успешных и неудачных публикаций
- Проверка дубликатов (уже опубликовано)
- Очередь `queue:ready` → публикация

### 7. Redis-слой (`redis_storage.py`)
- Очереди через `lpush`/`brpoplpush` (атомарное перемещение)
- Медиа-агрегация через Redis-списки с TTL
- Блокировки (`set nx ex`)
- Флаг `in_progress` для защиты от дублирующей обработки
- Хранение времени последней публикации

### 8. Надёжная очередь (`reliable_queue.py`)
- Шаблон `brpoplpush` — атомарное перемещение в processing-очередь
- Удаление из processing только после успешной обработки
- Обработка ошибок с логированием

### 9. PostgreSQL-слой (`db.py`)
- Таблицы: `source_channels`, `raw_posts`, `generated_content`, `publish_log`
- Автосоздание таблиц при старте (`CREATE TABLE IF NOT EXISTS`)
- `UNIQUE(source_channel_id, message_id)` — защита от дубликатов
- `ON CONFLICT DO NOTHING` при вставке сырых постов

---

## ❌ Критические ошибки (не запустится)

### 1. `content_generator.py:31` — Несуществующая функция `create_agent`
```python
from langchain.agents import create_agent  # ОШИБКА!
```
Функция `create_agent` **не существует** в langchain. В зависимости от версии:
- `langchain.agents.create_openai_tools_agent` (новые версии)
- `langchain.agents.initialize_agent` (старые версии, deprecated)

**Код упадёт с `AttributeError` при импорте/запуске.**

### 2. `content_generator.py:62` — Неверный формат вызова AgentExecutor
```python
result = await agent.ainvoke({"messages": messages})
```
`AgentExecutor` ожидает `{"input": "<text>"}`, а не `{"messages": [...]}`.
Либо нужно использовать `ainvoke({"input": text})`, либо переходить на `AIMessage`-чатинг.

### 3. Нет файла `.env`
В проекте есть только `.env.example`. Без `.env` `pydantic-settings` не найдёт переменные, и бот не запустится.

---

## ⚠️ Существенные проблемы

### 4. Потеря постов при ошибке в `reliable_queue.py:27`
```python
except Exception as e:
    await remove_from_processing(processing_queue, item)  # ← потеря
```
При любой ошибке в `process_func` элемент **удаляется** из processing-очереди и **теряется навсегда**. Должна быть либо retry-логика, либо dead-letter queue, либо возврат в основную очередь.

### 5. Нет восстановления processing-очереди при старте
После перезапуска бота элементы, оставшиеся в `queue:raw:processing` / `queue:ready:processing`, будут висеть там вечно, т.к. нет кода, который возвращает их в основную очередь при старте. Это нарушает всю надёжность `brpoplpush`.

### 6. `media_aggregator.py` — не сохраняется `media_group_id` в parts
```python
def _extract_part(msg: Message) -> dict:
    return {
        "message_id": msg.message_id,
        "text": msg.caption or "",
        "media": media,
        # нет media_group_id!
    }
```
В `_merge_parts` используется `parts[0].get("media_group_id")`, но оно всегда будет `None`, т.к. не сохраняется.

### 7. `media_aggregator.py` — гонка при параллельных медиа-группах
Глобальный словарь `_media_futures` не чистится, если таймер сработал, а потом пришло ещё одно сообщение из той же группы (с большим запозданием). Оно никогда не будет обработано.

### 8. `content_generator.py` — текст передаётся дважды
```python
SystemMessage(content=_REWRITE_PROMPT.format(text=text)),  # текст уже внутри
HumanMessage(content=text),                                  # и ещё раз
```
LLM получит текст дважды. Системный промпт уже содержит текст, отдельный HumanMessage избыточен.

### 9. Таблица `source_channels` не используется
Создаётся в `init_db`, но никогда не заполняется и не имеет внешнего ключа из `raw_posts`. Бесполезная таблица.

### 10. Redis-соединение без retry/reconnect
```python
_redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
```
При падении Redis бот упадёт с ошибкой. Нет настройки `retry_on_timeout`, `socket_keepalive`, `health_check_interval`.

---

## 🔧 Незначительные проблемы и неточности

### 11. Неиспользуемый импорт `ParseMode` в `publisher.py`
```python
from aiogram.enums import ParseMode  # не используется
```

### 12. Отсутствует `LLM_MAX_RETRIES` в `.env.example`
Поле есть в `Config`, используется в `content_generator.py`, но не указано в примере.

### 13. Отсутствует `.gitignore`
`.env` и `__pycache__` будут коммититься в git (если его инициализировать).

### 14. Нет graceful shutdown
При `SIGINT`/`SIGTERM` asyncio.run() прервёт все корутины. Нет закрытия пула соединений с БД, нет закрытия Redis.

### 15. `_merge_parts` — caption берётся с последнего элемента группы
```python
if p.get("text"):
    text = p["text"] or text  # перезаписывается каждым элементом
```
В Telegram caption обычно только на первом файле альбома, так что это может работать, но логика неочевидная.

### 16. Нет alembic / миграций
Таблицы создаются через `CREATE TABLE IF NOT EXISTS`. Изменение схемы потребует ручного вмешательства.

### 17. Нет тестов
Весь проект — ни одного теста. Невозможно гарантировать работу после изменений.

### 18. Нет Docker/docker-compose
Для запуска нужны PostgreSQL и Redis, но нет готовой инфраструктуры для локальной разработки.

### 19. Избыточное создание Bot-инстансов
- `bot.py:22` — создаётся `Bot` для polling
- `bot.py:24` / `publisher.py:15` — `init_bot` создаёт **второй** `Bot` для публикации
- Фактически два независимых клиента aiogram. Это не ошибка, но стоит унифицировать.

### 20. Нет хендлеров для админ-команд
Нет `/start`, `/help`, команд управления источниками/целями. Только пассивное прослушивание каналов.

---

## 📝 Что надо реализовать (приоритет)

### Критично (без этого не заработает)
1. **Исправить `create_agent`** — заменить на `create_openai_tools_agent` или `create_react_agent`
2. **Исправить вызов агента** — правильный формат `ainvoke`
3. **Создать `.env`** на основе `.env.example`

### Важно
4. **Добавить восстановление processing-очереди** при старте бота
5. **Retry или dead-letter очередь** при ошибках в reliable_worker
6. **Сохранять `media_group_id`** в `_extract_part`
7. **Добавить reconnection logic** для Redis и PostgreSQL
8. **Graceful shutdown** — закрытие соединений по сигналу

### Желательно
9. **Добавить `.gitignore`**
10. **Обновить `.env.example`** — добавить `LLM_MAX_RETRIES`
11. **Убрать неиспользуемый `ParseMode`**
12. **Добавить тесты**
13. **Docker-compose** с PostgreSQL + Redis
14. **Удалить неиспользуемую таблицу `source_channels`** или начать её заполнять
15. **Добавить метрики/мониторинг**
16. **Rate limiting для публикации** (Telegram имеет лимиты)

---

## 🔍 Итоговая оценка

| Аспект | Оценка |
|--------|--------|
| Архитектура | Хорошая — чёткое разделение на модули |
| Надёжность | Средняя — нет восстановления после сбоев |
| Готовность к запуску | **Не готов** — упадёт на `create_agent` |
| Покрытие кодом | Нулевое — нет тестов |
| Production-ready | Нет — нет graceful shutdown, миграций, мониторинга |
