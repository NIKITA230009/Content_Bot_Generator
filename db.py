import json

import asyncpg
from config import config

_pool: asyncpg.Pool | None = None
_DB_URL = config.DATABASE_URL.replace("+asyncpg", "").replace("+psycopg2", "")


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(_DB_URL, min_size=1, max_size=2)
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS source_channels (
                id BIGINT PRIMARY KEY,
                title TEXT,
                username TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS generated_content (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                raw_post_id BIGINT,
                rewritten_text TEXT NOT NULL,
                model_used TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_posts (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                source_channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                text TEXT,
                media_group_id TEXT,
                media JSONB,
                created_at TIMESTAMPTZ DEFAULT now(),
                processed BOOLEAN DEFAULT FALSE,
                generated_content_id BIGINT REFERENCES generated_content(id),
                UNIQUE (source_channel_id, message_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS publish_log (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                generated_content_id BIGINT REFERENCES generated_content(id),
                target_channel_id BIGINT NOT NULL,
                published_message_id BIGINT,
                published_at TIMESTAMPTZ DEFAULT now(),
                success BOOLEAN NOT NULL,
                error TEXT
            )
        """)


async def save_raw_post(
    source_channel_id: int,
    message_id: int,
    text: str,
    media_group_id: str | None,
    media: list,
) -> int | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO raw_posts (source_channel_id, message_id, text, media_group_id, media)
               VALUES ($1, $2, $3, $4, $5::jsonb)
               ON CONFLICT (source_channel_id, message_id) DO NOTHING
               RETURNING id""",
            source_channel_id, message_id, text, media_group_id,
            json.dumps(media) if media else None,
        )
        return row["id"] if row else None


async def get_raw_post_by_id(post_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM raw_posts WHERE id = $1", post_id)
        return dict(row) if row else None


async def mark_raw_post_processed(post_id: int, gen_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE raw_posts SET processed = TRUE, generated_content_id = $2 WHERE id = $1",
            post_id, gen_id,
        )


async def save_generated_content(raw_post_id: int, rewritten_text: str, model_used: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO generated_content (raw_post_id, rewritten_text, model_used) VALUES ($1, $2, $3) RETURNING id",
            raw_post_id, rewritten_text, model_used,
        )
        return row["id"]


async def get_generated_content(content_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT gc.*, rp.source_channel_id, rp.media, rp.text AS original_text
               FROM generated_content gc
               JOIN raw_posts rp ON rp.id = gc.raw_post_id
               WHERE gc.id = $1""",
            content_id,
        )
        return dict(row) if row else None


async def log_publication(
    generated_content_id: int,
    target_channel_id: int,
    published_message_id: int | None,
    success: bool,
    error: str | None,
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO publish_log (generated_content_id, target_channel_id, published_message_id, success, error)
               VALUES ($1, $2, $3, $4, $5)""",
            generated_content_id, target_channel_id, published_message_id, success, error,
        )


async def is_already_published(generated_content_id: int, target_channel_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM publish_log WHERE generated_content_id = $1 AND target_channel_id = $2 AND success = TRUE",
            generated_content_id, target_channel_id,
        )
        return row is not None
