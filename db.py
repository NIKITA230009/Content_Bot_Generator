import json

from sqlalchemy import (
    Column, BigInteger, Text, Boolean, DateTime, JSON,
    ForeignKey, UniqueConstraint, select, update, func,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from config import config


# ── Models ────────────────────────────────────────────────


class Base(DeclarativeBase):
    pass


class SourceChannel(Base):
    __tablename__ = "source_channels"

    id = Column(BigInteger, primary_key=True)
    title = Column(Text, nullable=True)
    username = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class GeneratedContent(Base):
    __tablename__ = "generated_content"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_post_id = Column(BigInteger, nullable=True)
    rewritten_text = Column(Text, nullable=False)
    model_used = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    raw_post = relationship("RawPost", foreign_keys=[raw_post_id], lazy="selectin")
    publish_logs = relationship("PublishLog", back_populates="generated_content", lazy="selectin")


class RawPost(Base):
    __tablename__ = "raw_posts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    text = Column(Text, nullable=True)
    media_group_id = Column(Text, nullable=True)
    media = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    processed = Column(Boolean, default=False)
    generated_content_id = Column(BigInteger, ForeignKey("generated_content.id"), nullable=True)

    generated_content = relationship("GeneratedContent", lazy="selectin")

    __table_args__ = (
        UniqueConstraint("source_channel_id", "message_id", name="uq_raw_post_source_message"),
    )


class PublishLog(Base):
    __tablename__ = "publish_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    generated_content_id = Column(BigInteger, ForeignKey("generated_content.id"), nullable=True)
    target_channel_id = Column(BigInteger, nullable=False)
    published_message_id = Column(BigInteger, nullable=True)
    published_at = Column(DateTime(timezone=True), server_default=func.now())
    success = Column(Boolean, nullable=False)
    error = Column(Text, nullable=True)

    generated_content = relationship("GeneratedContent", back_populates="publish_logs", lazy="selectin")


# ── Engine / Session ──────────────────────────────────────


_engine = None
_session_factory = None


async def get_session() -> AsyncSession:
    global _engine, _session_factory
    if _engine is None:
        _engine = create_async_engine(
            config.DATABASE_URL,
            pool_size=2,
            max_overflow=0,
            pool_pre_ping=True,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _session_factory()


# ── Repository ────────────────────────────────────────────


async def init_db():
    async with get_session() as session:
        async with session.begin():
            await session.run_sync(Base.metadata.create_all)


async def save_raw_post(
    source_channel_id: int,
    message_id: int,
    text: str,
    media_group_id: str | None,
    media: list,
) -> int | None:
    async with get_session() as session:
        post = RawPost(
            source_channel_id=source_channel_id,
            message_id=message_id,
            text=text,
            media_group_id=media_group_id,
            media=json.dumps(media) if media else None,
        )
        session.add(post)
        try:
            await session.commit()
            return post.id
        except IntegrityError:
            await session.rollback()
            return None


async def get_raw_post_by_id(post_id: int) -> RawPost | None:
    async with get_session() as session:
        result = await session.execute(
            select(RawPost).where(RawPost.id == post_id)
        )
        return result.scalar_one_or_none()


async def mark_raw_post_processed(post_id: int, gen_id: int):
    async with get_session() as session:
        await session.execute(
            update(RawPost)
            .where(RawPost.id == post_id)
            .values(processed=True, generated_content_id=gen_id)
        )
        await session.commit()


async def save_generated_content(raw_post_id: int, rewritten_text: str, model_used: str) -> int:
    async with get_session() as session:
        gc = GeneratedContent(
            raw_post_id=raw_post_id,
            rewritten_text=rewritten_text,
            model_used=model_used,
        )
        session.add(gc)
        await session.commit()
        return gc.id


async def get_generated_content(content_id: int) -> GeneratedContent | None:
    async with get_session() as session:
        result = await session.execute(
            select(GeneratedContent).where(GeneratedContent.id == content_id)
        )
        gc = result.scalar_one_or_none()
        if gc is None or gc.raw_post is None:
            return None
        return gc


async def log_publication(
    generated_content_id: int,
    target_channel_id: int,
    published_message_id: int | None,
    success: bool,
    error: str | None,
):
    async with get_session() as session:
        session.add(
            PublishLog(
                generated_content_id=generated_content_id,
                target_channel_id=target_channel_id,
                published_message_id=published_message_id,
                success=success,
                error=error,
            )
        )
        await session.commit()


async def is_already_published(generated_content_id: int, target_channel_id: int) -> bool:
    async with get_session() as session:
        result = await session.execute(
            select(PublishLog.id)
            .where(
                PublishLog.generated_content_id == generated_content_id,
                PublishLog.target_channel_id == target_channel_id,
                PublishLog.success == True,
            )
        )
        return result.first() is not None
