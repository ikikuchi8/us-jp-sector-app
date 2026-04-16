"""
Database connection and session management.

Provides:
- SQLAlchemy engine (synchronous)
- Session factory
- `get_db` dependency for FastAPI route injection
- `Base` declarative base for ORM models (Task 1-3 以降で使用)

将来の拡張ポイント:
- 非同期エンジン (AsyncSession) への切り替えは async_database_url を使用
- コネクションプール設定は create_engine の pool_* パラメータで調整
"""

from collections.abc import Generator
from typing import Annotated

from fastapi import Depends
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import Settings, get_settings


class Base(DeclarativeBase):
    """Declarative base class for all ORM models.

    すべての ORM モデルはこのクラスを継承する。

    Alembic の autogenerate を使う場合、`alembic/env.py` で
    以下のように全モデルを import する必要がある::

        import app.models  # noqa: F401  -- registers all tables to Base.metadata
        from app.database import Base
        target_metadata = Base.metadata
    """


def _build_engine(settings: Settings):
    """Create SQLAlchemy engine from settings.

    pool_pre_ping=True により、コネクションの死活確認を自動実行する。
    """
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,
        echo=settings.app_debug,  # SQL ログ (development のみ)
    )


def _build_session_factory(settings: Settings) -> sessionmaker[Session]:
    engine = _build_engine(settings)
    return sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )


# ---------------------------------------------------------------------------
# Module-level singletons (初期化は import 時に 1 回だけ実行)
# ---------------------------------------------------------------------------
_settings = get_settings()
engine = _build_engine(_settings)
SessionLocal = _build_session_factory(_settings)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------
def get_db() -> Generator[Session, None, None]:
    """Yield a database session and ensure it is closed after the request.

    Usage in route::

        @router.get("/example")
        def example(db: DbSession):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Annotated shorthand for route signatures
DbSession = Annotated[Session, Depends(get_db)]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def check_db_connection() -> bool:
    """Perform a lightweight connectivity check against the database.

    Returns True if the connection succeeds, False otherwise.
    Used by the /health endpoint.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
