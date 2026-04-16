"""
Alembic マイグレーション実行環境。

役割:
1. backend/ を sys.path に追加し、app パッケージを import 可能にする
2. app.models を import して全 ORM モデルを Base.metadata に登録する
3. app.config.Settings から database_url を取得し Alembic に注入する
4. オフライン / オンライン両モードでのマイグレーション実行を提供する

注意:
- alembic.ini の sqlalchemy.url は placeholder。
  実際の URL は get_settings().database_url で上書きされる。
- autogenerate を使う場合は必ず DB を起動した状態で実行すること。
"""

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------------
# sys.path 設定
# ---------------------------------------------------------------------------
# backend/ ディレクトリを sys.path の先頭に追加する。
# alembic.ini の prepend_sys_path = . と二重になるが明示的に保持する。
_backend_dir = Path(__file__).parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

# ---------------------------------------------------------------------------
# アプリケーションモジュールの import
# ---------------------------------------------------------------------------
# app.models を import することで、全 ORM モデルが Base.metadata に登録される。
# この import がないと autogenerate でテーブルが検出されない。
import app.models  # noqa: F401

from app.config import get_settings
from app.database import Base

# ---------------------------------------------------------------------------
# Alembic 設定
# ---------------------------------------------------------------------------
config = context.config

# alembic.ini の logging 設定を適用
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# .env ベースの database_url を Alembic に注入 (alembic.ini の値を上書き)
_settings = get_settings()
config.set_main_option("sqlalchemy.url", _settings.database_url)

# autogenerate のターゲット: 全登録モデルのメタデータ
target_metadata = Base.metadata


# ---------------------------------------------------------------------------
# マイグレーション実行関数
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    """オフラインモード: DB 接続なしで SQL スクリプトを標準出力に書き出す。

    Usage::

        alembic upgrade head --sql > migration.sql
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,           # カラム型の変更を検出
        compare_server_default=True, # server_default の変更を検出
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """オンラインモード: DB に接続してマイグレーションを直接実行する。

    NullPool を使うことでマイグレーション後にコネクションを即座に解放する。
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
