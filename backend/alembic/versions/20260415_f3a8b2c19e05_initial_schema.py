"""initial schema

Revision ID: f3a8b2c19e05
Revises: (なし / 最初のマイグレーション)
Create Date: 2026-04-15 00:00:00.000000 UTC

作成するテーブル:
    1. instrument_master        -- 業種 ETF マスタ
    2. price_daily              -- 日次価格データ
    3. signal_daily             -- 日次売買シグナル
    4. backtest_run             -- バックテスト実行管理
    5. backtest_result_daily    -- バックテスト日次損益

作成する PostgreSQL ENUM 型:
    - market_type: 'US' | 'JP'  (instrument_master.market で使用)

作成順序の根拠:
    - instrument_master が FK の親のため最初に作成
    - backtest_result_daily が backtest_run に依存するため最後に作成
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# ---------------------------------------------------------------------------
# Alembic revision identifiers
# ---------------------------------------------------------------------------
revision: str = "f3a8b2c19e05"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------ #
    # 1. instrument_master
    #    - market カラムに PostgreSQL native ENUM (market_type) を使用
    #    - ENUM 型は op.create_table() 内の sa.Enum() が自動生成する
    # ------------------------------------------------------------------ #
    op.create_table(
        "instrument_master",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "ticker",
            sa.String(length=20),
            nullable=False,
            comment="ティッカーシンボル (ビジネスキー)",
        ),
        sa.Column(
            "market",
            sa.Enum("US", "JP", name="market_type"),
            nullable=False,
            comment="上場市場 (US | JP)",
        ),
        sa.Column(
            "instrument_name",
            sa.String(length=200),
            nullable=False,
            comment="銘柄正式名称",
        ),
        sa.Column(
            "sector_name",
            sa.String(length=200),
            nullable=False,
            comment="業種名称",
        ),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
            comment="有効フラグ (False = 論理削除)",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="レコード作成日時 (UTC)",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_instrument_master")),
        sa.UniqueConstraint("ticker", name=op.f("uq_instrument_master_ticker")),
    )

    # ------------------------------------------------------------------ #
    # 2. price_daily
    #    - ticker は instrument_master.ticker を参照 (ON DELETE RESTRICT)
    #    - market は文字列カラム (非正規化。ENUM 型は使わない)
    # ------------------------------------------------------------------ #
    op.create_table(
        "price_daily",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "ticker",
            sa.String(length=20),
            nullable=False,
            comment="ティッカーシンボル",
        ),
        sa.Column(
            "market",
            sa.String(length=10),
            nullable=False,
            comment="上場市場 (US | JP)  ※ 非正規化カラム",
        ),
        sa.Column(
            "business_date",
            sa.Date(),
            nullable=False,
            comment="営業日 (取引所カレンダー基準)",
        ),
        sa.Column(
            "open_price",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="始値",
        ),
        sa.Column(
            "high_price",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="高値",
        ),
        sa.Column(
            "low_price",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="安値",
        ),
        sa.Column(
            "close_price",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="終値",
        ),
        sa.Column(
            "adjusted_close_price",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
            comment="調整後終値",
        ),
        sa.Column(
            "volume",
            sa.BigInteger(),
            nullable=True,
            comment="出来高",
        ),
        sa.Column(
            "data_source",
            sa.String(length=50),
            nullable=False,
            comment="データ取得元 (例: yfinance)",
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="データ取得日時 (UTC)",
        ),
        sa.ForeignKeyConstraint(
            ["ticker"],
            ["instrument_master.ticker"],
            name=op.f("fk_price_daily_ticker_instrument_master"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_price_daily")),
        sa.UniqueConstraint(
            "ticker",
            "business_date",
            name="uq_price_daily_ticker_date",
        ),
    )
    op.create_index("ix_price_daily_ticker", "price_daily", ["ticker"])
    op.create_index("ix_price_daily_business_date", "price_daily", ["business_date"])

    # ------------------------------------------------------------------ #
    # 3. signal_daily
    #    - target_ticker は instrument_master.ticker を参照 (ON DELETE RESTRICT)
    #    - suggested_side は文字列カラム (将来の値追加を DDL 変更なしに対応)
    #    - us_signal_date と jp_execution_date を分離して先読みバイアスを防止
    # ------------------------------------------------------------------ #
    op.create_table(
        "signal_daily",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "signal_type",
            sa.String(length=50),
            nullable=False,
            comment="シグナル種別識別子 (例: simple_v1, pca_v1)",
        ),
        sa.Column(
            "target_ticker",
            sa.String(length=20),
            nullable=False,
            comment="対象 JP 業種ティッカー",
        ),
        sa.Column(
            "us_signal_date",
            sa.Date(),
            nullable=False,
            comment="米国側シグナル生成日 (この日の Close 時点情報のみ使用)",
        ),
        sa.Column(
            "jp_execution_date",
            sa.Date(),
            nullable=False,
            comment="日本市場執行日 (Open-to-Close リターン評価対象)",
        ),
        sa.Column(
            "signal_score",
            sa.Numeric(precision=12, scale=6),
            nullable=False,
            comment="シグナルスコア (高いほど long 推奨)",
        ),
        sa.Column(
            "signal_rank",
            sa.SmallInteger(),
            nullable=False,
            comment="業種内ランク (1 = 最強 long, N = 最強 short)",
        ),
        sa.Column(
            "suggested_side",
            sa.String(length=10),
            nullable=False,
            comment="売買推奨方向 (long | short | neutral)",
        ),
        sa.Column(
            "input_metadata_json",
            sa.JSON(),
            nullable=True,
            comment="シグナル生成入力値スナップショット (再現性・デバッグ用)",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="レコード作成日時 (UTC)",
        ),
        sa.ForeignKeyConstraint(
            ["target_ticker"],
            ["instrument_master.ticker"],
            name=op.f("fk_signal_daily_target_ticker_instrument_master"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_signal_daily")),
        sa.UniqueConstraint(
            "signal_type",
            "target_ticker",
            "us_signal_date",
            name="uq_signal_daily_type_ticker_date",
        ),
    )
    op.create_index("ix_signal_daily_target_ticker", "signal_daily", ["target_ticker"])
    op.create_index("ix_signal_daily_us_signal_date", "signal_daily", ["us_signal_date"])
    op.create_index(
        "ix_signal_daily_jp_execution_date", "signal_daily", ["jp_execution_date"]
    )

    # ------------------------------------------------------------------ #
    # 4. backtest_run
    #    - status は文字列カラム (running | done | failed)
    #    - parameter_json に start_date, end_date, cost_bps 等を格納
    # ------------------------------------------------------------------ #
    op.create_table(
        "backtest_run",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "run_name",
            sa.String(length=200),
            nullable=True,
            comment="実行名称 (任意)",
        ),
        sa.Column(
            "signal_type",
            sa.String(length=50),
            nullable=False,
            comment="使用したシグナル種別",
        ),
        sa.Column(
            "parameter_json",
            sa.JSON(),
            nullable=False,
            comment="実行パラメータ (start_date, end_date, cost_bps 等)",
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="実行開始日時 (UTC)",
        ),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="実行完了日時 (UTC)  -- 実行中は NULL",
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            comment="実行ステータス (running | done | failed)",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_backtest_run")),
    )

    # ------------------------------------------------------------------ #
    # 5. backtest_result_daily
    #    - backtest_run_id は backtest_run.id を参照 (ON DELETE CASCADE)
    #    - 親 run を削除すると日次結果も連鎖削除される
    # ------------------------------------------------------------------ #
    op.create_table(
        "backtest_result_daily",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "backtest_run_id",
            sa.Integer(),
            nullable=False,
            comment="親 BacktestRun の id",
        ),
        sa.Column(
            "business_date",
            sa.Date(),
            nullable=False,
            comment="対象営業日 (jp_execution_date 基準)",
        ),
        sa.Column(
            "daily_return",
            sa.Numeric(precision=12, scale=6),
            nullable=False,
            comment="日次ポートフォリオリターン (手数料・スリッページ考慮後)",
        ),
        sa.Column(
            "cumulative_return",
            sa.Numeric(precision=12, scale=6),
            nullable=False,
            comment="期間開始からの累積リターン",
        ),
        sa.Column(
            "long_count",
            sa.SmallInteger(),
            nullable=True,
            comment="その日のロングポジション数",
        ),
        sa.Column(
            "short_count",
            sa.SmallInteger(),
            nullable=True,
            comment="その日のショートポジション数",
        ),
        sa.Column(
            "note",
            sa.Text(),
            nullable=True,
            comment="補足メモ (任意)",
        ),
        sa.ForeignKeyConstraint(
            ["backtest_run_id"],
            ["backtest_run.id"],
            name=op.f("fk_backtest_result_daily_backtest_run_id_backtest_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_backtest_result_daily")),
        sa.UniqueConstraint(
            "backtest_run_id",
            "business_date",
            name="uq_backtest_result_run_date",
        ),
    )
    op.create_index(
        "ix_backtest_result_daily_run_id", "backtest_result_daily", ["backtest_run_id"]
    )
    op.create_index(
        "ix_backtest_result_daily_business_date",
        "backtest_result_daily",
        ["business_date"],
    )


def downgrade() -> None:
    # テーブル削除は FK 依存順の逆順で行う
    op.drop_index("ix_backtest_result_daily_business_date", table_name="backtest_result_daily")
    op.drop_index("ix_backtest_result_daily_run_id", table_name="backtest_result_daily")
    op.drop_table("backtest_result_daily")

    op.drop_table("backtest_run")

    op.drop_index("ix_signal_daily_jp_execution_date", table_name="signal_daily")
    op.drop_index("ix_signal_daily_us_signal_date", table_name="signal_daily")
    op.drop_index("ix_signal_daily_target_ticker", table_name="signal_daily")
    op.drop_table("signal_daily")

    op.drop_index("ix_price_daily_business_date", table_name="price_daily")
    op.drop_index("ix_price_daily_ticker", table_name="price_daily")
    op.drop_table("price_daily")

    op.drop_table("instrument_master")

    # PostgreSQL ENUM 型を削除 (instrument_master 削除後に実行)
    sa.Enum(name="market_type").drop(op.get_bind(), checkfirst=True)
