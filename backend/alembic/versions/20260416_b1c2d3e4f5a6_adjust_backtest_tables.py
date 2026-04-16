"""adjust backtest_run and backtest_result_daily for v0_01

Revision ID: b1c2d3e4f5a6
Revises: a3f2c1b9d8e0
Create Date: 2026-04-16 00:00:00.000000 UTC

変更内容:
    [backtest_result_daily]
    1. business_date カラムを jp_execution_date にリネームする
       (signal_daily.jp_execution_date との命名統一)
    2. daily_return を nullable=True に変更する
       (シグナル・価格が全欠損の日は None)
    3. cumulative_return を nullable=True に変更する
       (スキップ日のキャリーオーバー表現のため)
    4. long_return カラムを追加する (nullable)
    5. short_return カラムを追加する (nullable)
    6. note カラムを削除する (v0_01 不要)
    7. ユニーク制約 uq_backtest_result_run_date のカラム参照を更新する
    8. インデックス ix_backtest_result_daily_business_date をリネームする

    [backtest_run]
    9.  start_date カラムを追加する (nullable)
    10. end_date カラムを追加する (nullable)
    11. commission_rate カラムを追加する (nullable)
    12. slippage_rate カラムを追加する (nullable)
    13. trading_days カラムを追加する (nullable)
    14. total_return カラムを追加する (nullable)
    15. annual_return カラムを追加する (nullable)
    16. annual_vol カラムを追加する (nullable)
    17. sharpe_ratio カラムを追加する (nullable)
    18. max_drawdown カラムを追加する (nullable)
    19. win_rate カラムを追加する (nullable)

変更理由:
    [カラムリネーム]
    signal_daily は jp_execution_date を使って JP 執行日を表す。
    backtest_result_daily も同じ概念の日付を保存するため、命名を統一する。

    [daily_return / cumulative_return の nullable 化]
    シグナルが存在しない日・JP 価格が全欠損の日は daily_return を計算できない。
    sentinel 値 (0.0) を使うと計算不能と正常値が区別できなくなるため None を使う。

    [long_return / short_return 追加]
    ロング・ショート各ブックのリターンを個別に記録することで
    BacktestService 以外でも分析が可能になる。

    [note 削除]
    v0_01 では使用しない。

    [BacktestRun へのカラム追加]
    実行パラメータ (start_date, end_date, cost) と
    完了後のサマリー統計を構造化カラムとして保持することで
    BacktestRepository.get_run() で即座に取得できるようにする。

前提:
    backtest_run / backtest_result_daily テーブルは空 (BacktestService 未実装のため実データなし)。
    既存行へのデータ変換処理は不要。
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# ---------------------------------------------------------------------------
# Alembic revision identifiers
# ---------------------------------------------------------------------------
revision: str = "b1c2d3e4f5a6"
down_revision: Union[str, None] = "a3f2c1b9d8e0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ==============================================================
    # backtest_result_daily の変更
    # ==============================================================

    # 1. 旧インデックスを先に削除 (カラムリネーム前)
    op.drop_index(
        "ix_backtest_result_daily_business_date",
        table_name="backtest_result_daily",
    )

    # 2. business_date → jp_execution_date にリネーム
    op.alter_column(
        "backtest_result_daily",
        "business_date",
        new_column_name="jp_execution_date",
        existing_type=sa.Date(),
        existing_nullable=False,
        comment="対象営業日 (signal_daily.jp_execution_date 基準)",
    )

    # 3. daily_return を nullable に変更
    op.alter_column(
        "backtest_result_daily",
        "daily_return",
        existing_type=sa.Numeric(precision=12, scale=6),
        nullable=True,
        comment="日次ポートフォリオリターン (コスト考慮後)。全欠損日は None。",
    )

    # 4. cumulative_return を nullable に変更
    op.alter_column(
        "backtest_result_daily",
        "cumulative_return",
        existing_type=sa.Numeric(precision=12, scale=6),
        nullable=True,
        comment="期間開始からの累積リターン。スキップ日はキャリーオーバー。",
    )

    # 5. long_return カラムを追加
    op.add_column(
        "backtest_result_daily",
        sa.Column(
            "long_return",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
            comment="long ブック平均リターン。有効銘柄なし / 欠損時は None。",
        ),
    )

    # 6. short_return カラムを追加
    op.add_column(
        "backtest_result_daily",
        sa.Column(
            "short_return",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
            comment="short ブック平均リターン。有効銘柄なし / 欠損時は None。",
        ),
    )

    # 7. note カラムを削除
    op.drop_column("backtest_result_daily", "note")

    # 8. リネーム後の新インデックスを作成
    op.create_index(
        "ix_backtest_result_daily_jp_execution_date",
        "backtest_result_daily",
        ["jp_execution_date"],
    )

    # ユニーク制約 uq_backtest_result_run_date は business_date → jp_execution_date
    # のカラムリネームにより自動的に新カラム名を参照する (PostgreSQL 挙動)。
    # 制約名は変更しない。

    # ==============================================================
    # backtest_run の変更 (カラム追加のみ)
    # ==============================================================

    # 9-10. 実行期間
    op.add_column(
        "backtest_run",
        sa.Column(
            "start_date",
            sa.Date(),
            nullable=True,
            comment="バックテスト開始日 (jp_execution_date 基準)",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "end_date",
            sa.Date(),
            nullable=True,
            comment="バックテスト終了日 (jp_execution_date 基準)",
        ),
    )

    # 11-12. コストパラメータ
    op.add_column(
        "backtest_run",
        sa.Column(
            "commission_rate",
            sa.Numeric(precision=8, scale=6),
            nullable=True,
            comment="ラウンドトリップ手数料率 (例: 0.001000 = 0.1%)",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "slippage_rate",
            sa.Numeric(precision=8, scale=6),
            nullable=True,
            comment="片道エントリースリッページ率 (例: 0.001000 = 0.1%)",
        ),
    )

    # 13-19. サマリー統計
    op.add_column(
        "backtest_run",
        sa.Column(
            "trading_days",
            sa.SmallInteger(),
            nullable=True,
            comment="実際に計算できた日数 (skipped 日を除く)",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "total_return",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
            comment="最終累積リターン",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "annual_return",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
            comment="年率リターン (252 日ベース)",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "annual_vol",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
            comment="年率ボラティリティ (252 日ベース)",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "sharpe_ratio",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
            comment="シャープレシオ (annual_return / annual_vol)",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "max_drawdown",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
            comment="最大ドローダウン (正値, 例: 0.15 = 15%)",
        ),
    )
    op.add_column(
        "backtest_run",
        sa.Column(
            "win_rate",
            sa.Numeric(precision=8, scale=6),
            nullable=True,
            comment="勝率 (daily_return > 0 の日数 / trading_days)",
        ),
    )


def downgrade() -> None:
    # 前提: downgrade 実行前に backtest_run / backtest_result_daily が空であること。
    # daily_return / cumulative_return に NULL 値が存在すると NOT NULL 変更が失敗する。

    # ==============================================================
    # backtest_run のカラム削除
    # ==============================================================
    for col in (
        "win_rate",
        "max_drawdown",
        "sharpe_ratio",
        "annual_vol",
        "annual_return",
        "total_return",
        "trading_days",
        "slippage_rate",
        "commission_rate",
        "end_date",
        "start_date",
    ):
        op.drop_column("backtest_run", col)

    # ==============================================================
    # backtest_result_daily の変更を逆順に戻す
    # ==============================================================

    # 新インデックスを削除
    op.drop_index(
        "ix_backtest_result_daily_jp_execution_date",
        table_name="backtest_result_daily",
    )

    # note カラムを復元
    op.add_column(
        "backtest_result_daily",
        sa.Column("note", sa.Text(), nullable=True, comment="補足メモ (任意)"),
    )

    # long_return / short_return を削除
    op.drop_column("backtest_result_daily", "short_return")
    op.drop_column("backtest_result_daily", "long_return")

    # cumulative_return を NOT NULL に戻す
    op.alter_column(
        "backtest_result_daily",
        "cumulative_return",
        existing_type=sa.Numeric(precision=12, scale=6),
        nullable=False,
        comment="期間開始からの累積リターン",
    )

    # daily_return を NOT NULL に戻す
    op.alter_column(
        "backtest_result_daily",
        "daily_return",
        existing_type=sa.Numeric(precision=12, scale=6),
        nullable=False,
        comment="日次ポートフォリオリターン (手数料・スリッページ考慮後)",
    )

    # jp_execution_date → business_date に戻す
    op.alter_column(
        "backtest_result_daily",
        "jp_execution_date",
        new_column_name="business_date",
        existing_type=sa.Date(),
        existing_nullable=False,
        comment="対象営業日 (jp_execution_date 基準)",
    )

    # 旧インデックスを復元
    op.create_index(
        "ix_backtest_result_daily_business_date",
        "backtest_result_daily",
        ["business_date"],
    )
