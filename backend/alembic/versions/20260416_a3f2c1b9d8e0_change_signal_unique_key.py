"""change signal_daily unique key and nullable columns

Revision ID: a3f2c1b9d8e0
Revises: f3a8b2c19e05
Create Date: 2026-04-16 00:00:00.000000 UTC

変更内容:
    1. UNIQUE (signal_type, target_ticker, us_signal_date) を削除し
       UNIQUE (signal_type, target_ticker, jp_execution_date) を追加する

    2. signal_score を nullable=True に変更する

    3. signal_rank を nullable=True に変更する

変更理由:
    [UNIQUE キー変更]
    US 休場日の翌 JP 営業日に「前の JP 営業日と同一の us_signal_date」が割り当てられる
    ケースが年間 5〜8 回発生する。us_signal_date ベースの UNIQUE では 2 日目の保存時に
    衝突が起き、その JP 日のシグナルを記録できない。
    jp_execution_date は JP 側取引の主軸であり、各 JP 営業日に対して 1 シグナル行が
    対応する設計が自然かつ正確。

    [signal_score / signal_rank の nullable 化]
    対応する US 銘柄が全欠損の JP 業種は score / rank を算出できない。
    sentinel 値 (0 等) よりも None の方が表現が正確で扱いやすい。
    suggested_side は "neutral" を保存するため NOT NULL のまま変更しない。

前提:
    signal_daily テーブルは空 (signal_service 未実装のため実データなし)。
    既存行へのデータ変換処理は不要。
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# ---------------------------------------------------------------------------
# Alembic revision identifiers
# ---------------------------------------------------------------------------
revision: str = "a3f2c1b9d8e0"
down_revision: Union[str, None] = "f3a8b2c19e05"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. 旧 UNIQUE 制約を削除
    op.drop_constraint(
        "uq_signal_daily_type_ticker_date",
        "signal_daily",
        type_="unique",
    )

    # 2. 新 UNIQUE 制約を追加 (jp_execution_date 軸)
    op.create_unique_constraint(
        "uq_signal_daily_type_ticker_jpdate",
        "signal_daily",
        ["signal_type", "target_ticker", "jp_execution_date"],
    )

    # 3. signal_score を nullable に変更
    op.alter_column(
        "signal_daily",
        "signal_score",
        existing_type=sa.Numeric(precision=12, scale=6),
        nullable=True,
        comment="シグナルスコア (高いほど long 推奨)。対応 US 銘柄が全欠損の場合は None。",
    )

    # 4. signal_rank を nullable に変更
    op.alter_column(
        "signal_daily",
        "signal_rank",
        existing_type=sa.SmallInteger(),
        nullable=True,
        comment="業種内ランク (1 = 最強 long, N = 最強 short)。signal_score が None の場合は None。",
    )


def downgrade() -> None:
    # 前提: downgrade 実行前に signal_daily が空であること。
    # signal_score / signal_rank に NULL 値が存在すると NOT NULL 変更が失敗する。

    # 4. signal_rank を NOT NULL に戻す
    op.alter_column(
        "signal_daily",
        "signal_rank",
        existing_type=sa.SmallInteger(),
        nullable=False,
        comment="業種内ランク (1 = 最強 long, N = 最強 short)",
    )

    # 3. signal_score を NOT NULL に戻す
    op.alter_column(
        "signal_daily",
        "signal_score",
        existing_type=sa.Numeric(precision=12, scale=6),
        nullable=False,
        comment="シグナルスコア (高いほど long 推奨)",
    )

    # 2. 新 UNIQUE 制約を削除
    op.drop_constraint(
        "uq_signal_daily_type_ticker_jpdate",
        "signal_daily",
        type_="unique",
    )

    # 1. 旧 UNIQUE 制約を復元
    op.create_unique_constraint(
        "uq_signal_daily_type_ticker_date",
        "signal_daily",
        ["signal_type", "target_ticker", "us_signal_date"],
    )
