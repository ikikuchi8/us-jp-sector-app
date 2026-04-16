"""
signal_daily テーブル。

日本業種ごとの売買シグナルを保存する。

重要な設計概念:
    us_signal_date   : 米国側でシグナルを生成した営業日。
                       この日の Close 時点で利用可能な情報のみを使う (先読み防止)。
    jp_execution_date: 日本市場で執行する営業日 (us_signal_date の翌営業日以降)。
                       Open-to-Close リターンを評価対象とする。

この 2 つの日付を分けて管理することで、日米の営業日ズレと先読みバイアスを明示的に制御する。
"""

import enum
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Index,
    JSON,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

# シグナルスコアの精度
_SCORE_PRECISION = 12
_SCORE_SCALE = 6


class SuggestedSide(str, enum.Enum):
    """売買推奨方向。

    str を継承することで JSON シリアライズ時に文字列として扱われる。
    """

    LONG = "long"
    SHORT = "short"
    NEUTRAL = "neutral"


class SignalDaily(Base):
    """日次売買シグナル。

    Columns:
        id:                  サロゲートキー
        signal_type:         シグナル種別 (例: 'simple_v1')
                             将来の PCA / 正則化版への差し替え時に区別する
        target_ticker:       対象 JP 業種 ticker (→ instrument_master.ticker)
        us_signal_date:      米国側シグナル生成日 (先読み防止の基準日)
        jp_execution_date:   日本市場執行日
        signal_score:        シグナルスコア (高いほど long 推奨)
        signal_rank:         スコアに基づく業種内ランク (1 = 最強 long)
        suggested_side:      売買推奨方向 ('long' | 'short' | 'neutral')
        input_metadata_json: シグナル生成時の入力値スナップショット (再現性確保)
        created_at:          レコード作成日時 (UTC)

    Constraints:
        UNIQUE (signal_type, target_ticker, jp_execution_date)

    Indexes:
        ix_signal_daily_target_ticker
        ix_signal_daily_us_signal_date
        ix_signal_daily_jp_execution_date
    """

    __tablename__ = "signal_daily"

    __table_args__ = (
        # jp_execution_date を一意キーの軸とする。
        # us_signal_date は US 休場時に複数の jp_execution_date から参照されうるため
        # 一意キーに含めない (含めると US 休場翌 JP 日の保存で衝突が発生する)。
        UniqueConstraint(
            "signal_type",
            "target_ticker",
            "jp_execution_date",
            name="uq_signal_daily_type_ticker_jpdate",
        ),
        Index("ix_signal_daily_target_ticker", "target_ticker"),
        Index("ix_signal_daily_us_signal_date", "us_signal_date"),
        Index("ix_signal_daily_jp_execution_date", "jp_execution_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    signal_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="シグナル種別識別子 (例: simple_v1, pca_v1)",
    )

    target_ticker: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("instrument_master.ticker", ondelete="RESTRICT"),
        nullable=False,
        comment="対象 JP 業種ティッカー",
    )

    us_signal_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="米国側シグナル生成日 (この日の Close 時点情報のみ使用)",
    )

    jp_execution_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="日本市場執行日 (Open-to-Close リターン評価対象)",
    )

    signal_score: Mapped[Decimal | None] = mapped_column(
        Numeric(_SCORE_PRECISION, _SCORE_SCALE),
        nullable=True,
        comment="シグナルスコア (高いほど long 推奨)。対応 US 銘柄が全欠損の場合は None。",
    )

    signal_rank: Mapped[int | None] = mapped_column(
        SmallInteger,
        nullable=True,
        comment="業種内ランク (1 = 最強 long, N = 最強 short)。signal_score が None の場合は None。",
    )

    suggested_side: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="売買推奨方向 (long | short | neutral)",
    )

    input_metadata_json: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="シグナル生成入力値スナップショット (再現性・デバッグ用)",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="レコード作成日時 (UTC)",
    )

    def __repr__(self) -> str:
        return (
            f"<SignalDaily type={self.signal_type!r}"
            f" ticker={self.target_ticker!r}"
            f" us_date={self.us_signal_date!r}"
            f" side={self.suggested_side!r}"
            f" rank={self.signal_rank!r}>"
        )
