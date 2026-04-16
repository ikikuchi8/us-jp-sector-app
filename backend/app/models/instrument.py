"""
instrument_master テーブル。

米国 / 日本の業種 ETF マスタを管理する。
ticker をビジネスキーとして他テーブルから参照する。
"""

import enum
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Market(str, enum.Enum):
    """対象市場区分。

    str を継承することで JSON シリアライズ時に文字列として扱われる。
    """

    US = "US"
    JP = "JP"


class InstrumentMaster(Base):
    """業種 ETF マスタ。

    Columns:
        id:              サロゲートキー
        ticker:          ティッカーシンボル (ビジネスキー / UNIQUE)
                         米国例: XLB, XLE  日本例: 1615.T
        market:          上場市場 ('US' | 'JP')
        instrument_name: 銘柄名称 (例: "Materials Select Sector SPDR Fund")
        sector_name:     業種名称 (例: "素材")
        is_active:       有効フラグ (論理削除用)
        created_at:      レコード作成日時 (UTC)
    """

    __tablename__ = "instrument_master"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ticker: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        unique=True,
        comment="ティッカーシンボル (ビジネスキー)",
    )

    market: Mapped[str] = mapped_column(
        Enum(Market, name="market_type", create_type=True),
        nullable=False,
        comment="上場市場 (US | JP)",
    )

    instrument_name: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
        comment="銘柄正式名称",
    )

    sector_name: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
        comment="業種名称",
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        comment="有効フラグ (False = 論理削除)",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="レコード作成日時 (UTC)",
    )

    def __repr__(self) -> str:
        return f"<InstrumentMaster ticker={self.ticker!r} market={self.market!r}>"
