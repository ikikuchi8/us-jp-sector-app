"""
price_daily テーブル。

米国・日本の業種 ETF 日次価格データを保存する。
(ticker, business_date) の組み合わせで一意。
"""

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

# 価格の精度: 整数部 12桁 + 小数部 6桁
_PRICE_PRECISION = 18
_PRICE_SCALE = 6


class PriceDaily(Base):
    """日次価格データ。

    Columns:
        id:                   サロゲートキー
        ticker:               ティッカーシンボル (→ instrument_master.ticker)
        market:               上場市場 ('US' | 'JP')  ※ 非正規化して検索を容易にする
        business_date:        営業日 (取引所カレンダー基準)
        open_price:           始値
        high_price:           高値
        low_price:            安値
        close_price:          終値
        adjusted_close_price: 調整後終値 (分割・配当考慮)
        volume:               出来高
        data_source:          データ取得元 (例: 'yfinance')
        fetched_at:           データ取得日時 (UTC)

    Constraints:
        UNIQUE (ticker, business_date)  -- 同一銘柄の同一営業日は 1 レコードのみ

    Indexes:
        ix_price_daily_ticker          -- ticker 単体での検索
        ix_price_daily_business_date   -- 日付範囲での検索
    """

    __tablename__ = "price_daily"

    __table_args__ = (
        UniqueConstraint("ticker", "business_date", name="uq_price_daily_ticker_date"),
        Index("ix_price_daily_ticker", "ticker"),
        Index("ix_price_daily_business_date", "business_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ticker: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("instrument_master.ticker", ondelete="RESTRICT"),
        nullable=False,
        comment="ティッカーシンボル",
    )

    market: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="上場市場 (US | JP)  ※ 非正規化カラム",
    )

    business_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="営業日 (取引所カレンダー基準)",
    )

    open_price: Mapped[Decimal | None] = mapped_column(
        Numeric(_PRICE_PRECISION, _PRICE_SCALE),
        nullable=True,
        comment="始値",
    )

    high_price: Mapped[Decimal | None] = mapped_column(
        Numeric(_PRICE_PRECISION, _PRICE_SCALE),
        nullable=True,
        comment="高値",
    )

    low_price: Mapped[Decimal | None] = mapped_column(
        Numeric(_PRICE_PRECISION, _PRICE_SCALE),
        nullable=True,
        comment="安値",
    )

    close_price: Mapped[Decimal | None] = mapped_column(
        Numeric(_PRICE_PRECISION, _PRICE_SCALE),
        nullable=True,
        comment="終値",
    )

    adjusted_close_price: Mapped[Decimal | None] = mapped_column(
        Numeric(_PRICE_PRECISION, _PRICE_SCALE),
        nullable=True,
        comment="調整後終値",
    )

    volume: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="出来高",
    )

    data_source: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="データ取得元 (例: yfinance)",
    )

    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="データ取得日時 (UTC)",
    )

    def __repr__(self) -> str:
        return (
            f"<PriceDaily ticker={self.ticker!r}"
            f" date={self.business_date!r}"
            f" close={self.close_price!r}>"
        )
