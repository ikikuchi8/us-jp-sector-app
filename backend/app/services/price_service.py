"""
PriceService: yfinance → price_daily 保存のビジネスロジック層。

# 責務
  - PriceFetcher から取得した DataFrame を PriceDaily ORM オブジェクトに変換する
  - PriceRepository.upsert_many で DB に保存する
  - ticker 単位で commit する (fail-soft: 一部失敗しても他 ticker を継続)
  - FetchResult にサマリーを収集して返す

# market の決定
  v0_01 では instrument_master / seed_data の定義を正本とする。
  update_prices() は _TICKER_TO_MARKET から market を自動解決する。
  seed_data に存在しない ticker は result.failed に記録してスキップする。
  呼び出し側が market を自由入力できる API は提供しない。

# DataFrame の検証  (空 vs 不正の区別)
  - 空 DataFrame            → result.empty (データなし、エラーではない)
  - 必須列不足 / 不正形式   → result.failed (フォーマットエラー)
  - NaN 値                  → None として DB に保存 (nullable カラム)

# commit 責務
  PriceService が ticker 単位で commit する。
  PriceRepository は flush のみ担当し commit しない。
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Final

import pandas as pd
from sqlalchemy.orm import Session

from app.models.price import PriceDaily
from app.repositories.price_repository import PriceRepository
from app.seed_data.instruments import ALL_INSTRUMENTS, JP_INSTRUMENTS, US_INSTRUMENTS
from app.services.price_fetcher import (
    COL_ADJ_CLOSE,
    COL_CLOSE,
    COL_HIGH,
    COL_LOW,
    COL_OPEN,
    COL_VOLUME,
    PriceFetcher,
    YFinanceFetcher,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# モジュール定数
# ---------------------------------------------------------------------------

# seed_data を正本として ticker → market のマッピングを構築する
# 呼び出し側が market を自由入力する API は v0_01 では提供しない
_TICKER_TO_MARKET: Final[dict[str, str]] = {
    row["ticker"]: row["market"] for row in ALL_INSTRUMENTS
}

# upsert に必要な全必須列 (DataFrame 検証に使用)
_REQUIRED_COLS: Final[frozenset[str]] = frozenset({
    COL_OPEN, COL_HIGH, COL_LOW, COL_CLOSE, COL_ADJ_CLOSE, COL_VOLUME,
})

_DATA_SOURCE: Final[str] = "yfinance"


# ---------------------------------------------------------------------------
# 返り値型
# ---------------------------------------------------------------------------

@dataclass
class FetchResult:
    """update_prices() の実行結果サマリー。

    Attributes:
        requested:  要求 ticker 数
        saved_rows: DB に保存した行数 (upsert_many の戻り値の合計)
        succeeded:  正常に保存できた ticker のリスト
        failed:     失敗した ticker → エラーメッセージ の dict
        empty:      データなし ticker のリスト (エラーではない)
    """

    requested: int = 0
    saved_rows: int = 0
    succeeded: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    empty: list[str] = field(default_factory=list)

    @property
    def has_failure(self) -> bool:
        """1 件以上の失敗があれば True。"""
        return bool(self.failed)

    def __add__(self, other: FetchResult) -> FetchResult:
        """update_all_prices で US と JP の結果を合算するための演算子。"""
        return FetchResult(
            requested=self.requested + other.requested,
            saved_rows=self.saved_rows + other.saved_rows,
            succeeded=self.succeeded + other.succeeded,
            failed={**self.failed, **other.failed},
            empty=self.empty + other.empty,
        )


# ---------------------------------------------------------------------------
# PriceService
# ---------------------------------------------------------------------------

class PriceService:
    """yfinance → price_daily への取得・保存を担うサービス。

    Args:
        session:    SQLAlchemy Session。commit は PriceService が ticker 単位で行う。
        fetcher:    PriceFetcher 実装。None の場合は YFinanceFetcher を使用する。
        repository: PriceRepository 実装。None の場合は session から自動生成する。
                    テストでモックを注入する際に使用する。
    """

    def __init__(
        self,
        session: Session,
        fetcher: PriceFetcher | None = None,
        repository: PriceRepository | None = None,
    ) -> None:
        self._session = session
        self._fetcher: PriceFetcher = fetcher or YFinanceFetcher()
        self._repo: PriceRepository = repository or PriceRepository(session)

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------

    def update_prices(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> FetchResult:
        """指定した ticker リストの価格を取得して DB に保存する。

        # market の解決
          各 ticker の market は _TICKER_TO_MARKET (seed_data 由来) から決定する。
          seed_data に存在しない ticker は result.failed に記録してスキップする。

        # DataFrame 検証
          空 DataFrame         → result.empty (エラーでない)
          必須列不足 / 不正    → result.failed
          NaN 値               → None として DB に保存

        # commit
          ticker 単位で commit する (fail-soft)。
          1 ticker が失敗しても他 ticker の保存は継続する。

        Args:
            tickers: 取得する ticker リスト。
            start:   取得開始日 (inclusive)。
            end:     取得終了日 (inclusive)。

        Returns:
            FetchResult サマリー。
        """
        result = FetchResult(requested=len(tickers))

        if start > end:
            logger.warning(
                "update_prices: start > end のためスキップ (start=%s end=%s)", start, end
            )
            return result

        for ticker in tickers:
            self._process_ticker(ticker, start, end, result)

        logger.info(
            "update_prices 完了: requested=%d succeeded=%d failed=%d empty=%d saved_rows=%d",
            result.requested,
            len(result.succeeded),
            len(result.failed),
            len(result.empty),
            result.saved_rows,
        )
        return result

    def update_us_prices(self, start: date, end: date) -> FetchResult:
        """US 11 業種 (XLB〜XLY) を一括更新する。

        取得対象は seed_data.US_INSTRUMENTS の ticker リストを使用する。
        """
        tickers = [row["ticker"] for row in US_INSTRUMENTS]
        logger.info("update_us_prices: %d 銘柄 (%s〜%s)", len(tickers), start, end)
        return self.update_prices(tickers, start, end)

    def update_jp_prices(self, start: date, end: date) -> FetchResult:
        """JP 17 業種 (1617.T〜1633.T) を一括更新する。

        取得対象は seed_data.JP_INSTRUMENTS の ticker リストを使用する。
        """
        tickers = [row["ticker"] for row in JP_INSTRUMENTS]
        logger.info("update_jp_prices: %d 銘柄 (%s〜%s)", len(tickers), start, end)
        return self.update_prices(tickers, start, end)

    def update_all_prices(self, start: date, end: date) -> FetchResult:
        """US + JP 全 28 銘柄を一括更新する。

        US と JP の FetchResult を合算して返す。
        """
        logger.info("update_all_prices: %s〜%s", start, end)
        us_result = self.update_us_prices(start, end)
        jp_result = self.update_jp_prices(start, end)
        return us_result + jp_result

    # ------------------------------------------------------------------
    # プライベート: ticker 単位処理
    # ------------------------------------------------------------------

    def _process_ticker(
        self,
        ticker: str,
        start: date,
        end: date,
        result: FetchResult,
    ) -> None:
        """1 ticker の取得・変換・保存を行い result を更新する。

        すべての分岐で result の succeeded / failed / empty のいずれかに記録する。
        """
        # 1. market を seed_data から解決 (自由入力は許可しない)
        market = _TICKER_TO_MARKET.get(ticker)
        if market is None:
            result.failed[ticker] = f"seed_data に未登録の ticker: {ticker!r}"
            logger.warning("未登録 ticker をスキップ: %s", ticker)
            return

        # 2. fetch (ネットワークエラーは例外として伝播してくる)
        try:
            df = self._fetcher.fetch(ticker, start, end)
        except Exception as exc:
            result.failed[ticker] = f"fetch エラー: {exc}"
            logger.warning("fetch 失敗 ticker=%s: %s", ticker, exc, exc_info=True)
            return

        # 3. 空 DataFrame → empty 扱い (エラーではない)
        if df.empty:
            result.empty.append(ticker)
            logger.info("空データ ticker=%s: empty に記録", ticker)
            return

        # 4. 必須列検証 → 不足なら failed 扱い
        validation_error = _validate_columns(df)
        if validation_error:
            result.failed[ticker] = validation_error
            logger.warning("不正 DataFrame ticker=%s: %s", ticker, validation_error)
            return

        # 5. ORM 変換
        rows = _to_price_daily(df, ticker, market)

        # 6. upsert + commit (ticker 単位の fail-soft)
        try:
            self._repo.upsert_many(rows)
            self._session.commit()
            result.succeeded.append(ticker)
            result.saved_rows += len(rows)
            logger.debug("保存完了 ticker=%s rows=%d", ticker, len(rows))
        except Exception as exc:
            self._session.rollback()
            result.failed[ticker] = f"保存エラー: {exc}"
            logger.warning("保存失敗 ticker=%s: %s", ticker, exc, exc_info=True)


# ---------------------------------------------------------------------------
# モジュールプライベート: 検証・変換ヘルパー
# ---------------------------------------------------------------------------

def _validate_columns(df: pd.DataFrame) -> str | None:
    """必須列の存在を検証する。

    Returns:
        問題なし → None
        必須列不足 → エラーメッセージ文字列 (result.failed に格納する用途)
    """
    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        return f"必須列が不足: {sorted(missing)}"
    return None


def _to_price_daily(
    df: pd.DataFrame,
    ticker: str,
    market: str,
    data_source: str = _DATA_SOURCE,
) -> list[PriceDaily]:
    """DataFrame → PriceDaily のリストに変換する。

    Args:
        df:          標準列名済みの DataFrame (open, high, low, close, adj_close, volume)
        ticker:      ティッカーシンボル
        market:      "US" | "JP"
        data_source: データ取得元ラベル (デフォルト: "yfinance")

    Returns:
        PriceDaily のリスト。行数 = df の行数。
        NaN / None 値は None として変換する (nullable カラム)。

    Note:
        id は設定しない。PostgreSQL は SERIAL で自動採番する。
        SQLite テストでは upsert_many の mock を使うため id 不要。
    """
    fetched_at = datetime.now(tz=timezone.utc)
    rows: list[PriceDaily] = []

    for idx, row in df.iterrows():
        # DatetimeIndex の場合 .date() で date 型に変換する
        business_date: date = idx.date() if hasattr(idx, "date") else idx

        rows.append(
            PriceDaily(
                ticker=ticker,
                market=market,
                business_date=business_date,
                open_price=_to_decimal(row[COL_OPEN]),
                high_price=_to_decimal(row[COL_HIGH]),
                low_price=_to_decimal(row[COL_LOW]),
                close_price=_to_decimal(row[COL_CLOSE]),
                adjusted_close_price=_to_decimal(row[COL_ADJ_CLOSE]),
                volume=_to_int(row[COL_VOLUME]),
                data_source=data_source,
                fetched_at=fetched_at,
            )
        )

    return rows


def _to_decimal(v: object) -> Decimal | None:
    """pandas 値 (float / numpy.float64 / NaN / None) → Decimal | None に変換する。

    NaN および None は None として返す。
    """
    if pd.isna(v):
        return None
    return Decimal(str(v))


def _to_int(v: object) -> int | None:
    """pandas 値 → int | None に変換する。NaN / None は None。"""
    if pd.isna(v):
        return None
    return int(v)
