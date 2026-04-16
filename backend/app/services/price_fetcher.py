"""
PriceFetcher: 外部価格データ取得の抽象と yfinance 具体実装。

# Protocol 定義: PriceFetcher
  - fetch(ticker, start, end) → pd.DataFrame
  - 返り値の列名は標準化済み: open, high, low, close, adj_close, volume
  - データなし時 → 空 DataFrame を返す (例外を発生させない)
  - ネットワークエラー等 → 例外を re-raise (PriceService がキャッチして result.failed へ)

# YFinanceFetcher
  - yf.download(auto_adjust=False) で Open / High / Low / Close / Adj Close / Volume を取得
  - end は yfinance の exclusive 仕様に合わせて +1 日して渡す
  - MultiIndex 対応: 単一 ticker でも yfinance バージョンによって MultiIndex が生成されることがある
  - yfinance の lazy import により、MockFetcher を使うテストで yfinance の起動コストを回避する
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Final, Protocol

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 標準列名定数  (PriceService が参照する列名の正本)
# ---------------------------------------------------------------------------

COL_OPEN: Final[str] = "open"
COL_HIGH: Final[str] = "high"
COL_LOW: Final[str] = "low"
COL_CLOSE: Final[str] = "close"
COL_ADJ_CLOSE: Final[str] = "adj_close"
COL_VOLUME: Final[str] = "volume"

# yfinance 列名 → 標準列名 のマッピング
_YFINANCE_COL_MAP: Final[dict[str, str]] = {
    "Open": COL_OPEN,
    "High": COL_HIGH,
    "Low": COL_LOW,
    "Close": COL_CLOSE,
    "Adj Close": COL_ADJ_CLOSE,
    "Volume": COL_VOLUME,
}


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

class PriceFetcher(Protocol):
    """価格データ取得の抽象インターフェース。

    YFinanceFetcher はこの Protocol を構造的に満たす。
    テストでは MockFetcher で差し替える。
    """

    def fetch(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """1 ticker 分の価格データを返す。

        Args:
            ticker: ティッカーシンボル (例: "XLB", "1617.T")
            start:  取得開始日 (inclusive)
            end:    取得終了日 (inclusive)

        Returns:
            columns: open, high, low, close, adj_close, volume
            index:   DatetimeIndex (営業日)
            データなし: 空 DataFrame (例外を発生させない)

        Raises:
            Exception: ネットワークエラー等、取得自体が失敗した場合。
                       PriceService がキャッチして result.failed に記録する。
        """
        ...


# ---------------------------------------------------------------------------
# yfinance 実装
# ---------------------------------------------------------------------------

class YFinanceFetcher:
    """yfinance を使った PriceFetcher の具体実装。

    Note:
        yfinance は fetch() 呼び出し時に lazy import する。
        MockFetcher を使うテストでは yfinance のロードが発生しない。
    """

    def fetch(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """yfinance.download で取得し、標準列名に変換して返す。

        Args:
            ticker: ティッカーシンボル
            start:  取得開始日 (inclusive)
            end:    取得終了日 (inclusive, yfinance には +1 日して渡す)

        Returns:
            標準化済み DataFrame。データなしは空 DataFrame。

        Raises:
            Exception: ネットワークエラー、yfinance 内部エラー等。
        """
        import yfinance as yf  # lazy import

        # yfinance の end は exclusive なので +1 日
        yf_end = end + timedelta(days=1)

        logger.debug("yfinance fetch: ticker=%s start=%s end=%s", ticker, start, end)

        raw: pd.DataFrame = yf.download(
            tickers=ticker,
            start=start.isoformat(),
            end=yf_end.isoformat(),
            auto_adjust=False,
            progress=False,
        )

        if raw.empty:
            logger.info("yfinance から空データ: ticker=%s", ticker)
            return pd.DataFrame()

        # yfinance バージョン差異で MultiIndex になる場合への対応
        if isinstance(raw.columns, pd.MultiIndex):
            # 単一 ticker の場合: level=1 がティッカー名, level=0 が価格種別
            raw = raw.droplevel(level=1, axis=1)

        # 標準列名に変換 (対応する列のみ抽出)
        available = {k: v for k, v in _YFINANCE_COL_MAP.items() if k in raw.columns}
        df = raw[list(available.keys())].rename(columns=available)

        logger.debug("yfinance fetch 完了: ticker=%s rows=%d", ticker, len(df))
        return df
