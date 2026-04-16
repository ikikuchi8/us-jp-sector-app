"""
PriceService のユニットテスト。

# テスト方針
  - MockFetcher でネットワーク呼び出しを完全に排除する
  - mock_repo (MagicMock) で DB 操作を排除し、upsert_many への引数を検証する
  - mock_session (MagicMock) で commit / rollback を検証する
  - ライブ API に依存するテストはこのファイルには含めない

# MockFetcher の動作
  - data: dict[str, pd.DataFrame | Exception] を受け取る
  - fetch() 呼び出し時: Exception なら raise、DataFrame なら返す
  - ticker が data に存在しない場合: 空 DataFrame を返す

# upsert_many への引数取得パターン
  args, _ = mock_repo.upsert_many.call_args
  saved_rows: list[PriceDaily] = args[0]
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, call

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from app.repositories.price_repository import PriceRepository
from app.seed_data.instruments import JP_INSTRUMENTS, US_INSTRUMENTS
from app.services.price_service import FetchResult, PriceService

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_START = date(2025, 1, 6)   # 月
_END   = date(2025, 1, 8)   # 水
_DATES = [date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8)]

_TICKER_US = "XLB"   # seed_data に存在する US ticker
_TICKER_JP = "1617.T"  # seed_data に存在する JP ticker


# ---------------------------------------------------------------------------
# MockFetcher
# ---------------------------------------------------------------------------

class MockFetcher:
    """ネットワーク非依存の PriceFetcher 実装。

    data に Exception を渡すと fetch() 時に raise する。
    data に DataFrame を渡すとそのまま返す。
    data にキーが存在しない場合は空 DataFrame を返す。
    """

    def __init__(self, data: dict[str, pd.DataFrame | Exception]) -> None:
        self._data = data

    def fetch(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        value = self._data.get(ticker, pd.DataFrame())
        if isinstance(value, Exception):
            raise value
        return value


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def _make_valid_df(
    close_prices: list[float] | None = None,
    dates: list[date] | None = None,
) -> pd.DataFrame:
    """標準列を持つ有効な DataFrame を生成する。"""
    dates = dates or _DATES
    close_prices = close_prices or [100.0, 101.0, 102.0]
    return pd.DataFrame(
        {
            "open":      [p * 0.99 for p in close_prices],
            "high":      [p * 1.01 for p in close_prices],
            "low":       [p * 0.98 for p in close_prices],
            "close":     close_prices,
            "adj_close": [p * 0.95 for p in close_prices],
            "volume":    [1_000_000] * len(close_prices),
        },
        index=pd.DatetimeIndex(dates),
    )


def _make_service(
    mock_session: MagicMock,
    mock_repo: MagicMock,
    data: dict[str, pd.DataFrame | Exception],
) -> PriceService:
    """MockFetcher + mock_repo を注入した PriceService を生成する。"""
    return PriceService(
        session=mock_session,
        fetcher=MockFetcher(data),
        repository=mock_repo,
    )


def _get_saved_rows(mock_repo: MagicMock, call_index: int = 0):
    """mock_repo.upsert_many の呼び出し引数から保存 rows を取得する。"""
    return mock_repo.upsert_many.call_args_list[call_index][0][0]


# ---------------------------------------------------------------------------
# フィクスチャ
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session() -> MagicMock:
    return MagicMock(spec=Session)


@pytest.fixture
def mock_repo() -> MagicMock:
    return MagicMock(spec=PriceRepository)


# ---------------------------------------------------------------------------
# 1. 正常保存
# ---------------------------------------------------------------------------

class TestNormalSave:
    def test_result_succeeded_contains_ticker(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """正常取得 → result.succeeded に ticker が記録される。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: _make_valid_df()})
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert _TICKER_US in result.succeeded

    def test_saved_rows_count(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """3 行の DataFrame → saved_rows = 3。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: _make_valid_df()})
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert result.saved_rows == 3

    def test_upsert_many_called_with_price_daily_objects(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """upsert_many が PriceDaily オブジェクトのリストで呼ばれる。"""
        from app.models.price import PriceDaily

        svc = _make_service(mock_session, mock_repo, {_TICKER_US: _make_valid_df()})
        svc.update_prices([_TICKER_US], _START, _END)

        mock_repo.upsert_many.assert_called_once()
        rows = _get_saved_rows(mock_repo)
        assert all(isinstance(r, PriceDaily) for r in rows)

    def test_commit_called_per_ticker(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """ticker ごとに commit が呼ばれる。"""
        df = _make_valid_df()
        svc = _make_service(mock_session, mock_repo, {
            _TICKER_US: df,
            "XLK": _make_valid_df(close_prices=[200.0, 201.0, 202.0]),
        })
        svc.update_prices([_TICKER_US, "XLK"], _START, _END)
        assert mock_session.commit.call_count == 2

    def test_requested_count(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """result.requested は入力 ticker 数を返す。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: _make_valid_df()})
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert result.requested == 1


# ---------------------------------------------------------------------------
# 2. 価格フィールドの保存確認
# ---------------------------------------------------------------------------

class TestPriceFieldsSaved:
    def test_adjusted_close_price_is_saved(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """adjusted_close_price が正しく保存される。

        adj_close = close * 0.95 として生成した DataFrame を使用する。
        """
        df = _make_valid_df(close_prices=[100.0])
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df})
        svc.update_prices([_TICKER_US], _START, _START)

        rows = _get_saved_rows(mock_repo)
        assert rows[0].adjusted_close_price == Decimal("95.0")

    def test_open_price_is_saved(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """open_price が正しく保存される。

        open = close * 0.99 として生成した DataFrame を使用する。
        """
        df = _make_valid_df(close_prices=[100.0])
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df})
        svc.update_prices([_TICKER_US], _START, _START)

        rows = _get_saved_rows(mock_repo)
        assert rows[0].open_price == Decimal("99.0")

    def test_close_price_is_saved(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """close_price が正しく保存される。"""
        df = _make_valid_df(close_prices=[123.45])
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df})
        svc.update_prices([_TICKER_US], _START, _START)

        rows = _get_saved_rows(mock_repo)
        assert rows[0].close_price == Decimal("123.45")

    def test_nan_value_is_saved_as_none(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """NaN 値は None として保存される (nullable カラム)。"""
        import math
        df = _make_valid_df(close_prices=[100.0])
        df.loc[df.index[0], "open"] = float("nan")

        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df})
        svc.update_prices([_TICKER_US], _START, _START)

        rows = _get_saved_rows(mock_repo)
        assert rows[0].open_price is None

    def test_market_derived_from_seed_data(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """market は seed_data から自動決定される (US ticker → "US")。"""
        df = _make_valid_df()
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df})
        svc.update_prices([_TICKER_US], _START, _END)

        rows = _get_saved_rows(mock_repo)
        assert all(r.market == "US" for r in rows)

    def test_jp_market_derived_from_seed_data(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """JP ticker の market は seed_data から "JP" と決定される。"""
        df = _make_valid_df()
        svc = _make_service(mock_session, mock_repo, {_TICKER_JP: df})
        svc.update_prices([_TICKER_JP], _START, _END)

        rows = _get_saved_rows(mock_repo)
        assert all(r.market == "JP" for r in rows)


# ---------------------------------------------------------------------------
# 3. 空 DataFrame → empty 扱い
# ---------------------------------------------------------------------------

class TestEmptyDataFrame:
    def test_empty_df_recorded_as_empty(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """空 DataFrame が返った ticker は result.empty に記録される。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: pd.DataFrame()})
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert _TICKER_US in result.empty

    def test_empty_df_does_not_call_upsert(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """空 DataFrame の場合 upsert_many は呼ばれない。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: pd.DataFrame()})
        svc.update_prices([_TICKER_US], _START, _END)
        mock_repo.upsert_many.assert_not_called()

    def test_empty_df_does_not_commit(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """空 DataFrame の場合 commit は呼ばれない。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: pd.DataFrame()})
        svc.update_prices([_TICKER_US], _START, _END)
        mock_session.commit.assert_not_called()

    def test_empty_is_not_in_failed(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """空 DataFrame はエラーではないため result.failed には記録されない。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: pd.DataFrame()})
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert _TICKER_US not in result.failed


# ---------------------------------------------------------------------------
# 4. 必須列不足 → failed 扱い
# ---------------------------------------------------------------------------

class TestInvalidDataFrame:
    def test_missing_required_cols_recorded_as_failed(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """必須列が不足する DataFrame は result.failed に記録される。"""
        df_missing_adj_close = pd.DataFrame(
            {
                "open": [100.0],
                "high": [101.0],
                "low":  [99.0],
                "close": [100.0],
                "volume": [1_000_000],
                # adj_close を意図的に欠落
            },
            index=pd.DatetimeIndex([_START]),
        )
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df_missing_adj_close})
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert _TICKER_US in result.failed

    def test_failed_message_mentions_missing_column(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """エラーメッセージに不足列名が含まれる。"""
        df = pd.DataFrame(
            {"open": [100.0], "close": [100.0]},  # high / low / adj_close / volume が欠落
            index=pd.DatetimeIndex([_START]),
        )
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df})
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert "adj_close" in result.failed[_TICKER_US]

    def test_invalid_df_does_not_call_upsert(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """不正 DataFrame の場合 upsert_many は呼ばれない。"""
        df = pd.DataFrame({"close": [100.0]}, index=pd.DatetimeIndex([_START]))
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: df})
        svc.update_prices([_TICKER_US], _START, _END)
        mock_repo.upsert_many.assert_not_called()


# ---------------------------------------------------------------------------
# 5. 部分失敗 (fetch エラー)
# ---------------------------------------------------------------------------

class TestPartialFailure:
    def test_fetch_exception_recorded_as_failed(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """fetcher が例外を送出した ticker は result.failed に記録される。"""
        svc = _make_service(mock_session, mock_repo, {
            _TICKER_US: RuntimeError("network timeout"),
        })
        result = svc.update_prices([_TICKER_US], _START, _END)
        assert _TICKER_US in result.failed
        assert "network timeout" in result.failed[_TICKER_US]

    def test_partial_failure_other_ticker_is_saved(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """1 ticker が失敗しても他 ticker は正常に保存される (fail-soft)。"""
        svc = _make_service(mock_session, mock_repo, {
            _TICKER_US: RuntimeError("fetch error"),
            "XLK": _make_valid_df(close_prices=[200.0, 201.0, 202.0]),
        })
        result = svc.update_prices([_TICKER_US, "XLK"], _START, _END)

        assert _TICKER_US in result.failed
        assert "XLK" in result.succeeded
        mock_repo.upsert_many.assert_called_once()

    def test_unknown_ticker_recorded_as_failed(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """seed_data に存在しない ticker は result.failed に記録される。"""
        svc = _make_service(mock_session, mock_repo, {"UNKNOWN": _make_valid_df()})
        result = svc.update_prices(["UNKNOWN"], _START, _END)
        assert "UNKNOWN" in result.failed
        assert "seed_data" in result.failed["UNKNOWN"]

    def test_failed_ticker_does_not_commit(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """失敗 ticker では commit が呼ばれない。"""
        svc = _make_service(mock_session, mock_repo, {
            _TICKER_US: RuntimeError("error"),
        })
        svc.update_prices([_TICKER_US], _START, _END)
        mock_session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# 6. 再取得で upsert が更新される
# ---------------------------------------------------------------------------

class TestUpsertOnSecondFetch:
    def test_upsert_many_called_twice_on_second_fetch(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """同一期間を 2 回取得すると upsert_many が 2 回呼ばれる。"""
        data = {_TICKER_US: _make_valid_df()}
        svc = _make_service(mock_session, mock_repo, data)

        svc.update_prices([_TICKER_US], _START, _END)
        svc.update_prices([_TICKER_US], _START, _END)

        assert mock_repo.upsert_many.call_count == 2

    def test_second_fetch_passes_updated_close_price(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """2 回目の取得で close_price が新しい値で upsert_many に渡される。"""
        fetcher_data: dict = {_TICKER_US: _make_valid_df(close_prices=[100.0, 101.0, 102.0])}
        svc = PriceService(
            session=mock_session,
            fetcher=MockFetcher(fetcher_data),
            repository=mock_repo,
        )

        svc.update_prices([_TICKER_US], _START, _END)

        # 2 回目: close_price を変更
        fetcher_data[_TICKER_US] = _make_valid_df(close_prices=[110.0, 111.0, 112.0])
        svc.update_prices([_TICKER_US], _START, _END)

        second_rows = _get_saved_rows(mock_repo, call_index=1)
        assert second_rows[0].close_price == Decimal("110.0")


# ---------------------------------------------------------------------------
# 7. start > end の扱い
# ---------------------------------------------------------------------------

class TestStartAfterEnd:
    def test_returns_empty_result(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """start > end の場合は即座に空 FetchResult を返す。"""
        svc = _make_service(mock_session, mock_repo, {_TICKER_US: _make_valid_df()})
        result = svc.update_prices([_TICKER_US], date(2025, 1, 8), date(2025, 1, 6))

        assert result.requested == 1
        assert result.succeeded == []
        assert result.failed == {}
        assert result.empty == []
        assert result.saved_rows == 0

    def test_no_fetch_called_when_start_after_end(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """start > end の場合 fetcher が呼ばれない。"""
        called = []

        class TrackingFetcher:
            def fetch(self, ticker, start, end):
                called.append(ticker)
                return pd.DataFrame()

        svc = PriceService(session=mock_session, fetcher=TrackingFetcher(), repository=mock_repo)
        svc.update_prices([_TICKER_US], date(2025, 1, 8), date(2025, 1, 6))
        assert called == []


# ---------------------------------------------------------------------------
# 8. update_us_prices / update_jp_prices / update_all_prices
# ---------------------------------------------------------------------------

class TestShorthandMethods:
    def test_update_us_prices_fetches_11_tickers(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """update_us_prices は US_INSTRUMENTS の 11 ticker を取得対象にする。"""
        fetched: list[str] = []

        class TrackingFetcher:
            def fetch(self, ticker, start, end):
                fetched.append(ticker)
                return pd.DataFrame()  # empty → no save

        svc = PriceService(session=mock_session, fetcher=TrackingFetcher(), repository=mock_repo)
        result = svc.update_us_prices(_START, _END)

        expected = [row["ticker"] for row in US_INSTRUMENTS]
        assert fetched == expected
        assert result.requested == 11

    def test_update_jp_prices_fetches_17_tickers(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """update_jp_prices は JP_INSTRUMENTS の 17 ticker を取得対象にする。"""
        fetched: list[str] = []

        class TrackingFetcher:
            def fetch(self, ticker, start, end):
                fetched.append(ticker)
                return pd.DataFrame()

        svc = PriceService(session=mock_session, fetcher=TrackingFetcher(), repository=mock_repo)
        result = svc.update_jp_prices(_START, _END)

        expected = [row["ticker"] for row in JP_INSTRUMENTS]
        assert fetched == expected
        assert result.requested == 17

    def test_update_all_prices_requested_is_28(
        self, mock_session: MagicMock, mock_repo: MagicMock
    ) -> None:
        """update_all_prices の requested = US 11 + JP 17 = 28。"""

        class EmptyFetcher:
            def fetch(self, ticker, start, end):
                return pd.DataFrame()

        svc = PriceService(session=mock_session, fetcher=EmptyFetcher(), repository=mock_repo)
        result = svc.update_all_prices(_START, _END)
        assert result.requested == 28


# ---------------------------------------------------------------------------
# 9. FetchResult の合算
# ---------------------------------------------------------------------------

class TestFetchResultAddition:
    def test_add_combines_fields(self) -> None:
        """FetchResult.__add__ で requested / saved_rows / succeeded / failed / empty が合算される。"""
        us = FetchResult(
            requested=11,
            saved_rows=33,
            succeeded=["XLB", "XLK"],
            failed={"XLE": "error"},
            empty=["XLF"],
        )
        jp = FetchResult(
            requested=17,
            saved_rows=51,
            succeeded=["1617.T"],
            failed={},
            empty=["1618.T"],
        )
        combined = us + jp

        assert combined.requested == 28
        assert combined.saved_rows == 84
        assert "XLB" in combined.succeeded and "1617.T" in combined.succeeded
        assert "XLE" in combined.failed
        assert "XLF" in combined.empty and "1618.T" in combined.empty

    def test_has_failure_true_when_failed_exists(self) -> None:
        result = FetchResult(failed={"XLB": "err"})
        assert result.has_failure is True

    def test_has_failure_false_when_no_failure(self) -> None:
        result = FetchResult(succeeded=["XLB"])
        assert result.has_failure is False
