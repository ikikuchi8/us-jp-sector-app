"""
tests/test_signal_service.py — SignalService の単体テスト。

テスト戦略:
  - DB / ネットワーク接続なし: PriceRepository / SignalRepository / Session をモック注入
  - CalendarService もモックし build_date_alignment() の戻り値を制御する
  - _score_jp_sectors / _rank_and_side はモジュールレベル関数として直接テストする
  - _compute_us_returns は price_repo の get_prices_up_to をモックして検証する

テスト範囲:
  [A] _compute_us_returns
      - 2 行から正しいリターンを計算すること
      - 1 行のみの場合は None
      - 全行欠損の場合は全 None
      - as_of_date = us_signal_date で呼ばれること
      - jp_execution_date は as_of_date として使われないこと

  [B] _score_jp_sectors (純粋関数)
      - 1 US ティッカー対応の JP 業種: そのリターンがスコアになること
      - 2 US ティッカー対応の JP 業種: 等ウェイト平均になること
      - 全対応 US が None → スコア None
      - 一部 US が None → 残りで平均

  [C] _rank_and_side (純粋関数)
      - rank 1 = 最高スコア、rank 17 = 最低スコア
      - rank 1〜3 → long、下位 3 → short、それ以外 → neutral
      - None スコア → rank=None, side="neutral"
      - タイブレーク: 同スコア時は ticker 昇順

  [D] _generate_for_date / generate_signals_for_range
      - 全 US 欠損 → 空リスト返却 (skipped に記録)
      - upsert_many が呼ばれること
      - 同日に 2 回呼ぶと upsert_many が 2 回呼ばれること (upsert 冪等性)
      - jp_execution_date ペア数分の succeeded が返ること
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from app.models.price import PriceDaily
from app.models.signal import SignalDaily, SuggestedSide
from app.repositories.price_repository import PriceRepository
from app.repositories.signal_repository import SignalRepository
from app.seed_data.sector_mapping import JP_SECTOR_MAPPINGS, JP_TICKER_TO_US_TICKERS
from app.services.calendar_service import (
    COL_JP_EXECUTION_DATE,
    COL_US_SIGNAL_DATE,
    CalendarService,
)
from app.services.signal_service import (
    SignalGenerationResult,
    SignalService,
    _rank_and_side,
    _score_jp_sectors,
)
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_US_DATE = date(2025, 1, 14)
_JP_DATE = date(2025, 1, 15)

# JP 17業種の全 ticker (順序は sector_mapping に従う)
_ALL_JP_TICKERS = [m["jp_ticker"] for m in JP_SECTOR_MAPPINGS]

# 全ユニーク US ティッカー
_ALL_US_TICKERS = sorted(
    {t for us_list in JP_TICKER_TO_US_TICKERS.values() for t in us_list}
)


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def _make_price(adj_close: float, business_date: date | None = None) -> MagicMock:
    """PriceDaily のモック (adjusted_close_price のみ設定)。"""
    p = MagicMock(spec=PriceDaily)
    p.adjusted_close_price = Decimal(str(adj_close))
    p.business_date = business_date or _US_DATE
    return p


def _make_all_us_returns(default: float = 0.01, **overrides: float | None) -> dict[str, float | None]:
    """全 US ティッカーに default 値を設定し、overrides で上書きした dict を返す。"""
    result: dict[str, float | None] = {t: default for t in _ALL_US_TICKERS}
    result.update(overrides)
    return result


def _make_scores_17(base: float = 0.0) -> dict[str, float | None]:
    """JP 17業種に順番に異なるスコア (base + index * 0.01) を与える dict を返す。"""
    return {t: base + i * 0.01 for i, t in enumerate(_ALL_JP_TICKERS)}


def _make_alignment(pairs: list[tuple[date, date]]) -> pd.DataFrame:
    """(us_date, jp_date) タプルのリストから build_date_alignment() 相当の DataFrame を返す。"""
    return pd.DataFrame(
        [
            {COL_US_SIGNAL_DATE: us, COL_JP_EXECUTION_DATE: jp}
            for us, jp in pairs
        ]
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_session() -> MagicMock:
    return MagicMock(spec=Session)


@pytest.fixture
def mock_price_repo() -> MagicMock:
    return MagicMock(spec=PriceRepository)


@pytest.fixture
def mock_signal_repo() -> MagicMock:
    return MagicMock(spec=SignalRepository)


@pytest.fixture
def mock_calendar() -> MagicMock:
    return MagicMock(spec=CalendarService)


@pytest.fixture
def service(
    mock_session: MagicMock,
    mock_price_repo: MagicMock,
    mock_signal_repo: MagicMock,
    mock_calendar: MagicMock,
) -> SignalService:
    return SignalService(
        session=mock_session,
        calendar_service=mock_calendar,
        price_repository=mock_price_repo,
        signal_repository=mock_signal_repo,
    )


# ---------------------------------------------------------------------------
# [A] _compute_us_returns
# ---------------------------------------------------------------------------


class TestComputeUsReturns:
    """_compute_us_returns のテスト群。"""

    def test_return_calculation_from_two_prices(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """2 行の価格から (curr - prev) / prev が計算されること。"""
        def side_effect(ticker, as_of_date, limit):
            if ticker == "XLB":
                return [_make_price(100.0), _make_price(102.0)]
            return []

        mock_price_repo.get_prices_up_to.side_effect = side_effect

        result = service._compute_us_returns(_US_DATE)

        assert result["XLB"] == pytest.approx(0.02)  # (102 - 100) / 100

    def test_negative_return(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """価格下落時に負のリターンが返ること。"""
        def side_effect(ticker, as_of_date, limit):
            if ticker == "XLE":
                return [_make_price(200.0), _make_price(190.0)]
            return []

        mock_price_repo.get_prices_up_to.side_effect = side_effect
        result = service._compute_us_returns(_US_DATE)

        assert result["XLE"] == pytest.approx(-0.05)

    def test_single_price_row_returns_none(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """1 行のみの場合はリターン計算不能 → None。"""
        def side_effect(ticker, as_of_date, limit):
            if ticker == "XLB":
                return [_make_price(100.0)]
            return []

        mock_price_repo.get_prices_up_to.side_effect = side_effect
        result = service._compute_us_returns(_US_DATE)

        assert result["XLB"] is None

    def test_zero_price_rows_returns_none(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """全ティッカーで 0 行の場合、全値が None になること。"""
        mock_price_repo.get_prices_up_to.return_value = []

        result = service._compute_us_returns(_US_DATE)

        assert all(v is None for v in result.values())
        assert len(result) == len(_ALL_US_TICKERS)

    def test_as_of_date_is_us_signal_date(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """get_prices_up_to の as_of_date が us_signal_date であること。"""
        mock_price_repo.get_prices_up_to.return_value = []

        service._compute_us_returns(_US_DATE)

        for c in mock_price_repo.get_prices_up_to.call_args_list:
            assert c.kwargs["as_of_date"] == _US_DATE

    def test_jp_execution_date_never_used_as_as_of_date(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """jp_execution_date (_JP_DATE) は as_of_date として一切渡されないこと。"""
        mock_price_repo.get_prices_up_to.return_value = []

        # us_signal_date だけを渡して実行
        service._compute_us_returns(_US_DATE)

        for c in mock_price_repo.get_prices_up_to.call_args_list:
            assert c.kwargs.get("as_of_date") != _JP_DATE

    def test_limit_is_always_2(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """limit=2 で get_prices_up_to が呼ばれること。"""
        mock_price_repo.get_prices_up_to.return_value = []

        service._compute_us_returns(_US_DATE)

        for c in mock_price_repo.get_prices_up_to.call_args_list:
            assert c.kwargs["limit"] == 2


# ---------------------------------------------------------------------------
# [B] _score_jp_sectors
# ---------------------------------------------------------------------------


class TestScoreJpSectors:
    """_score_jp_sectors のテスト群 (純粋関数)。"""

    def test_single_us_ticker_mapping(self) -> None:
        """XLP のみに対応する 1617.T (食品) のスコア = XLP のリターン。"""
        us_returns = _make_all_us_returns(XLP=0.05)
        scores = _score_jp_sectors(us_returns)
        assert scores["1617.T"] == pytest.approx(0.05)

    def test_two_us_tickers_equal_weight_average(self) -> None:
        """XLB と XLI に対応する 1619.T (建設・資材) のスコア = 等ウェイト平均。"""
        us_returns = _make_all_us_returns(XLB=0.02, XLI=0.04)
        scores = _score_jp_sectors(us_returns)
        assert scores["1619.T"] == pytest.approx(0.03)  # (0.02 + 0.04) / 2

    def test_all_mapped_tickers_none_gives_none_score(self) -> None:
        """全対応 US ティッカーが None の JP 業種は score = None になること。"""
        us_returns: dict[str, float | None] = {t: None for t in _ALL_US_TICKERS}
        scores = _score_jp_sectors(us_returns)
        assert all(s is None for s in scores.values())

    def test_partial_none_uses_available_tickers(self) -> None:
        """1619.T は XLB, XLI 対応。XLI が None でも XLB だけで平均を計算すること。"""
        us_returns: dict[str, float | None] = {t: None for t in _ALL_US_TICKERS}
        us_returns["XLB"] = 0.04
        scores = _score_jp_sectors(us_returns)
        assert scores["1619.T"] == pytest.approx(0.04)

    def test_all_17_jp_tickers_have_scores(self) -> None:
        """戻り値に JP 17 業種全ての ticker が含まれること。"""
        us_returns = _make_all_us_returns()
        scores = _score_jp_sectors(us_returns)
        assert set(scores.keys()) == set(_ALL_JP_TICKERS)


# ---------------------------------------------------------------------------
# [C] _rank_and_side
# ---------------------------------------------------------------------------


class TestRankAndSide:
    """_rank_and_side のテスト群 (純粋関数)。"""

    def test_highest_score_is_rank_1(self) -> None:
        """スコアが最大の業種が rank=1 になること。"""
        # 各 ticker にインデックス順のスコアを付与: 末尾 ticker が最高スコア
        scores = _make_scores_17()
        result = _rank_and_side(scores)
        max_ticker = max(scores, key=lambda t: scores[t])
        assert result[max_ticker][0] == 1

    def test_lowest_score_is_rank_17(self) -> None:
        """スコアが最小の業種が rank=17 になること。"""
        scores = _make_scores_17()
        result = _rank_and_side(scores)
        min_ticker = min(scores, key=lambda t: scores[t])
        assert result[min_ticker][0] == 17

    def test_rank_1_to_3_are_long(self) -> None:
        """rank 1〜3 の業種が suggested_side="long" になること。"""
        scores = _make_scores_17()
        result = _rank_and_side(scores)
        long_ranks = [r for _, (r, s) in result.items() if s == SuggestedSide.LONG.value]
        assert sorted(long_ranks) == [1, 2, 3]

    def test_bottom_3_are_short(self) -> None:
        """スコア下位 3 業種が suggested_side="short" になること。"""
        scores = _make_scores_17()
        result = _rank_and_side(scores)
        short_ranks = [r for _, (r, s) in result.items() if s == SuggestedSide.SHORT.value]
        assert sorted(short_ranks) == [15, 16, 17]

    def test_rank_4_to_14_are_neutral(self) -> None:
        """rank 4〜14 の業種が suggested_side="neutral" になること。"""
        scores = _make_scores_17()
        result = _rank_and_side(scores)
        neutral_ranks = sorted(
            r for _, (r, s) in result.items()
            if s == SuggestedSide.NEUTRAL.value and r is not None
        )
        assert neutral_ranks == list(range(4, 15))

    def test_none_score_gives_none_rank_and_neutral(self) -> None:
        """スコアが None の業種は rank=None, side="neutral" になること。"""
        scores = _make_scores_17()
        # 1617.T のスコアを None に
        scores["1617.T"] = None
        result = _rank_and_side(scores)
        assert result["1617.T"] == (None, SuggestedSide.NEUTRAL.value)

    def test_tiebreak_by_ticker_ascending(self) -> None:
        """同スコア時は ticker 昇順でランクが決まること。"""
        # 全業種を同スコアにする
        scores: dict[str, float | None] = {t: 0.01 for t in _ALL_JP_TICKERS}
        result = _rank_and_side(scores)
        # ticker 昇順にソートしたとき rank が連番になること
        ranked = sorted(_ALL_JP_TICKERS, key=lambda t: result[t][0])
        assert ranked == sorted(_ALL_JP_TICKERS)

    def test_all_17_tickers_in_result(self) -> None:
        """戻り値に JP 17 業種全ての ticker が含まれること。"""
        scores = _make_scores_17()
        result = _rank_and_side(scores)
        assert set(result.keys()) == set(_ALL_JP_TICKERS)


# ---------------------------------------------------------------------------
# [D] _generate_for_date / generate_signals_for_range
# ---------------------------------------------------------------------------


class TestGenerateForDate:
    """_generate_for_date のテスト群。"""

    def _setup_all_prices(self, mock_price_repo: MagicMock) -> None:
        """全 US ティッカーに 2 行の価格データを返す side_effect を設定する。"""
        def side_effect(ticker, as_of_date, limit):
            return [_make_price(100.0), _make_price(101.0)]

        mock_price_repo.get_prices_up_to.side_effect = side_effect

    def test_generates_17_rows(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """有効な価格データがあるとき JP 17 業種分の SignalDaily が生成されること。"""
        self._setup_all_prices(mock_price_repo)
        rows = service._generate_for_date(_JP_DATE, _US_DATE)
        assert len(rows) == 17

    def test_all_us_missing_returns_empty_list(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """全 US ティッカーが欠損の場合は空リストが返ること。"""
        mock_price_repo.get_prices_up_to.return_value = []
        rows = service._generate_for_date(_JP_DATE, _US_DATE)
        assert rows == []

    def test_row_fields_are_set(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """生成された SignalDaily に必須フィールドが設定されていること。"""
        self._setup_all_prices(mock_price_repo)
        rows = service._generate_for_date(_JP_DATE, _US_DATE)

        for row in rows:
            assert row.signal_type == "simple_v1"
            assert row.jp_execution_date == _JP_DATE
            assert row.us_signal_date == _US_DATE
            assert row.suggested_side in ("long", "short", "neutral")
            assert row.input_metadata_json is not None

    def test_none_score_rows_have_neutral_side(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """スコアが None の業種は suggested_side="neutral" になること。"""
        # 全 US を欠損にするが、XLP だけ有効にする (1617.T のみスコアあり)
        def side_effect(ticker, as_of_date, limit):
            if ticker == "XLP":
                return [_make_price(100.0), _make_price(101.0)]
            return []

        mock_price_repo.get_prices_up_to.side_effect = side_effect
        rows = service._generate_for_date(_JP_DATE, _US_DATE)

        none_score_rows = [r for r in rows if r.signal_score is None]
        for row in none_score_rows:
            assert row.signal_rank is None
            assert row.suggested_side == "neutral"

    def test_metadata_json_contains_us_returns(
        self, service: SignalService, mock_price_repo: MagicMock
    ) -> None:
        """input_metadata_json に us_returns が含まれること。"""
        self._setup_all_prices(mock_price_repo)
        rows = service._generate_for_date(_JP_DATE, _US_DATE)

        for row in rows:
            assert "us_returns" in row.input_metadata_json


class TestGenerateSignalsForRange:
    """generate_signals_for_range のテスト群。"""

    def _setup(
        self,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        pairs: list[tuple[date, date]],
    ) -> None:
        mock_calendar.build_date_alignment.return_value = _make_alignment(pairs)
        # 全 US に有効な 2 行の価格を返す
        mock_price_repo.get_prices_up_to.return_value = [
            _make_price(100.0), _make_price(101.0)
        ]

    def test_requested_count_matches_alignment_rows(
        self,
        service: SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """result.requested が alignment の行数と一致すること。"""
        self._setup(mock_calendar, mock_price_repo, [(_US_DATE, _JP_DATE)])
        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)
        assert result.requested == 1

    def test_succeeded_contains_jp_execution_date(
        self,
        service: SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """正常完了時に result.succeeded に jp_execution_date が含まれること。"""
        self._setup(mock_calendar, mock_price_repo, [(_US_DATE, _JP_DATE)])
        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)
        assert _JP_DATE in result.succeeded

    def test_upsert_many_called_per_date(
        self,
        service: SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """2 つの jp_execution_date を処理したとき upsert_many が 2 回呼ばれること。"""
        date2_us = date(2025, 1, 15)
        date2_jp = date(2025, 1, 16)
        self._setup(
            mock_calendar,
            mock_price_repo,
            [(_US_DATE, _JP_DATE), (date2_us, date2_jp)],
        )
        service.generate_signals_for_range(_JP_DATE, date2_jp)
        assert mock_signal_repo.upsert_many.call_count == 2

    def test_second_call_also_invokes_upsert(
        self,
        service: SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """同一 jp_execution_date で 2 回呼んでも upsert_many が呼ばれること (冪等性)。"""
        self._setup(mock_calendar, mock_price_repo, [(_US_DATE, _JP_DATE)])
        service.generate_signals_for_range(_JP_DATE, _JP_DATE)
        service.generate_signals_for_range(_JP_DATE, _JP_DATE)
        assert mock_signal_repo.upsert_many.call_count == 2

    def test_all_us_missing_records_skipped(
        self,
        service: SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """全 US 価格欠損の日は skipped に記録され upsert_many は呼ばれないこと。"""
        mock_calendar.build_date_alignment.return_value = _make_alignment(
            [(_US_DATE, _JP_DATE)]
        )
        mock_price_repo.get_prices_up_to.return_value = []  # 全欠損

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.skipped
        assert result.succeeded == []
        mock_signal_repo.upsert_many.assert_not_called()

    def test_empty_alignment_returns_zero_requested(
        self,
        service: SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """alignment が空のとき requested=0 で即返ること。"""
        mock_calendar.build_date_alignment.return_value = _make_alignment([])
        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)
        assert result.requested == 0
        mock_signal_repo.upsert_many.assert_not_called()

    def test_commit_called_per_succeeded_date(
        self,
        service: SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """正常完了した jp_date 数だけ session.commit が呼ばれること。"""
        self._setup(mock_calendar, mock_price_repo, [(_US_DATE, _JP_DATE)])
        service.generate_signals_for_range(_JP_DATE, _JP_DATE)
        assert mock_session.commit.call_count == 1
