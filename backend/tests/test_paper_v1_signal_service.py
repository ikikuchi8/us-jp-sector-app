"""
tests/test_paper_v1_signal_service.py — PaperV1SignalService の単体テスト。

テスト戦略:
  - DB / ネットワーク接続なし: PriceRepository / SignalRepository / Session をモック注入
  - CalendarService もモックして build_date_alignment() 戻り値を制御する
  - 純粋関数 (_standardize, _select_n_components, _fit_model, _compute_scores,
    _rank_and_side_paper_v1) はモック不要で直接テストする

テスト範囲:
  [A] _standardize
      - 基本的な標準化 (mean≈0, std≈1)
      - 既存 mu/sigma を指定した標準化 (テスト点の変換)
      - 定数列 (sigma≈0) でも _EPSILON で除算してゼロ除算しないこと

  [B] _select_n_components
      - 累積寄与率が threshold を超える最小 K を選ぶこと
      - max_k で上限クリップされること
      - 1 成分で十分な場合

  [C] _fit_model / _compute_scores
      - 返り値の shape が正しいこと
      - 学習 → 予測が完結すること (shape 確認)
      - スコアが訓練データの統計量を含む PaperV1Model を持つこと

  [D] _rank_and_side_paper_v1
      - 上位 N_LONG = 5 業種が "long" になること
      - 下位 N_SHORT = 5 業種が "short" になること
      - 中間 7 業種が "neutral" になること
      - rank 1 = 最高スコア
      - タイブレーク: 同スコア時は ticker 昇順

  [E] PaperV1SignalService.generate_signals_for_range
      - 訓練データ不足 → スキップ (skipped に記録、upsert_many は呼ばれない)
      - 現在 US リターン不足 → スキップ
      - 正常系: 17 業種分の SignalDaily が生成されること
      - 正常系: upsert_many → commit が呼ばれること
      - 先読み防止: get_prices_up_to の as_of_date = us_signal_date であること
      - 先読み防止: jp_execution_date が as_of_date として使われないこと
      - alignment 空 → requested=0, succeeded=[]
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import numpy as np
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
from app.services.paper_v1_signal_service import (
    N_LONG,
    N_SHORT,
    SIGNAL_TYPE_PAPER_V1,
    WINDOW_SIZE,
    PaperV1SignalService,
    _ALL_JP_TICKERS,
    _ALL_US_TICKERS,
    _EPSILON,
    _compute_scores,
    _fit_model,
    _rank_and_side_paper_v1,
    _select_n_components,
    _standardize,
)
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_US_DATE = date(2025, 1, 14)
_JP_DATE = date(2025, 1, 15)

_N_US = len(_ALL_US_TICKERS)   # 11 US ETF
_N_JP = len(_ALL_JP_TICKERS)   # 17 JP 業種


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------


def _make_alignment(pairs: list[tuple[date, date]]) -> pd.DataFrame:
    """(us_date, jp_date) タプルのリストから build_date_alignment() 相当の DataFrame を返す。"""
    return pd.DataFrame(
        [{COL_US_SIGNAL_DATE: us, COL_JP_EXECUTION_DATE: jp} for us, jp in pairs]
    )


def _make_adj_close_price(adj_close: float, business_date: date | None = None) -> MagicMock:
    """adjusted_close_price を持つ PriceDaily モックを返す (CC リターン用)。"""
    p = MagicMock(spec=PriceDaily)
    p.adjusted_close_price = Decimal(str(adj_close))
    p.business_date = business_date or _US_DATE
    return p


def _make_oc_price(
    open_p: float,
    close_p: float,
    business_date: date | None = None,
) -> MagicMock:
    """open_price / close_price を持つ PriceDaily モックを返す (JP OC リターン用)。"""
    p = MagicMock(spec=PriceDaily)
    p.open_price = Decimal(str(open_p))
    p.close_price = Decimal(str(close_p))
    p.business_date = business_date or _JP_DATE
    return p


def _synthetic_X(n: int = 80, p: int | None = None) -> np.ndarray:
    """乱数で再現可能な訓練 X 行列 (n, p) を返す。"""
    if p is None:
        p = _N_US
    rng = np.random.default_rng(42)
    return rng.normal(0.0, 0.01, size=(n, p))


def _synthetic_Y(n: int = 80) -> np.ndarray:
    """乱数で再現可能な訓練 Y 行列 (n, 17) を返す。"""
    rng = np.random.default_rng(99)
    return rng.normal(0.0, 0.005, size=(n, _N_JP))


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
) -> PaperV1SignalService:
    return PaperV1SignalService(
        session=mock_session,
        calendar_service=mock_calendar,
        price_repository=mock_price_repo,
        signal_repository=mock_signal_repo,
    )


# ---------------------------------------------------------------------------
# [A] _standardize
# ---------------------------------------------------------------------------


class TestStandardize:
    def test_basic_standardize_mean_zero_std_one(self) -> None:
        """標準化後の列平均は 0、列標準偏差は 1 に近いこと。"""
        rng = np.random.default_rng(0)
        X = rng.normal(5.0, 2.0, size=(100, 3))
        X_std, mu, sigma = _standardize(X)

        np.testing.assert_allclose(X_std.mean(axis=0), 0.0, atol=1e-10)
        np.testing.assert_allclose(X_std.std(axis=0, ddof=1), 1.0, atol=1e-10)
        np.testing.assert_allclose(mu, X.mean(axis=0))
        np.testing.assert_allclose(sigma, X.std(axis=0, ddof=1))

    def test_standardize_with_provided_params(self) -> None:
        """既存の mu/sigma を渡した場合、その値で標準化されること。"""
        mu = np.array([1.0, 2.0])
        sigma = np.array([0.5, 1.0])
        x = np.array([[1.5, 3.0]])

        X_std, returned_mu, returned_sigma = _standardize(x, mu=mu, sigma=sigma)

        expected = (x - mu) / sigma
        np.testing.assert_allclose(X_std, expected)
        # 渡した mu/sigma がそのまま返ること
        np.testing.assert_array_equal(returned_mu, mu)
        np.testing.assert_array_equal(returned_sigma, sigma)

    def test_constant_column_no_zero_division(self) -> None:
        """定数列 (sigma ≈ 0) でもゼロ除算が発生しないこと。"""
        X = np.ones((10, 2))
        X[:, 1] = np.arange(10, dtype=float)  # 2 列目は正常

        X_std, _, sigma = _standardize(X)

        # 定数列は sigma が 0 → _EPSILON で置換 → 全行が 0 になる
        assert np.all(np.isfinite(X_std))
        assert sigma[0] < _EPSILON * 2  # 実際の sigma は 0

    def test_vector_input(self) -> None:
        """1 行ベクトルも動作すること (reshape なし)。"""
        x = np.array([1.0, 2.0, 3.0])
        mu = np.array([0.0, 0.0, 0.0])
        sigma = np.array([1.0, 2.0, 1.0])

        x_std, _, _ = _standardize(x, mu=mu, sigma=sigma)
        np.testing.assert_allclose(x_std, x / sigma)


# ---------------------------------------------------------------------------
# [B] _select_n_components
# ---------------------------------------------------------------------------


class TestSelectNComponents:
    def test_selects_minimum_to_reach_threshold(self) -> None:
        """累積寄与率が threshold に達する最小 K を選ぶこと。"""
        evr = np.array([0.4, 0.25, 0.20, 0.10, 0.05])
        # cumvar: [0.4, 0.65, 0.85, 0.95, 1.0]
        # threshold=0.80 → 3 成分で 0.85 >= 0.80 → K=3
        assert _select_n_components(evr, threshold=0.80, max_k=5) == 3

    def test_first_component_sufficient(self) -> None:
        """1 成分で threshold を超える場合は K=1。"""
        evr = np.array([0.95, 0.04, 0.01])
        assert _select_n_components(evr, threshold=0.80, max_k=5) == 1

    def test_respects_max_k(self) -> None:
        """必要コンポーネント数が max_k を超える場合は max_k で上限クリップ。"""
        evr = np.array([0.1, 0.1, 0.1, 0.1, 0.1, 0.5])
        # cumvar: [0.1, 0.2, 0.3, 0.4, 0.5, 1.0]
        # threshold=0.80 なら K=6 が必要だが max_k=5 でクリップ
        assert _select_n_components(evr, threshold=0.80, max_k=5) == 5

    def test_exact_threshold_at_boundary(self) -> None:
        """cumvar がちょうど threshold に等しいインデックスで選ぶこと。"""
        evr = np.array([0.50, 0.30, 0.20])
        # cumvar: [0.5, 0.8, 1.0]
        # threshold=0.80 → index 1 (0-indexed) で 0.8 >= 0.8 → K=2
        assert _select_n_components(evr, threshold=0.80, max_k=5) == 2


# ---------------------------------------------------------------------------
# [C] _fit_model / _compute_scores
# ---------------------------------------------------------------------------


class TestFitModelAndComputeScores:
    def test_fit_returns_correct_shapes(self) -> None:
        """PaperV1Model の各フィールドの shape が正しいこと。"""
        X = _synthetic_X()
        Y = _synthetic_Y()
        model = _fit_model(X, Y)

        n, p = X.shape
        k = model.n_components

        assert model.mu_x.shape == (p,)
        assert model.sigma_x.shape == (p,)
        assert model.mu_y.shape == (_N_JP,)
        assert model.sigma_y.shape == (_N_JP,)
        assert model.V_k.shape == (p, k)
        assert model.ridge_coef.shape == (k, _N_JP)
        assert 1 <= k <= 5

    def test_compute_scores_returns_shape_17(self) -> None:
        """_compute_scores が shape (17,) の配列を返すこと。"""
        X = _synthetic_X()
        Y = _synthetic_Y()
        model = _fit_model(X, Y)

        rng = np.random.default_rng(7)
        x_new = rng.normal(0.0, 0.01, size=(_N_US,))
        scores = _compute_scores(model, x_new)

        assert scores.shape == (_N_JP,)
        assert np.all(np.isfinite(scores))

    def test_scores_vary_with_different_inputs(self) -> None:
        """異なる x_new に対してスコアが変わること (定数でないこと)。"""
        X = _synthetic_X()
        Y = _synthetic_Y()
        model = _fit_model(X, Y)

        rng = np.random.default_rng(11)
        x1 = rng.normal(0.0, 0.01, size=(_N_US,))
        x2 = rng.normal(0.0, 0.01, size=(_N_US,))

        scores1 = _compute_scores(model, x1)
        scores2 = _compute_scores(model, x2)

        # 異なる入力なのでスコアが同一にはならないはず
        assert not np.allclose(scores1, scores2)

    def test_n_components_within_bounds(self) -> None:
        """n_components が 1 以上 PCA_MAX_COMPONENTS 以下であること。"""
        X = _synthetic_X(n=80, p=11)
        Y = _synthetic_Y(n=80)
        model = _fit_model(X, Y)
        assert 1 <= model.n_components <= 5


# ---------------------------------------------------------------------------
# [D] _rank_and_side_paper_v1
# ---------------------------------------------------------------------------


class TestRankAndSidePaperV1:
    def _make_scores(self, values: list[float]) -> np.ndarray:
        """list[float] を np.ndarray に変換するヘルパー。"""
        assert len(values) == _N_JP
        return np.array(values, dtype=float)

    def test_top_n_long_are_long(self) -> None:
        """上位 N_LONG=5 業種が "long" になること。"""
        # ticker 昇順に 0.16 から 0.01 ずつ下がるスコア
        scores = np.array([0.16 - i * 0.01 for i in range(_N_JP)])
        result = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)

        long_tickers = {t for t, (_, side) in result.items() if side == SuggestedSide.LONG.value}
        assert len(long_tickers) == N_LONG

    def test_bottom_n_short_are_short(self) -> None:
        """下位 N_SHORT=5 業種が "short" になること。"""
        scores = np.array([0.16 - i * 0.01 for i in range(_N_JP)])
        result = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)

        short_tickers = {t for t, (_, side) in result.items() if side == SuggestedSide.SHORT.value}
        assert len(short_tickers) == N_SHORT

    def test_middle_are_neutral(self) -> None:
        """中間 (17 - 5 - 5 = 7) 業種が "neutral" になること。"""
        scores = np.array([0.16 - i * 0.01 for i in range(_N_JP)])
        result = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)

        neutral_tickers = {
            t for t, (_, side) in result.items() if side == SuggestedSide.NEUTRAL.value
        }
        assert len(neutral_tickers) == _N_JP - N_LONG - N_SHORT

    def test_rank_1_is_highest_score(self) -> None:
        """rank=1 が最高スコアの業種に付くこと。"""
        # 先頭の ticker に最高スコアを与える
        scores = np.zeros(_N_JP)
        scores[0] = 1.0  # _ALL_JP_TICKERS[0] が最高スコア
        result = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)

        rank1_ticker = next(t for t, (rank, _) in result.items() if rank == 1)
        assert rank1_ticker == _ALL_JP_TICKERS[0]

    def test_tiebreak_by_ticker_ascending(self) -> None:
        """同スコア時は ticker 昇順でランク付けされること。"""
        # 全業種に同じスコアを与える
        scores = np.ones(_N_JP)
        result = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)

        # rank=1 は ticker が最小の業種になるはず
        rank1_ticker = next(t for t, (rank, _) in result.items() if rank == 1)
        assert rank1_ticker == min(_ALL_JP_TICKERS)

    def test_all_17_tickers_covered(self) -> None:
        """17 業種全てが result に含まれること。"""
        scores = np.linspace(0.01, 0.17, _N_JP)
        result = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)
        assert set(result.keys()) == set(_ALL_JP_TICKERS)

    def test_rank_monotonically_increases_with_decreasing_score(self) -> None:
        """スコアが降順のとき、rank は 1 から 17 に単調増加すること。"""
        scores = np.linspace(0.17, 0.01, _N_JP)  # 降順
        result = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)

        for i, ticker in enumerate(_ALL_JP_TICKERS):
            rank, _ = result[ticker]
            assert rank == i + 1


# ---------------------------------------------------------------------------
# [E] PaperV1SignalService
# ---------------------------------------------------------------------------


class TestPaperV1SignalServiceSkip:
    """訓練データ/現在データ不足でスキップするケース。"""

    def test_empty_alignment_returns_zero_requested(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
    ) -> None:
        """alignment が空の場合は requested=0、succeeded=[] を返すこと。"""
        mock_calendar.build_date_alignment.return_value = pd.DataFrame()

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert result.requested == 0
        assert result.succeeded == []

    def test_skip_when_insufficient_training_data(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
    ) -> None:
        """訓練データが WINDOW_SIZE 未満の場合はスキップ、upsert_many は呼ばれないこと。"""
        mock_calendar.build_date_alignment.side_effect = [
            # 外側の generate_signals_for_range 呼び出し (1 行)
            _make_alignment([(_US_DATE, _JP_DATE)]),
            # 内側の _build_training_data 呼び出し (空 → 訓練データなし)
            pd.DataFrame(),
        ]
        mock_price_repo.get_prices_between.return_value = []
        mock_price_repo.list_by_ticker.return_value = []

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.skipped
        mock_signal_repo.upsert_many.assert_not_called()

    def test_skip_when_current_us_returns_missing(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
    ) -> None:
        """現在の US リターンが取得できない場合もスキップすること。"""
        mock_calendar.build_date_alignment.side_effect = [
            _make_alignment([(_US_DATE, _JP_DATE)]),
            pd.DataFrame(),  # 訓練データも空
        ]
        mock_price_repo.get_prices_between.return_value = []
        mock_price_repo.list_by_ticker.return_value = []
        # 現在 US リターン用: 1 行しかない (2 行必要)
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close_price(100.0, _US_DATE)
        ]

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.skipped


class TestPaperV1SignalServiceNormal:
    """正常系: 17 業種のシグナルが生成される。"""

    def _setup_enough_training_data(
        self,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
    ) -> None:
        """80 有効行の訓練データが得られるようモックを設定する。"""
        # 訓練用 alignment: 80 ペア
        base_us = date(2024, 9, 1)
        base_jp = date(2024, 9, 2)
        training_pairs = [
            (base_us + timedelta(days=i), base_jp + timedelta(days=i))
            for i in range(80)
        ]
        outer_alignment = _make_alignment([(_US_DATE, _JP_DATE)])
        inner_alignment = _make_alignment(training_pairs)

        mock_calendar.build_date_alignment.side_effect = [outer_alignment, inner_alignment]

        # US バッチ取得: 各ティッカーに 82 行の価格 (CC リターン計算分 +2)
        def make_us_batch(ticker: str, start: date, end: date) -> list[MagicMock]:
            n_days = (end - start).days + 3
            return [
                _make_adj_close_price(100.0 + j * 0.01, start + timedelta(days=j))
                for j in range(n_days)
            ]

        # JP バッチ取得: 各ティッカーに 80 行の価格
        def make_jp_batch(ticker: str, *, start: date, end: date) -> list[MagicMock]:
            return [
                _make_oc_price(100.0, 101.0, start + timedelta(days=j))
                for j in range(80)
            ]

        mock_price_repo.get_prices_between.side_effect = make_us_batch
        mock_price_repo.list_by_ticker.side_effect = make_jp_batch

        # 現在 US リターン: 2 行
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close_price(100.0, _US_DATE - timedelta(days=1)),
            _make_adj_close_price(101.0, _US_DATE),
        ]

    def test_generates_17_sector_signals(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """正常系で 17 業種分の SignalDaily が upsert_many に渡ること。"""
        self._setup_enough_training_data(mock_calendar, mock_price_repo)

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.succeeded
        assert result.saved_rows == _N_JP

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        assert len(saved_rows) == _N_JP

    def test_signal_type_is_paper_v1(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """保存される SignalDaily の signal_type が "paper_v1" であること。"""
        self._setup_enough_training_data(mock_calendar, mock_price_repo)

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        assert all(r.signal_type == SIGNAL_TYPE_PAPER_V1 for r in saved_rows)

    def test_commit_called_after_upsert(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """upsert_many の後に session.commit() が呼ばれること。"""
        self._setup_enough_training_data(mock_calendar, mock_price_repo)

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        mock_session.commit.assert_called_once()

    def test_all_17_tickers_present_in_output(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """17 業種の全ティッカーが出力に含まれること。"""
        self._setup_enough_training_data(mock_calendar, mock_price_repo)

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        output_tickers = {r.target_ticker for r in saved_rows}
        assert output_tickers == set(_ALL_JP_TICKERS)

    def test_long_short_neutral_counts(
        self,
        service: PaperV1SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """long 5、short 5、neutral 7 の分布になること。"""
        self._setup_enough_training_data(mock_calendar, mock_price_repo)

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        sides = [r.suggested_side for r in saved_rows]
        assert sides.count(SuggestedSide.LONG.value) == N_LONG
        assert sides.count(SuggestedSide.SHORT.value) == N_SHORT
        assert sides.count(SuggestedSide.NEUTRAL.value) == _N_JP - N_LONG - N_SHORT


class TestLookaheadPrevention:
    """先読み防止: _fetch_current_us_returns が as_of_date=us_signal_date を使うこと。"""

    def test_get_prices_up_to_called_with_us_signal_date(
        self,
        service: PaperV1SignalService,
        mock_price_repo: MagicMock,
    ) -> None:
        """_fetch_current_us_returns 内の get_prices_up_to は
        as_of_date=us_signal_date で呼ばれること。"""
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close_price(100.0),
            _make_adj_close_price(101.0),
        ]

        service._fetch_current_us_returns(_US_DATE)

        for call_args in mock_price_repo.get_prices_up_to.call_args_list:
            _, kwargs = call_args
            assert kwargs.get("as_of_date") == _US_DATE, (
                f"as_of_date が us_signal_date={_US_DATE} ではなく "
                f"{kwargs.get('as_of_date')} で呼ばれた"
            )

    def test_jp_execution_date_not_used_as_as_of_date(
        self,
        service: PaperV1SignalService,
        mock_price_repo: MagicMock,
    ) -> None:
        """_fetch_current_us_returns 内で jp_execution_date を as_of_date に
        渡していないこと (jp_execution_date > us_signal_date が常に成立するため先読みになる)。"""
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close_price(100.0),
            _make_adj_close_price(101.0),
        ]

        service._fetch_current_us_returns(_US_DATE)

        for call_args in mock_price_repo.get_prices_up_to.call_args_list:
            _, kwargs = call_args
            assert kwargs.get("as_of_date") != _JP_DATE, (
                "jp_execution_date が as_of_date として誤って渡されている (先読みバイアス)"
            )

    def test_fetch_current_returns_none_when_prices_insufficient(
        self,
        service: PaperV1SignalService,
        mock_price_repo: MagicMock,
    ) -> None:
        """1 行のみの場合は CC リターンを計算できないため None を返すこと。"""
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close_price(100.0)
        ]

        result = service._fetch_current_us_returns(_US_DATE)

        assert result is None

    def test_fetch_current_returns_array_when_sufficient(
        self,
        service: PaperV1SignalService,
        mock_price_repo: MagicMock,
    ) -> None:
        """2 行以上ある場合は shape (_N_US,) の ndarray を返すこと。"""
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close_price(100.0),
            _make_adj_close_price(101.0),
        ]

        result = service._fetch_current_us_returns(_US_DATE)

        assert result is not None
        assert result.shape == (_N_US,)
        assert np.all(np.isfinite(result))
