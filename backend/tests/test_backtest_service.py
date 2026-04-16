"""
tests/test_backtest_service.py — BacktestService / 純粋関数の単体テスト。

テスト戦略:
  - DB / ネットワーク接続なし。
    PriceRepository / SignalRepository / BacktestRepository / Session / CalendarService
    を全て MagicMock で置き換える。
  - 純粋関数 (_compute_oc_return / _compute_daily_return / _compute_summary) は
    モジュールレベルで直接呼び出す。
  - BacktestService.run() は mock repository 経由で呼び出す。

テスト範囲:
  [A] _compute_oc_return
      - 正常 (正リターン / 負リターン / ゼロリターン)
      - open == 0 → None

  [B] _compute_daily_return
      - long3 + short3 (コストなし)
      - long のみ / short のみ (片側欠損)
      - 両方欠損 → (None, None, None)
      - コストが引かれること

  [C] _compute_summary
      - trading_days = non-None 件数
      - total_return の計算
      - annual_return (252 日ベース)
      - annual_vol が non-None (n >= 2)
      - sharpe_ratio
      - max_drawdown (peak-to-trough)
      - win_rate
      - 全 None → trading_days=0, 全統計 None
      - n == 1 → annual_vol=None, sharpe=None

  [D] BacktestService.run()
      - 正常 2 日間: daily_return / cumulative_return
      - シグナル欠損日 → daily_return=None
      - 価格欠損 ticker → その ticker を除いた平均
      - get_oc_on_date に渡す日付が jp_execution_date のみであること (先読み防止)
      - create_run が正しい引数で呼ばれること
      - save_daily_results が jp_days 数分の行を受け取ること
      - finalize_run が trading_days / summary stats で呼ばれること
      - コストゼロ: net == gross
      - daily_return=None の日は cumulative がキャリーオーバーされること
"""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, call

import pytest

from app.models.signal import SuggestedSide
from app.services.backtest_service import (
    BacktestRunResult,
    BacktestService,
    CostParams,
    DailyResult,
    _compute_daily_return,
    _compute_oc_return,
    _compute_summary,
)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_JAN_06 = date(2025, 1, 6)
_JAN_07 = date(2025, 1, 7)
_JAN_08 = date(2025, 1, 8)
_JAN_09 = date(2025, 1, 9)

_SIGNAL_TYPE = "simple_v1"
_NO_COST = CostParams(0.0, 0.0)


# ---------------------------------------------------------------------------
# テスト用ヘルパー
# ---------------------------------------------------------------------------


def _make_signal(ticker: str, side: str, rank: int = 1) -> MagicMock:
    """SignalDaily モックを作る。"""
    sig = MagicMock()
    sig.target_ticker = ticker
    sig.suggested_side = side
    sig.signal_rank = rank
    return sig


def _make_service(
    jp_days: list[date],
    signals_by_date: dict[date, list[MagicMock]] | None = None,
    prices_by_date: dict[date, dict[str, tuple[Decimal, Decimal] | None]] | None = None,
) -> tuple[BacktestService, MagicMock, MagicMock, MagicMock, MagicMock, MagicMock]:
    """BacktestService とその依存モックを一括生成するヘルパー。

    Returns:
        (service, mock_session, mock_calendar, mock_signal_repo, mock_price_repo, mock_bt_repo)
    """
    mock_session = MagicMock()
    mock_calendar = MagicMock()
    mock_signal_repo = MagicMock()
    mock_price_repo = MagicMock()
    mock_bt_repo = MagicMock()

    # CalendarService: get_jp_business_days の返り値を設定
    mock_calendar.get_jp_business_days.return_value = jp_days

    # BacktestRepository: create_run が BacktestRun モックを返す
    mock_run = MagicMock()
    mock_run.id = 42
    mock_bt_repo.create_run.return_value = mock_run
    mock_bt_repo.finalize_run.return_value = mock_run

    # SignalRepository: 日付ごとのシグナルを設定
    _signals = signals_by_date or {}

    def _signal_side_effect(signal_type, jp_date):
        return _signals.get(jp_date, [])

    mock_signal_repo.list_by_jp_execution_date.side_effect = _signal_side_effect

    # PriceRepository: 日付ごとの価格を設定
    _prices = prices_by_date or {}

    def _price_side_effect(business_date, tickers):
        day_prices = _prices.get(business_date, {})
        # tickers 引数に含まれる ticker のみ返す
        return {t: day_prices.get(t, None) for t in tickers}

    mock_price_repo.get_oc_on_date.side_effect = _price_side_effect

    svc = BacktestService(
        session=mock_session,
        calendar_service=mock_calendar,
        price_repository=mock_price_repo,
        signal_repository=mock_signal_repo,
        backtest_repository=mock_bt_repo,
    )
    return svc, mock_session, mock_calendar, mock_signal_repo, mock_price_repo, mock_bt_repo


# ---------------------------------------------------------------------------
# [A] _compute_oc_return
# ---------------------------------------------------------------------------


class TestComputeOcReturn:
    def test_positive_return(self) -> None:
        """値上がり: (100 → 110) = 10%。"""
        result = _compute_oc_return(Decimal("100"), Decimal("110"))
        assert result == pytest.approx(0.10)

    def test_negative_return(self) -> None:
        """値下がり: (100 → 90) = -10%。"""
        result = _compute_oc_return(Decimal("100"), Decimal("90"))
        assert result == pytest.approx(-0.10)

    def test_zero_return(self) -> None:
        """変わらず: (100 → 100) = 0%。"""
        result = _compute_oc_return(Decimal("100"), Decimal("100"))
        assert result == pytest.approx(0.0)

    def test_open_price_zero_returns_none(self) -> None:
        """open == 0 はゼロ除算のため None を返す。"""
        result = _compute_oc_return(Decimal("0"), Decimal("100"))
        assert result is None

    def test_fractional_return(self) -> None:
        """小数リターン: (200 → 201) = 0.5%。"""
        result = _compute_oc_return(Decimal("200"), Decimal("201"))
        assert result == pytest.approx(0.005)


# ---------------------------------------------------------------------------
# [B] _compute_daily_return
# ---------------------------------------------------------------------------


class TestComputeDailyReturn:
    def test_long3_short3_no_cost(self) -> None:
        """long3 + short3 (コストなし): 各ブックの平均が正しいこと。

        long OC: [0.02, 0.04, 0.06] → long_book = 0.04
        short OC: [-0.02, -0.04, -0.06] → short_net = [0.02, 0.04, 0.06] → short_book = 0.04
        daily = (0.04 + 0.04) / 2 = 0.04
        """
        long_oc = [0.02, 0.04, 0.06]
        short_oc = [-0.02, -0.04, -0.06]
        daily, long_book, short_book = _compute_daily_return(long_oc, short_oc, _NO_COST)
        assert daily == pytest.approx(0.04)
        assert long_book == pytest.approx(0.04)
        assert short_book == pytest.approx(0.04)

    def test_long_only_short_empty(self) -> None:
        """short が全欠損: short_book=None、daily = long / 2。

        long_book = 0.04, short=0 → daily = (0.04 + 0) / 2 = 0.02
        """
        long_oc = [0.02, 0.04, 0.06]
        daily, long_book, short_book = _compute_daily_return(long_oc, [], _NO_COST)
        assert short_book is None
        assert long_book == pytest.approx(0.04)
        assert daily == pytest.approx(0.02)

    def test_short_only_long_empty(self) -> None:
        """long が全欠損: long_book=None、daily = short / 2。

        short OC = [-0.04] → short_net = 0.04 → daily = (0 + 0.04) / 2 = 0.02
        """
        short_oc = [-0.04]
        daily, long_book, short_book = _compute_daily_return([], short_oc, _NO_COST)
        assert long_book is None
        assert short_book == pytest.approx(0.04)
        assert daily == pytest.approx(0.02)

    def test_both_empty_returns_none(self) -> None:
        """long / short 両方欠損 → (None, None, None)。"""
        daily, long_book, short_book = _compute_daily_return([], [], _NO_COST)
        assert daily is None
        assert long_book is None
        assert short_book is None

    def test_cost_reduces_return(self) -> None:
        """コストが各ポジションから差し引かれること。

        long OC = [0.02, 0.02, 0.02], cost = 0.005
        long_net = [0.015, 0.015, 0.015], long_book = 0.015
        short OC = [-0.02, -0.02, -0.02], short_net = [0.015, 0.015, 0.015]
        short_book = 0.015
        daily = (0.015 + 0.015) / 2 = 0.015
        """
        cost = CostParams(commission_rate=0.003, slippage_rate=0.002)  # total 0.005
        long_oc = [0.02, 0.02, 0.02]
        short_oc = [-0.02, -0.02, -0.02]
        daily, long_book, short_book = _compute_daily_return(long_oc, short_oc, cost)
        assert long_book == pytest.approx(0.015)
        assert short_book == pytest.approx(0.015)
        assert daily == pytest.approx(0.015)

    def test_cost_with_no_cost_params_is_gross(self) -> None:
        """CostParams(0,0) のとき net == gross。"""
        long_oc = [0.01, 0.03]
        daily_nocost, _, _ = _compute_daily_return(long_oc, [], _NO_COST)
        daily_gross = sum(long_oc) / len(long_oc) / 2  # 50:50
        assert daily_nocost == pytest.approx(daily_gross)

    def test_sign_flip_for_short(self) -> None:
        """short 側は OC リターンが正 (値上がり) のとき短期ポジションは損失。"""
        # 短いポジションが値上がり → short_net は負
        short_oc = [0.05]  # 値上がり → ショートは損失
        daily, long_book, short_book = _compute_daily_return([], short_oc, _NO_COST)
        assert short_book == pytest.approx(-0.05)
        assert daily == pytest.approx(-0.025)  # (0 + (-0.05)) / 2


# ---------------------------------------------------------------------------
# [C] _compute_summary
# ---------------------------------------------------------------------------


class TestComputeSummary:
    def test_all_none_returns_zero_trading_days(self) -> None:
        """全 None → trading_days=0、全統計 None。"""
        result = _compute_summary([None, None, None])
        assert result["trading_days"] == 0
        assert result["total_return"] is None
        assert result["annual_return"] is None
        assert result["annual_vol"] is None
        assert result["sharpe_ratio"] is None
        assert result["max_drawdown"] is None
        assert result["win_rate"] is None

    def test_empty_list_returns_zero_trading_days(self) -> None:
        """空リスト → trading_days=0。"""
        result = _compute_summary([])
        assert result["trading_days"] == 0

    def test_trading_days_counts_only_non_none(self) -> None:
        """trading_days は None を除いた件数。"""
        result = _compute_summary([0.01, None, 0.02, None, None])
        assert result["trading_days"] == 2

    def test_total_return_compound(self) -> None:
        """total_return は複利計算: (1+r1)*(1+r2) - 1。"""
        result = _compute_summary([0.1, 0.1])
        # (1.1 * 1.1) - 1 = 0.21
        assert result["total_return"] == pytest.approx(0.21)

    def test_total_return_with_none_days_skipped(self) -> None:
        """None 日はリターン計算に影響しない。"""
        result_without_none = _compute_summary([0.1, 0.1])
        result_with_none = _compute_summary([0.1, None, 0.1])
        assert result_without_none["total_return"] == pytest.approx(
            result_with_none["total_return"]
        )

    def test_annual_return_uses_252_base(self) -> None:
        """annual_return = (1 + total)^(252/n) - 1。"""
        returns = [0.01, 0.01]  # n=2
        result = _compute_summary(returns)
        total = 1.01 * 1.01 - 1
        expected = (1 + total) ** (252 / 2) - 1
        assert result["annual_return"] == pytest.approx(expected, rel=1e-6)

    def test_annual_vol_none_for_single_day(self) -> None:
        """n == 1 → annual_vol=None、sharpe=None。"""
        result = _compute_summary([0.01])
        assert result["annual_vol"] is None
        assert result["sharpe_ratio"] is None

    def test_annual_vol_computed_for_two_days(self) -> None:
        """n >= 2 → annual_vol が計算されること。"""
        result = _compute_summary([0.01, -0.01])
        assert result["annual_vol"] is not None
        assert result["annual_vol"] > 0

    def test_annual_vol_formula(self) -> None:
        """annual_vol = sample_std(returns) * sqrt(252)。"""
        returns = [0.02, -0.02, 0.01]
        result = _compute_summary(returns)
        n = 3
        mean_r = sum(returns) / n
        var = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
        expected_vol = math.sqrt(var * 252)
        assert result["annual_vol"] == pytest.approx(expected_vol, rel=1e-6)

    def test_sharpe_ratio_computed(self) -> None:
        """sharpe = annual_return / annual_vol。"""
        returns = [0.02, 0.01, 0.03]
        result = _compute_summary(returns)
        if result["annual_vol"] and result["annual_vol"] > 0:
            expected = result["annual_return"] / result["annual_vol"]
            assert result["sharpe_ratio"] == pytest.approx(expected, rel=1e-6)

    def test_max_drawdown_peak_to_trough(self) -> None:
        """max_drawdown: ピークから谷への最大下落率。

        returns = [0.1, -0.2, 0.0]
        wealth:   1.0 → 1.1 → 0.88 → 0.88
        peak=1.1, trough=0.88, dd = (1.1 - 0.88) / 1.1 = 0.2
        """
        returns = [0.1, -0.2, 0.0]
        result = _compute_summary(returns)
        # (1.1 - 0.88) / 1.1 = 0.2
        expected_dd = (1.1 - 1.1 * 0.8) / 1.1
        assert result["max_drawdown"] == pytest.approx(expected_dd, rel=1e-6)

    def test_max_drawdown_zero_when_always_up(self) -> None:
        """常に上昇 → max_drawdown = 0。"""
        result = _compute_summary([0.01, 0.02, 0.01])
        assert result["max_drawdown"] == pytest.approx(0.0, abs=1e-10)

    def test_win_rate_half(self) -> None:
        """勝率 50%: 正リターン 2 / 負リターン 2。"""
        result = _compute_summary([0.01, -0.01, 0.02, -0.02])
        assert result["win_rate"] == pytest.approx(0.5)

    def test_win_rate_all_positive(self) -> None:
        """全勝 → win_rate = 1.0。"""
        result = _compute_summary([0.01, 0.02, 0.03])
        assert result["win_rate"] == pytest.approx(1.0)

    def test_win_rate_all_negative(self) -> None:
        """全敗 → win_rate = 0.0。"""
        result = _compute_summary([-0.01, -0.02])
        assert result["win_rate"] == pytest.approx(0.0)

    def test_none_days_not_counted_in_win_rate(self) -> None:
        """None 日は win_rate の分母に含まれない。"""
        # valid = [0.01, -0.01] → win_rate = 0.5
        result_with_none = _compute_summary([0.01, None, -0.01, None])
        result_without_none = _compute_summary([0.01, -0.01])
        assert result_with_none["win_rate"] == pytest.approx(
            result_without_none["win_rate"]
        )


# ---------------------------------------------------------------------------
# [D] BacktestService.run()
# ---------------------------------------------------------------------------


class TestBacktestServiceRun:
    """BacktestService.run() の統合テスト (mock repo 使用)。"""

    # ------------------------------------------------------------------
    # 正常系 2 日間
    # ------------------------------------------------------------------

    def test_two_day_run_succeeds(self) -> None:
        """2 日間の正常系: BacktestRunResult が返ること。"""
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value, rank=1),
                _make_signal("1618.T", SuggestedSide.LONG.value, rank=2),
                _make_signal("1619.T", SuggestedSide.LONG.value, rank=3),
                _make_signal("1631.T", SuggestedSide.SHORT.value, rank=15),
                _make_signal("1632.T", SuggestedSide.SHORT.value, rank=16),
                _make_signal("1633.T", SuggestedSide.SHORT.value, rank=17),
            ],
            _JAN_07: [
                _make_signal("1617.T", SuggestedSide.LONG.value, rank=1),
                _make_signal("1631.T", SuggestedSide.SHORT.value, rank=17),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": (Decimal("100"), Decimal("102")),
                "1618.T": (Decimal("100"), Decimal("103")),
                "1619.T": (Decimal("100"), Decimal("101")),
                "1631.T": (Decimal("100"), Decimal("98")),
                "1632.T": (Decimal("100"), Decimal("97")),
                "1633.T": (Decimal("100"), Decimal("96")),
            },
            _JAN_07: {
                "1617.T": (Decimal("102"), Decimal("103")),
                "1631.T": (Decimal("98"), Decimal("97")),
            },
        }
        svc, *_ = _make_service([_JAN_06, _JAN_07], signals, prices)
        result = svc.run(_JAN_06, _JAN_07, signal_type=_SIGNAL_TYPE)

        assert isinstance(result, BacktestRunResult)
        assert result.run_id == 42
        assert len(result.daily_results) == 2

    def test_two_day_daily_return_values(self) -> None:
        """日次リターンが正しく計算されること (コストなし)。

        Jan 6:
          long: 1617.T +2%, 1618.T +3%, 1619.T +1% → long_book = 2%
          short: 1631.T -2%, 1632.T -3%, 1633.T -4% → short_net = +2%, +3%, +4% → short_book = 3%
          daily = (2% + 3%) / 2 = 2.5%
        """
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1618.T", SuggestedSide.LONG.value),
                _make_signal("1619.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
                _make_signal("1632.T", SuggestedSide.SHORT.value),
                _make_signal("1633.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": (Decimal("100"), Decimal("102")),   # +2%
                "1618.T": (Decimal("100"), Decimal("103")),   # +3%
                "1619.T": (Decimal("100"), Decimal("101")),   # +1%
                "1631.T": (Decimal("100"), Decimal("98")),    # -2%
                "1632.T": (Decimal("100"), Decimal("97")),    # -3%
                "1633.T": (Decimal("100"), Decimal("96")),    # -4%
            },
        }
        svc, *_ = _make_service([_JAN_06], signals, prices)
        result = svc.run(_JAN_06, _JAN_06, signal_type=_SIGNAL_TYPE)

        dr = result.daily_results[0]
        # long_book = (0.02 + 0.03 + 0.01) / 3 = 0.02
        # short_book = (0.02 + 0.03 + 0.04) / 3 = 0.03
        # daily = (0.02 + 0.03) / 2 = 0.025
        assert dr.daily_return == pytest.approx(0.025, rel=1e-6)
        assert dr.long_return == pytest.approx(0.02, rel=1e-6)
        assert dr.short_return == pytest.approx(0.03, rel=1e-6)

    def test_long_count_and_short_count(self) -> None:
        """long_count / short_count が正しく記録されること。"""
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1618.T", SuggestedSide.LONG.value),
                _make_signal("1619.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
                _make_signal("1632.T", SuggestedSide.SHORT.value),
                _make_signal("1633.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": (Decimal("100"), Decimal("102")),
                "1618.T": (Decimal("100"), Decimal("103")),
                "1619.T": (Decimal("100"), Decimal("101")),
                "1631.T": (Decimal("100"), Decimal("98")),
                "1632.T": (Decimal("100"), Decimal("97")),
                "1633.T": (Decimal("100"), Decimal("96")),
            },
        }
        svc, *_ = _make_service([_JAN_06], signals, prices)
        result = svc.run(_JAN_06, _JAN_06)

        dr = result.daily_results[0]
        assert dr.long_count == 3
        assert dr.short_count == 3

    # ------------------------------------------------------------------
    # シグナル欠損日
    # ------------------------------------------------------------------

    def test_missing_signal_day_returns_none(self) -> None:
        """シグナルが 0 件の日は daily_return=None になること。"""
        signals = {
            _JAN_06: [],  # シグナルなし
        }
        svc, *_ = _make_service([_JAN_06], signals)
        result = svc.run(_JAN_06, _JAN_06)

        dr = result.daily_results[0]
        assert dr.daily_return is None

    def test_missing_signal_day_does_not_count_in_trading_days(self) -> None:
        """シグナル欠損日は trading_days に含まれないこと。"""
        signals = {
            _JAN_06: [],
            _JAN_07: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_07: {
                "1617.T": (Decimal("100"), Decimal("102")),
                "1631.T": (Decimal("100"), Decimal("98")),
            },
        }
        svc, *_ = _make_service([_JAN_06, _JAN_07], signals, prices)
        result = svc.run(_JAN_06, _JAN_07)

        assert result.trading_days == 1  # Jan 6 は None → 1 日のみ

    # ------------------------------------------------------------------
    # 価格欠損
    # ------------------------------------------------------------------

    def test_partial_price_missing_uses_available(self) -> None:
        """一部 ticker の価格が欠損している場合、有効な ticker だけで平均を計算する。

        long: 1617.T +2% (価格あり), 1618.T (価格なし → 除外)
        → long_book = 2% (1 銘柄のみ)
        """
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1618.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": (Decimal("100"), Decimal("102")),  # +2%
                "1618.T": None,                              # 欠損
                "1631.T": (Decimal("100"), Decimal("98")),   # -2%
            },
        }
        svc, *_ = _make_service([_JAN_06], signals, prices)
        result = svc.run(_JAN_06, _JAN_06)

        dr = result.daily_results[0]
        # long_book = 0.02 (1618.T 除外)
        assert dr.long_return == pytest.approx(0.02, rel=1e-6)
        assert dr.long_count == 1  # 有効 1 銘柄

    def test_all_long_price_missing_uses_short_only(self) -> None:
        """long 側の全 ticker が価格欠損 → short のみで daily = short / 2。"""
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": None,                             # 全欠損
                "1631.T": (Decimal("100"), Decimal("96")),  # -4%
            },
        }
        svc, *_ = _make_service([_JAN_06], signals, prices)
        result = svc.run(_JAN_06, _JAN_06)

        dr = result.daily_results[0]
        # short_net = 0.04, daily = (0 + 0.04) / 2 = 0.02
        assert dr.long_return is None
        assert dr.short_return == pytest.approx(0.04, rel=1e-6)
        assert dr.daily_return == pytest.approx(0.02, rel=1e-6)

    def test_all_prices_missing_returns_none(self) -> None:
        """long / short 両方の全 ticker が価格欠損 → daily_return=None。"""
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": None,
                "1631.T": None,
            },
        }
        svc, *_ = _make_service([_JAN_06], signals, prices)
        result = svc.run(_JAN_06, _JAN_06)

        dr = result.daily_results[0]
        assert dr.daily_return is None

    # ------------------------------------------------------------------
    # ★先読み防止: get_oc_on_date に渡す日付
    # ------------------------------------------------------------------

    def test_get_oc_on_date_called_with_jp_execution_date(self) -> None:
        """★先読み防止: get_oc_on_date に渡す日付が jp_execution_date のみであること。

        BacktestService は us_signal_date にアクセスしない。
        get_oc_on_date に渡す日付は CalendarService から取得した jp_days のみ。
        """
        jp_days = [_JAN_06, _JAN_07]
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
            _JAN_07: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": (Decimal("100"), Decimal("102")),
                "1631.T": (Decimal("100"), Decimal("98")),
            },
            _JAN_07: {
                "1617.T": (Decimal("102"), Decimal("104")),
                "1631.T": (Decimal("98"), Decimal("96")),
            },
        }
        svc, _, _, _, mock_price_repo, _ = _make_service(jp_days, signals, prices)
        svc.run(_JAN_06, _JAN_07)

        # get_oc_on_date が呼ばれた日付を検証
        actual_dates = [
            call_args.args[0]
            for call_args in mock_price_repo.get_oc_on_date.call_args_list
        ]
        assert actual_dates == [_JAN_06, _JAN_07], (
            f"get_oc_on_date に渡した日付が jp_execution_dates と一致しない: {actual_dates}"
        )

    # ------------------------------------------------------------------
    # BacktestRepository の呼び出し検証
    # ------------------------------------------------------------------

    def test_create_run_called_once_with_correct_args(self) -> None:
        """create_run が正しい引数で 1 回呼ばれること。"""
        cost = CostParams(commission_rate=0.001, slippage_rate=0.0005)
        svc, _, _, _, _, mock_bt_repo = _make_service([_JAN_06])
        svc.run(_JAN_06, _JAN_07, signal_type="pca_v1", cost_params=cost)

        mock_bt_repo.create_run.assert_called_once()
        kwargs = mock_bt_repo.create_run.call_args.kwargs
        assert kwargs["signal_type"] == "pca_v1"
        assert kwargs["start_date"] == _JAN_06
        assert kwargs["end_date"] == _JAN_07
        assert kwargs["commission_rate"] == 0.001
        assert kwargs["slippage_rate"] == 0.0005

    def test_save_daily_results_called_with_all_days(self) -> None:
        """save_daily_results が jp_days の全日分の行を受け取ること。"""
        jp_days = [_JAN_06, _JAN_07, _JAN_08]
        svc, _, _, _, _, mock_bt_repo = _make_service(jp_days)
        svc.run(_JAN_06, _JAN_08)

        mock_bt_repo.save_daily_results.assert_called_once()
        rows = mock_bt_repo.save_daily_results.call_args.args[0]
        assert len(rows) == 3

    def test_finalize_run_called_with_correct_run_id(self) -> None:
        """finalize_run が create_run で取得した run_id で呼ばれること。"""
        svc, _, _, _, _, mock_bt_repo = _make_service([_JAN_06])
        svc.run(_JAN_06, _JAN_06)

        mock_bt_repo.finalize_run.assert_called_once()
        call_args = mock_bt_repo.finalize_run.call_args
        assert call_args.args[0] == 42  # mock_run.id = 42

    def test_finalize_run_receives_trading_days(self) -> None:
        """finalize_run に trading_days が渡されること。"""
        signals = {
            _JAN_06: [],  # skip → not counted
            _JAN_07: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_07: {
                "1617.T": (Decimal("100"), Decimal("101")),
                "1631.T": (Decimal("100"), Decimal("99")),
            },
        }
        svc, _, _, _, _, mock_bt_repo = _make_service(
            [_JAN_06, _JAN_07], signals, prices
        )
        svc.run(_JAN_06, _JAN_07)

        kwargs = mock_bt_repo.finalize_run.call_args.kwargs
        assert kwargs["trading_days"] == 1  # Jan 6 は skip

    def test_session_commit_called(self) -> None:
        """session.commit() が呼ばれること。"""
        svc, mock_session, *_ = _make_service([_JAN_06])
        svc.run(_JAN_06, _JAN_06)
        mock_session.commit.assert_called_once()

    # ------------------------------------------------------------------
    # コストゼロ確認
    # ------------------------------------------------------------------

    def test_zero_cost_net_equals_gross(self) -> None:
        """CostParams(0.0, 0.0) のとき、計算されるリターンはコストなしの gross と一致する。

        long +2%, short -3% (→ short_net +3%) のとき
        daily = (0.02 + 0.03) / 2 = 0.025
        """
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": (Decimal("100"), Decimal("102")),  # +2%
                "1631.T": (Decimal("100"), Decimal("97")),   # -3%
            },
        }
        svc, *_ = _make_service([_JAN_06], signals, prices)
        result = svc.run(_JAN_06, _JAN_06, cost_params=CostParams(0.0, 0.0))

        dr = result.daily_results[0]
        assert dr.daily_return == pytest.approx(0.025, rel=1e-6)

    # ------------------------------------------------------------------
    # cumulative_return キャリーオーバー
    # ------------------------------------------------------------------

    def test_cumulative_carry_over_on_none_day(self) -> None:
        """daily_return=None の日は cumulative_return がキャリーオーバーされること。

        Jan 6: daily=0.01, cumulative=0.01
        Jan 7: daily=None (シグナルなし), cumulative=0.01 (キャリーオーバー)
        Jan 8: daily=0.02, cumulative ≈ (1.01 * 1.02) - 1
        """
        signals = {
            _JAN_06: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
            _JAN_07: [],  # シグナルなし → daily_return=None
            _JAN_08: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_06: {
                "1617.T": (Decimal("100"), Decimal("102")),   # +2%
                "1631.T": (Decimal("100"), Decimal("98")),    # -2% → short_net +2%
            },
            _JAN_08: {
                "1617.T": (Decimal("102"), Decimal("104.04")),  # +2%
                "1631.T": (Decimal("98"), Decimal("96.04")),    # -2% → short_net +2%
            },
        }
        svc, *_ = _make_service([_JAN_06, _JAN_07, _JAN_08], signals, prices)
        result = svc.run(_JAN_06, _JAN_08)

        dr_06 = result.daily_results[0]
        dr_07 = result.daily_results[1]
        dr_08 = result.daily_results[2]

        # Jan 6: daily = (0.02 + 0.02) / 2 = 0.02, cumulative = 0.02
        assert dr_06.daily_return == pytest.approx(0.02, rel=1e-6)
        assert dr_06.cumulative_return == pytest.approx(0.02, rel=1e-6)

        # Jan 7: daily = None, cumulative = carry-over = 0.02
        assert dr_07.daily_return is None
        assert dr_07.cumulative_return == pytest.approx(0.02, rel=1e-6)

        # Jan 8: daily = 0.02, cumulative = (1.02 * 1.02) - 1 = 0.0404
        assert dr_08.daily_return == pytest.approx(0.02, rel=1e-6)
        assert dr_08.cumulative_return == pytest.approx(1.02 * 1.02 - 1, rel=1e-6)

    def test_cumulative_starts_at_zero_before_first_valid_day(self) -> None:
        """最初の日が None の場合、cumulative は 0.0 から始まること。"""
        signals = {
            _JAN_06: [],  # None
            _JAN_07: [
                _make_signal("1617.T", SuggestedSide.LONG.value),
                _make_signal("1631.T", SuggestedSide.SHORT.value),
            ],
        }
        prices = {
            _JAN_07: {
                "1617.T": (Decimal("100"), Decimal("101")),
                "1631.T": (Decimal("100"), Decimal("99")),
            },
        }
        svc, *_ = _make_service([_JAN_06, _JAN_07], signals, prices)
        result = svc.run(_JAN_06, _JAN_07)

        # Jan 6 (None): cumulative = 0.0
        assert result.daily_results[0].cumulative_return == pytest.approx(0.0, abs=1e-10)
