"""
BacktestService: signal_daily を使った JP 業種ロングショート日次バックテスト。

# 責務
  - CalendarService.get_jp_business_days() をループ軸として jp_execution_date 列を得る
  - SignalRepository.list_by_jp_execution_date() で各 JP 日のシグナルを取得する
  - PriceRepository.get_oc_on_date() で当日の open/close のみを取得する
  - 等ウェイト 50:50 ドル中立の日次 P&L を計算する
  - BacktestRepository で run / daily_results / summary を保存する

# 先読み防止 (最重要)
  get_oc_on_date には必ず jp_execution_date のみを渡す。
  us_signal_date や翌日以降の価格を渡すことは絶対にしない。
  signal_daily.suggested_side はシグナル生成時に先読み防止済みの価格から算出されており
  バックテスト層では再計算しない。

# ポートフォリオ構成
  - long  : suggested_side == "long"  の業種 (等ウェイト)
  - short : suggested_side == "short" の業種 (等ウェイト)
  - daily_return = (long_book_return + short_book_return) / 2
    ※ 一方が欠損の場合はその側を 0 として計算 (50:50 ドル中立想定)
    ※ 両方欠損の場合は daily_return=None (スキップ)

# モジュールレベル関数
  _compute_oc_return / _compute_daily_return / _compute_summary は
  テスト・再利用のためモジュールスコープで公開する。
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from sqlalchemy.orm import Session

from app.models.backtest import BacktestResultDaily, BacktestStatus
from app.models.signal import SuggestedSide
from app.repositories.backtest_repository import BacktestRepository
from app.repositories.price_repository import PriceRepository
from app.repositories.signal_repository import SignalRepository
from app.services.calendar_service import CalendarService
from app.services.signal_service import SIGNAL_TYPE_SIMPLE_V1

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# パラメータ / 返り値型
# ---------------------------------------------------------------------------


@dataclass
class CostParams:
    """バックテストコストパラメータ。

    Attributes:
        commission_rate: ラウンドトリップ手数料率 (例: 0.001 = 0.1%)。
                         エントリー・エグジット合計のコストを 1 つの数値で表す。
        slippage_rate:   片道エントリースリッページ率 (例: 0.001 = 0.1%)。
                         始値に対してエントリー時に追加的に支払うコストを表す。

    v0_01 デフォルト: 両方 0.0 (コストなし)。
    """

    commission_rate: float = 0.0
    slippage_rate: float = 0.0


@dataclass
class DailyResult:
    """1 jp_execution_date 分の計算結果。BacktestRunResult の要素。

    Attributes:
        jp_execution_date: 対象 JP 執行日
        daily_return:      日次ポートフォリオリターン (コスト考慮後)。全欠損日は None。
        cumulative_return: 期間開始からの累積リターン。スキップ日はキャリーオーバー。
        long_return:       long ブック平均リターン (コスト考慮後)。欠損時は None。
        short_return:      short ブック平均リターン (コスト考慮後)。欠損時は None。
        long_count:        その日の有効 long ポジション数 (価格欠損を除く)。
        short_count:       その日の有効 short ポジション数 (価格欠損を除く)。
    """

    jp_execution_date: date
    daily_return: float | None
    cumulative_return: float
    long_return: float | None
    short_return: float | None
    long_count: int
    short_count: int


@dataclass
class BacktestRunResult:
    """BacktestService.run() の実行結果サマリー。

    Attributes:
        run_id:        DB に保存された BacktestRun の id
        trading_days:  実際に日次リターンを計算できた日数 (None 日を除く)
        total_return:  最終累積リターン
        annual_return: 年率リターン (252 日ベース)
        annual_vol:    年率ボラティリティ (252 日ベース)
        sharpe_ratio:  シャープレシオ
        max_drawdown:  最大ドローダウン (正値)
        win_rate:      勝率 (daily_return > 0 の日数 / trading_days)
        daily_results: 各 jp_execution_date の DailyResult リスト (昇順)
    """

    run_id: int
    trading_days: int
    total_return: float | None
    annual_return: float | None
    annual_vol: float | None
    sharpe_ratio: float | None
    max_drawdown: float | None
    win_rate: float | None
    daily_results: list[DailyResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# モジュールレベル純粋関数 (テスト直接呼び出し可)
# ---------------------------------------------------------------------------


def _compute_oc_return(
    open_price: Decimal,
    close_price: Decimal,
) -> float | None:
    """1 銘柄の Open-to-Close リターンを計算する。

    return = (close - open) / open

    Args:
        open_price:  当日始値。
        close_price: 当日終値。

    Returns:
        float リターン。open_price == 0 の場合は None (ゼロ除算回避)。
    """
    if open_price == 0:
        return None
    return float(close_price - open_price) / float(open_price)


def _compute_daily_return(
    long_oc_returns: list[float],
    short_oc_returns: list[float],
    cost_params: CostParams,
) -> tuple[float | None, float | None, float | None]:
    """日次ポートフォリオリターンを計算する。

    # コスト
      cost = commission_rate + slippage_rate (1 ポジションあたり)
      long  net = oc_return  - cost  (値上がりで利益)
      short net = -oc_return - cost  (値下がりで利益)

    # ポートフォリオ合算
      long_book  = mean(long_net  for valid long  tickers)
      short_book = mean(short_net for valid short tickers)
      daily_return = (long_book + short_book) / 2  [50:50 ドル中立]

      一方のブックが欠損 (=対応 ticker の価格が全欠損) の場合:
        欠損側を 0 とみなして (available_book + 0) / 2 で計算する。
      両方欠損の場合: (None, None, None) を返す。

    Args:
        long_oc_returns:  long ポジションの OC リターンリスト (gross、符号未反転)。
        short_oc_returns: short ポジションの OC リターンリスト (gross、符号未反転)。
        cost_params:      コストパラメータ。

    Returns:
        (daily_return, long_book_return, short_book_return)
        long/short が全欠損の場合の対応する要素は None。
    """
    cost = cost_params.commission_rate + cost_params.slippage_rate

    long_book: float | None = None
    if long_oc_returns:
        long_nets = [r - cost for r in long_oc_returns]
        long_book = sum(long_nets) / len(long_nets)

    short_book: float | None = None
    if short_oc_returns:
        # 符号反転: OC が下がるほど short ポジションは利益
        short_nets = [-r - cost for r in short_oc_returns]
        short_book = sum(short_nets) / len(short_nets)

    if long_book is None and short_book is None:
        return None, None, None

    # 50:50 ドル中立: 欠損側は 0 として計算
    l = long_book if long_book is not None else 0.0
    s = short_book if short_book is not None else 0.0
    daily_return = (l + s) / 2.0

    return daily_return, long_book, short_book


def _compute_summary(
    daily_returns: list[float | None],
) -> dict[str, float | None | int]:
    """日次リターン列からサマリー統計を計算する。

    None 要素 (スキップ日) は統計計算から除外する。
    None 日は wealth が変化しないものとして扱い、
    max_drawdown は有効日の連続する wealth カーブで計算する。

    Args:
        daily_returns: 順序付き日次リターンリスト。None はスキップ日。

    Returns:
        dict with:
            trading_days:  有効日数 (non-None)
            total_return:  最終累積リターン (None if trading_days == 0)
            annual_return: 年率リターン 252 日ベース (None if trading_days == 0)
            annual_vol:    年率ボラ 252 日ベース (None if trading_days <= 1)
            sharpe_ratio:  annual_return / annual_vol (None if vol == 0 or None)
            max_drawdown:  最大ドローダウン 正値 (None if trading_days == 0)
            win_rate:      勝率 (None if trading_days == 0)
    """
    valid: list[float] = [r for r in daily_returns if r is not None]
    n = len(valid)

    if n == 0:
        return {
            "trading_days": 0,
            "total_return": None,
            "annual_return": None,
            "annual_vol": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "win_rate": None,
        }

    # ── 累積リターン / 年率リターン ──────────────────────────────────────
    wealth = 1.0
    for r in valid:
        wealth *= (1.0 + r)
    total_return: float = wealth - 1.0
    annual_return: float = (1.0 + total_return) ** (252.0 / n) - 1.0

    # ── 年率ボラティリティ (標本分散 → 年率換算) ─────────────────────────
    annual_vol: float | None = None
    if n >= 2:
        mean_r = sum(valid) / n
        variance = sum((r - mean_r) ** 2 for r in valid) / (n - 1)
        annual_vol = math.sqrt(variance * 252.0)

    # ── シャープレシオ ────────────────────────────────────────────────────
    sharpe_ratio: float | None = None
    if annual_vol is not None and annual_vol > 0.0:
        sharpe_ratio = annual_return / annual_vol

    # ── 最大ドローダウン (running peak 法) ───────────────────────────────
    peak = 1.0
    cum_w = 1.0
    max_dd = 0.0
    for r in valid:
        cum_w *= (1.0 + r)
        if cum_w > peak:
            peak = cum_w
        dd = (peak - cum_w) / peak
        if dd > max_dd:
            max_dd = dd

    # ── 勝率 ──────────────────────────────────────────────────────────────
    win_rate: float = sum(1 for r in valid if r > 0.0) / n

    return {
        "trading_days": n,
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_vol": annual_vol,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": max_dd,
        "win_rate": win_rate,
    }


# ---------------------------------------------------------------------------
# BacktestService
# ---------------------------------------------------------------------------


class BacktestService:
    """signal_daily を使った JP 業種ロングショート日次バックテストサービス。

    Args:
        session:              SQLAlchemy Session。commit は BacktestService が行う。
        calendar_service:     CalendarService のインスタンス。
        price_repository:     PriceRepository。None の場合は session から自動生成。
        signal_repository:    SignalRepository。None の場合は session から自動生成。
        backtest_repository:  BacktestRepository。None の場合は session から自動生成。
    """

    def __init__(
        self,
        session: Session,
        calendar_service: CalendarService,
        price_repository: PriceRepository | None = None,
        signal_repository: SignalRepository | None = None,
        backtest_repository: BacktestRepository | None = None,
    ) -> None:
        self._session = session
        self._calendar = calendar_service
        self._price_repo = price_repository or PriceRepository(session)
        self._signal_repo = signal_repository or SignalRepository(session)
        self._backtest_repo = backtest_repository or BacktestRepository(session)

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------

    def run(
        self,
        start: date,
        end: date,
        signal_type: str = SIGNAL_TYPE_SIMPLE_V1,
        cost_params: CostParams | None = None,
    ) -> BacktestRunResult:
        """[start, end] を jp_execution_date 軸でバックテストし DB に保存する。

        # ループ軸
          CalendarService.get_jp_business_days(start, end) から JPX 営業日を取得する。
          signal_daily の有無はループ軸に影響しない。

        # 先読み防止
          get_oc_on_date には jp_execution_date のみを渡す。
          us_signal_date へのアクセスはこのメソッド内では一切行わない。

        # 欠損ルール
          - シグナルが 0 件の日: daily_return=None でスキップ
          - long/short 両ブックの価格が全欠損: daily_return=None でスキップ
          - 一方のブックのみ欠損: 欠損側を 0 として (有効側 / 2) で計算

        Args:
            start:       jp_execution_date の開始日 (inclusive)
            end:         jp_execution_date の終了日 (inclusive)
            signal_type: シグナル種別 (デフォルト "simple_v1")
            cost_params: コストパラメータ (None の場合はコストなし)

        Returns:
            BacktestRunResult サマリー。DB に保存済み。
        """
        if cost_params is None:
            cost_params = CostParams()

        # 1. BacktestRun レコードを作成 (status=running)
        run = self._backtest_repo.create_run(
            signal_type=signal_type,
            start_date=start,
            end_date=end,
            commission_rate=cost_params.commission_rate,
            slippage_rate=cost_params.slippage_rate,
        )
        run_id = run.id

        # 2. ループ軸: JP 営業日リスト
        jp_days = self._calendar.get_jp_business_days(start, end)

        logger.info(
            "BacktestService.run 開始: run_id=%d signal_type=%s start=%s end=%s jp_days=%d",
            run_id, signal_type, start, end, len(jp_days),
        )

        # 3. 日次ループ
        daily_orm_rows: list[BacktestResultDaily] = []
        all_daily_returns: list[float | None] = []  # _compute_summary に渡す
        wealth = 1.0  # 累積リターン計算用 (1.0 = 初期資産)

        for jp_date in jp_days:
            daily_ret, long_book, short_book, long_cnt, short_cnt = (
                self._compute_for_date(jp_date, signal_type, cost_params)
            )

            # 欠損日は wealth をキャリーオーバー (wealth は変化しない)
            if daily_ret is not None:
                wealth *= (1.0 + daily_ret)
            cum_return = wealth - 1.0

            all_daily_returns.append(daily_ret)

            daily_orm_rows.append(
                BacktestResultDaily(
                    backtest_run_id=run_id,
                    jp_execution_date=jp_date,
                    daily_return=(
                        Decimal(str(round(daily_ret, 12))) if daily_ret is not None else None
                    ),
                    cumulative_return=Decimal(str(round(cum_return, 12))),
                    long_return=(
                        Decimal(str(round(long_book, 12))) if long_book is not None else None
                    ),
                    short_return=(
                        Decimal(str(round(short_book, 12))) if short_book is not None else None
                    ),
                    long_count=long_cnt,
                    short_count=short_cnt,
                )
            )

            logger.debug(
                "jp_date=%s daily_ret=%s cumulative=%s long=%d short=%d",
                jp_date, daily_ret, cum_return, long_cnt, short_cnt,
            )

        # 4. 日次結果を一括保存
        self._backtest_repo.save_daily_results(daily_orm_rows)

        # 5. サマリー統計を計算
        summary = _compute_summary(all_daily_returns)

        # 6. BacktestRun を完了状態に更新
        self._backtest_repo.finalize_run(
            run_id,
            trading_days=int(summary["trading_days"]),
            total_return=summary["total_return"],          # type: ignore[arg-type]
            annual_return=summary["annual_return"],        # type: ignore[arg-type]
            annual_vol=summary["annual_vol"],              # type: ignore[arg-type]
            sharpe_ratio=summary["sharpe_ratio"],          # type: ignore[arg-type]
            max_drawdown=summary["max_drawdown"],          # type: ignore[arg-type]
            win_rate=summary["win_rate"],                  # type: ignore[arg-type]
            status=BacktestStatus.DONE.value,
        )

        self._session.commit()

        logger.info(
            "BacktestService.run 完了: run_id=%d trading_days=%d total_return=%s",
            run_id, summary["trading_days"], summary.get("total_return"),
        )

        # 7. DailyResult リストを組み立てて返す
        daily_results = [
            DailyResult(
                jp_execution_date=r.jp_execution_date,
                daily_return=float(r.daily_return) if r.daily_return is not None else None,
                cumulative_return=float(r.cumulative_return),
                long_return=float(r.long_return) if r.long_return is not None else None,
                short_return=float(r.short_return) if r.short_return is not None else None,
                long_count=r.long_count or 0,
                short_count=r.short_count or 0,
            )
            for r in daily_orm_rows
        ]

        return BacktestRunResult(
            run_id=run_id,
            trading_days=int(summary["trading_days"]),
            total_return=summary["total_return"],          # type: ignore[return-value]
            annual_return=summary["annual_return"],        # type: ignore[return-value]
            annual_vol=summary["annual_vol"],              # type: ignore[return-value]
            sharpe_ratio=summary["sharpe_ratio"],          # type: ignore[return-value]
            max_drawdown=summary["max_drawdown"],          # type: ignore[return-value]
            win_rate=summary["win_rate"],                  # type: ignore[return-value]
            daily_results=daily_results,
        )

    # ------------------------------------------------------------------
    # プライベート: 1 jp_execution_date の処理
    # ------------------------------------------------------------------

    def _compute_for_date(
        self,
        jp_date: date,
        signal_type: str,
        cost_params: CostParams,
    ) -> tuple[float | None, float | None, float | None, int, int]:
        """1 jp_execution_date の P&L を計算する。

        # 先読み防止の核心
          get_oc_on_date に渡す日付は jp_date のみ。
          us_signal_date や翌日以降の価格を参照することは絶対にしない。
          signal_daily.suggested_side はシグナル生成時に先読み防止済みであり
          バックテスト層では再計算しない。

        Returns:
            (daily_return, long_book_return, short_book_return, long_count, short_count)
            シグナル/価格が全欠損の場合は (None, None, None, 0, 0)。
        """
        # ── シグナル取得 ─────────────────────────────────────────────────
        signals = self._signal_repo.list_by_jp_execution_date(signal_type, jp_date)

        if not signals:
            logger.debug("シグナルなし: jp_date=%s", jp_date)
            return None, None, None, 0, 0

        # ── long / short 対象銘柄を分類 (neutral は対象外) ──────────────
        long_tickers = [
            s.target_ticker
            for s in signals
            if s.suggested_side == SuggestedSide.LONG.value
        ]
        short_tickers = [
            s.target_ticker
            for s in signals
            if s.suggested_side == SuggestedSide.SHORT.value
        ]

        if not long_tickers and not short_tickers:
            # 全業種 neutral → 計算対象なし
            logger.debug("全業種 neutral: jp_date=%s", jp_date)
            return None, None, None, 0, 0

        # ── 価格取得 ─────────────────────────────────────────────────────
        # ★先読み防止: jp_execution_date の open/close のみを参照する。
        #   us_signal_date や jp_date より後の日付を渡してはならない。
        all_tickers = list(dict.fromkeys(long_tickers + short_tickers))
        oc_prices = self._price_repo.get_oc_on_date(jp_date, all_tickers)

        # ── OC リターン計算 ───────────────────────────────────────────────
        long_oc: list[float] = []
        for ticker in long_tickers:
            oc = oc_prices.get(ticker)
            if oc is not None:
                ret = _compute_oc_return(oc[0], oc[1])
                if ret is not None:
                    long_oc.append(ret)

        short_oc: list[float] = []
        for ticker in short_tickers:
            oc = oc_prices.get(ticker)
            if oc is not None:
                ret = _compute_oc_return(oc[0], oc[1])
                if ret is not None:
                    short_oc.append(ret)

        # ── ブック合算 → 日次リターン ────────────────────────────────────
        daily_ret, long_book, short_book = _compute_daily_return(
            long_oc, short_oc, cost_params
        )

        return daily_ret, long_book, short_book, len(long_oc), len(short_oc)
