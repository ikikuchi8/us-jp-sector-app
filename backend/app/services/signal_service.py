"""
SignalService: US 業種 ETF リターン → JP 業種シグナル生成ロジック。

# 責務
  - CalendarService.build_date_alignment() をループ軸にして (jp_execution_date, us_signal_date) を決定する
  - PriceRepository.get_prices_up_to(as_of_date=us_signal_date) で US 価格を取得する
  - sector_mapping の等ウェイト平均で JP 業種スコアを算出する
  - SignalRepository.upsert_many で signal_daily に保存する
  - jp_execution_date 単位で commit する (fail-soft)

# 先読み防止
  get_prices_up_to の as_of_date には必ず us_signal_date を渡す。
  jp_execution_date は as_of_date として絶対に使わない。
  CalendarService が jp_execution_date > us_signal_date を構造的に保証する。

# signal_type
  v0_01 では "simple_v1" 固定。将来の PCA 版等と共存できるよう signal_type を持つ。

# モジュールレベル関数
  _score_jp_sectors / _rank_and_side はテスト・再利用のためモジュールスコープで公開する。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Final

from sqlalchemy.orm import Session

from app.models.signal import SignalDaily, SuggestedSide
from app.repositories.price_repository import PriceRepository
from app.repositories.signal_repository import SignalRepository
from app.seed_data.sector_mapping import ALL_JP_TICKERS, JP_TICKER_TO_US_TICKERS
from app.services.calendar_service import (
    COL_JP_EXECUTION_DATE,
    COL_US_SIGNAL_DATE,
    CalendarService,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# モジュール定数
# ---------------------------------------------------------------------------

SIGNAL_TYPE_SIMPLE_V1: Final[str] = "simple_v1"

_LONG_RANK_CUTOFF: Final[int] = 3   # rank 1〜3 → long
_SHORT_RANK_CUTOFF: Final[int] = 3  # 下位 3 → short

# sector_mapping から導出した全 US ティッカーのセット (モジュールロード時に 1 回だけ構築)
_ALL_US_TICKERS: Final[frozenset[str]] = frozenset(
    ticker
    for us_list in JP_TICKER_TO_US_TICKERS.values()
    for ticker in us_list
)


# ---------------------------------------------------------------------------
# 返り値型
# ---------------------------------------------------------------------------

@dataclass
class SignalGenerationResult:
    """generate_signals_for_range() の実行結果サマリー。

    Attributes:
        requested:  処理対象の jp_execution_date 数 (alignment の行数)
        saved_rows: signal_daily に保存した行数
        succeeded:  正常完了した jp_execution_date のリスト
        failed:     失敗した jp_execution_date → エラーメッセージ の dict
        skipped:    US 価格が全欠損でスキップした jp_execution_date のリスト
    """

    requested: int = 0
    saved_rows: int = 0
    succeeded: list[date] = field(default_factory=list)
    failed: dict[date, str] = field(default_factory=dict)
    skipped: list[date] = field(default_factory=list)

    @property
    def has_failure(self) -> bool:
        """1 件以上の失敗があれば True。"""
        return bool(self.failed)


# ---------------------------------------------------------------------------
# SignalService
# ---------------------------------------------------------------------------

class SignalService:
    """US 業種 ETF リターンから JP 業種シグナルを生成・保存するサービス。

    Args:
        session:           SQLAlchemy Session。commit は SignalService が jp_date 単位で行う。
        calendar_service:  CalendarService のインスタンス。build_date_alignment() に使用する。
        price_repository:  PriceRepository。None の場合は session から自動生成する。
        signal_repository: SignalRepository。None の場合は session から自動生成する。
        signal_type:       シグナル種別識別子。デフォルト "simple_v1"。
    """

    def __init__(
        self,
        session: Session,
        calendar_service: CalendarService,
        price_repository: PriceRepository | None = None,
        signal_repository: SignalRepository | None = None,
        signal_type: str = SIGNAL_TYPE_SIMPLE_V1,
    ) -> None:
        self._session = session
        self._calendar = calendar_service
        self._price_repo = price_repository or PriceRepository(session)
        self._signal_repo = signal_repository or SignalRepository(session)
        self._signal_type = signal_type

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------

    def generate_signals_for_range(
        self,
        start: date,
        end: date,
    ) -> SignalGenerationResult:
        """[start, end] 内の全 jp_execution_date に対してシグナルを生成・保存する。

        CalendarService.build_date_alignment() をループ軸の正本とする。
        jp_execution_date 単位で fail-soft commit する。

        Args:
            start: jp_execution_date の開始日 (inclusive)
            end:   jp_execution_date の終了日 (inclusive)

        Returns:
            SignalGenerationResult サマリー。
        """
        alignment = self._calendar.build_date_alignment(start, end)
        result = SignalGenerationResult(requested=len(alignment))

        if alignment.empty:
            logger.info(
                "generate_signals_for_range: 対象日なし (start=%s end=%s)", start, end
            )
            return result

        for _, row in alignment.iterrows():
            jp_date: date = row[COL_JP_EXECUTION_DATE]
            us_date: date = row[COL_US_SIGNAL_DATE]
            self._process_date(jp_date, us_date, result)

        logger.info(
            "generate_signals_for_range 完了: requested=%d succeeded=%d "
            "failed=%d skipped=%d saved_rows=%d",
            result.requested,
            len(result.succeeded),
            len(result.failed),
            len(result.skipped),
            result.saved_rows,
        )
        return result

    # ------------------------------------------------------------------
    # プライベート: jp_execution_date 単位処理
    # ------------------------------------------------------------------

    def _process_date(
        self,
        jp_execution_date: date,
        us_signal_date: date,
        result: SignalGenerationResult,
    ) -> None:
        """1 jp_execution_date の処理を行い result を更新する (fail-soft)。"""
        try:
            rows = self._generate_for_date(jp_execution_date, us_signal_date)

            if not rows:
                result.skipped.append(jp_execution_date)
                logger.info(
                    "シグナル生成スキップ: jp_date=%s us_date=%s (全 US 価格欠損)",
                    jp_execution_date, us_signal_date,
                )
                return

            self._signal_repo.upsert_many(rows)
            self._session.commit()
            result.succeeded.append(jp_execution_date)
            result.saved_rows += len(rows)
            logger.debug("保存完了: jp_date=%s rows=%d", jp_execution_date, len(rows))

        except Exception as exc:
            self._session.rollback()
            result.failed[jp_execution_date] = str(exc)
            logger.warning(
                "シグナル生成失敗: jp_date=%s: %s", jp_execution_date, exc, exc_info=True
            )

    def _generate_for_date(
        self,
        jp_execution_date: date,
        us_signal_date: date,
    ) -> list[SignalDaily]:
        """1 つの (jp_execution_date, us_signal_date) に対して 17 業種分の SignalDaily を生成する。

        # 先読み防止
          _compute_us_returns に us_signal_date を渡す。
          jp_execution_date は価格取得に一切使わない。

        Returns:
            SignalDaily のリスト (保存はしない)。
            全 US リターンが欠損の場合は空リストを返す (caller が skipped に記録)。
        """
        # ── US リターン計算 (as_of_date = us_signal_date で先読みを防止) ──
        us_returns = self._compute_us_returns(us_signal_date)

        # 全 US ティッカーが欠損 → 意味あるシグナルを生成できない
        if not us_returns or all(v is None for v in us_returns.values()):
            return []

        # ── JP 業種スコア計算 ──
        jp_scores = _score_jp_sectors(us_returns)

        # ── ランク・サイド判定 ──
        rank_side = _rank_and_side(jp_scores)

        # ── SignalDaily オブジェクト生成 ──
        rows: list[SignalDaily] = []
        for jp_ticker in ALL_JP_TICKERS:
            rank, side = rank_side[jp_ticker]
            score = jp_scores[jp_ticker]
            mapped_us = JP_TICKER_TO_US_TICKERS[jp_ticker]

            # input_metadata_json: この JP 業種に対応する US 銘柄のリターンのみ記録
            metadata: dict = {
                "us_returns": {
                    t: (float(us_returns[t]) if us_returns.get(t) is not None else None)
                    for t in mapped_us
                    if t in us_returns
                }
            }

            rows.append(
                SignalDaily(
                    signal_type=self._signal_type,
                    target_ticker=jp_ticker,
                    us_signal_date=us_signal_date,
                    jp_execution_date=jp_execution_date,
                    signal_score=Decimal(str(score)) if score is not None else None,
                    signal_rank=rank,
                    suggested_side=side,
                    input_metadata_json=metadata,
                )
            )

        return rows

    def _compute_us_returns(self, us_signal_date: date) -> dict[str, float | None]:
        """US 11 業種の 1 日リターンを計算して返す。

        # 先読み防止の核心
          get_prices_up_to の as_of_date には us_signal_date のみを渡す。
          これにより WHERE business_date <= us_signal_date が SQL レベルで強制される。
          jp_execution_date を as_of_date として渡すことは絶対にしない。

        # リターン計算
          prices[-1] = us_signal_date の行 (最新)
          prices[-2] = 前 NYSE 営業日の行
          return = (adj_close[-1] - adj_close[-2]) / adj_close[-2]

        # 欠損ルール
          2 行未満 / adj_close が None / prev_close が 0 → None を返す

        Args:
            us_signal_date: リターン計算の基準日。この日以前のデータのみ参照する。

        Returns:
            {us_ticker: float リターン | None}
        """
        us_returns: dict[str, float | None] = {}

        for us_ticker in sorted(_ALL_US_TICKERS):  # 順序安定化
            # as_of_date=us_signal_date: この日以前の最新 2 行のみ取得 (先読み防止)
            prices = self._price_repo.get_prices_up_to(
                us_ticker,
                as_of_date=us_signal_date,  # ← 先読み防止: jp_execution_date は絶対に渡さない
                limit=2,
            )

            if len(prices) < 2:
                us_returns[us_ticker] = None
                logger.debug(
                    "US 価格不足: ticker=%s us_signal_date=%s rows=%d",
                    us_ticker, us_signal_date, len(prices),
                )
                continue

            # prices は business_date 昇順 (get_prices_up_to の仕様)
            prev_close = prices[-2].adjusted_close_price
            curr_close = prices[-1].adjusted_close_price

            if prev_close is None or curr_close is None or prev_close == 0:
                us_returns[us_ticker] = None
                continue

            us_returns[us_ticker] = float(curr_close - prev_close) / float(prev_close)

        return us_returns


# ---------------------------------------------------------------------------
# モジュールレベル関数 (純粋関数 / テストで直接呼び出し可能)
# ---------------------------------------------------------------------------

def _score_jp_sectors(
    us_returns: dict[str, float | None],
) -> dict[str, float | None]:
    """US リターンから JP 17 業種のシグナルスコアを計算する。

    各 JP 業種のスコア = 対応 US 銘柄のうち有効なリターンの等ウェイト平均。
    対応 US 銘柄が全欠損の場合は None。
    一部欠損の場合は有効なリターンだけで平均を計算する。

    Args:
        us_returns: {us_ticker: float | None}

    Returns:
        {jp_ticker: float スコア | None}
    """
    scores: dict[str, float | None] = {}
    for jp_ticker, us_tickers in JP_TICKER_TO_US_TICKERS.items():
        valid = [
            us_returns[t]
            for t in us_tickers
            if t in us_returns and us_returns[t] is not None
        ]
        scores[jp_ticker] = sum(valid) / len(valid) if valid else None
    return scores


def _rank_and_side(
    scores: dict[str, float | None],
) -> dict[str, tuple[int | None, str]]:
    """スコアから signal_rank と suggested_side を決定する。

    # ランク付け規則
      - 有効スコアを持つ業種のみランク付けする (1 = 最高スコア)
      - 同スコアのタイブレーク: jp_ticker 昇順 (アルファベット順) で安定ソート
      - スコアが None の業種: rank=None, side="neutral"

    # side 判定
      - rank 1〜_LONG_RANK_CUTOFF (3)       → "long"
      - 下位 _SHORT_RANK_CUTOFF (3) 以内    → "short"
      - それ以外                             → "neutral"
      long の判定が short より優先する (n が少ない場合の衝突を防ぐ)

    Args:
        scores: {jp_ticker: float | None}

    Returns:
        {jp_ticker: (rank | None, suggested_side)}
    """
    valid = {t: s for t, s in scores.items() if s is not None}

    # スコア降順、同スコア時は ticker 昇順 (安定タイブレーク)
    ranked = sorted(valid, key=lambda t: (-valid[t], t))
    n = len(ranked)

    result: dict[str, tuple[int | None, str]] = {}

    for rank_0, ticker in enumerate(ranked):
        rank = rank_0 + 1
        if rank <= _LONG_RANK_CUTOFF:
            side = SuggestedSide.LONG.value
        elif n - rank < _SHORT_RANK_CUTOFF:  # 下位 3 以内: n-rank が 0,1,2
            side = SuggestedSide.SHORT.value
        else:
            side = SuggestedSide.NEUTRAL.value
        result[ticker] = (rank, side)

    # スコアが None の業種
    for ticker in scores:
        if ticker not in result:
            result[ticker] = (None, SuggestedSide.NEUTRAL.value)

    return result
