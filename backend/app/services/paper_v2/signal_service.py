"""
PaperV2SignalService: subspace-regularized PCA による paper_v2 シグナル生成.

参照: 論文 §3.1-3.3, v2.2-lite + addendum-1

# パイプライン (1 jp_execution_date あたり)
  1. OOS 境界チェック: jp_execution_date < PAPER_V2_OOS_START なら skip
  2. 訓練窓構築: jp_execution_date - 1 日を末尾とする L=60 営業日の US+JP 28 CC returns
     完全観測行 (complete-case)
     - 欠損行は complete-case で除外
     - 有効行 < 60 なら SkipReason.INSUFFICIENT_WINDOW
     - US 欠損主因 → MISSING_US_PRICES, JP 欠損主因 → MISSING_JP_PRICES
  3. 標準化: μ, σ (ddof=1) を列ごとに計算、z_window = (rcc_window - μ) / σ_safe
  4. reg-PCA: fit_reg_pca(z_window, c_0, λ=0.9, K=3, n_us=11)
  5. 現在 US リターン取得 (as_of_date=us_signal_date): rcc_U_current
     - 当日 (us_signal_date) 価格の厳密確認: prices[-1].business_date == us_signal_date
     - 取得失敗 → SkipReason.MISSING_US_PRICES
  6. z_U_current = (rcc_U_current - μ[:11]) / σ_safe[:11]
  7. シグナル: hat_z_J = V_J @ (V_U.T @ z_U_current)  ∈ R^17
  8. ランク: 上位 5 long, 下位 5 short, 残 7 neutral
  9. SignalDaily rows (signal_score = hat_z_J の標準化空間値)

# 先読み防止 (paper_v1 と同等水準)
  - 訓練窓の末尾: training_end = jp_execution_date - 1 日 (paper_v1 と同じ契約)
    alignment は [buffer_start, training_end] で構築; 各行の us_date < jp_date ≤ training_end
  - 現在 US リターン: as_of_date=us_signal_date のみ; 且つ当日価格の厳密確認を実施
  - jp_execution_date を as_of_date として渡す経路が存在しないこと
  - CalendarService が us_signal_date < jp_execution_date を構造的保証

# JP 価格の strict adjusted-close-only ポリシー
  - JP の adjusted_close_price=None の場合は fallback (close_price) なし
  - C_0 builder / current US との契約を統一 (PdM 判断)
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Final

import numpy as np
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
from app.services.signal_service import SignalGenerationResult
from app.services.paper_v2.artifact_loader import LoadedC0Artifact, load_c0_artifact
from app.services.paper_v2.constants import (
    C0_VERSION,
    C_FULL_TRAIN_END,
    C_FULL_TRAIN_START,
    K,
    LAMBDA,
    N_JP,
    N_LONG,
    N_SHORT,
    N_US,
    PAPER_V2_OOS_START,
    SIGNAL_TYPE_PAPER_V2,
    UNIVERSE_SIZE,
    WINDOW_SIZE,
    _EPSILON,
    _FETCH_BUFFER_DAYS,
)
from app.services.paper_v2.reg_pca import fit_reg_pca
from app.services.paper_v2.skip_reasons import SkipReason

logger = logging.getLogger(__name__)


# canonical ticker tuples (paper_v1 と同じ導出ロジック)
_ALL_US_TICKERS: Final[tuple[str, ...]] = tuple(
    sorted({t for lst in JP_TICKER_TO_US_TICKERS.values() for t in lst})
)
_ALL_JP_TICKERS: Final[tuple[str, ...]] = tuple(ALL_JP_TICKERS)


@dataclass
class PaperV2GenerationResult(SignalGenerationResult):
    """
    paper_v2 専用の拡張返却型。
    親の fields (requested, succeeded, failed, skipped, saved_rows) に加えて
    skip_reasons を保持する (addendum-1 §5-B)。
    """

    skip_reasons: dict[date, str] = field(default_factory=dict)


class PaperV2SignalService:
    """subspace-regularized PCA による paper_v2 シグナルを生成・保存するサービス."""

    def __init__(
        self,
        session: Session,
        calendar_service: CalendarService,
        price_repository: PriceRepository | None = None,
        signal_repository: SignalRepository | None = None,
        artifact: LoadedC0Artifact | None = None,
    ) -> None:
        self._session = session
        self._calendar = calendar_service
        self._price_repo = price_repository or PriceRepository(session)
        self._signal_repo = signal_repository or SignalRepository(session)

        # artifact 起動時ロード (fail-fast)
        self._artifact = artifact if artifact is not None else load_c0_artifact(
            expected_us_tickers=_ALL_US_TICKERS,
            expected_jp_tickers=_ALL_JP_TICKERS,
        )

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------

    def generate_signals_for_range(
        self,
        start: date,
        end: date,
    ) -> PaperV2GenerationResult:
        """[start, end] 内の全 jp_execution_date に対してシグナルを生成・保存する。

        Args:
            start: jp_execution_date の開始日 (inclusive)
            end:   jp_execution_date の終了日 (inclusive)

        Returns:
            PaperV2GenerationResult サマリー。
        """
        alignment = self._calendar.build_date_alignment(start, end)
        result = PaperV2GenerationResult(requested=len(alignment))

        if alignment.empty:
            logger.info(
                "paper_v2 generate_signals_for_range: 対象日なし (start=%s end=%s)",
                start,
                end,
            )
            return result

        for _, row in alignment.iterrows():
            jp_date: date = row[COL_JP_EXECUTION_DATE]
            us_date: date = row[COL_US_SIGNAL_DATE]
            self._process_date(jp_date, us_date, result)

        logger.info(
            "paper_v2 generate_signals_for_range 完了: requested=%d succeeded=%d "
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
        result: PaperV2GenerationResult,
    ) -> None:
        """1 jp_execution_date の処理を行い result を更新する (fail-soft)。"""
        try:
            rows, skip_reason = self._generate_for_date(jp_execution_date, us_signal_date)

            if not rows:
                result.skipped.append(jp_execution_date)
                if skip_reason is not None:
                    result.skip_reasons[jp_execution_date] = skip_reason
                logger.info(
                    "paper_v2 スキップ: jp_date=%s us_date=%s reason=%s",
                    jp_execution_date,
                    us_signal_date,
                    skip_reason,
                )
                return

            self._signal_repo.upsert_many(rows)
            self._session.commit()
            result.succeeded.append(jp_execution_date)
            result.saved_rows += len(rows)
            logger.debug(
                "paper_v2 保存完了: jp_date=%s rows=%d", jp_execution_date, len(rows)
            )

        except Exception as exc:
            self._session.rollback()
            result.failed[jp_execution_date] = str(exc)
            logger.warning(
                "paper_v2 生成失敗: jp_date=%s: %s",
                jp_execution_date,
                exc,
                exc_info=True,
            )

    def _generate_for_date(
        self,
        jp_date: date,
        us_date: date,
    ) -> tuple[list[SignalDaily], str | None]:
        """
        Returns:
            (rows, skip_reason)
            rows が非空 → 成功
            rows が空 + skip_reason=非None → skip
        """
        # ── OOS 境界チェック ──
        if jp_date < PAPER_V2_OOS_START:
            return [], SkipReason.BEFORE_OOS_START

        # ── 訓練窓構築 ──
        rcc_window, window_skip_reason = self._build_window(us_date, jp_date)
        if window_skip_reason is not None:
            return [], window_skip_reason
        if rcc_window is None or len(rcc_window) < WINDOW_SIZE:
            return [], SkipReason.INSUFFICIENT_WINDOW

        # ── 標準化 ──
        mu = rcc_window.mean(axis=0)
        sigma = rcc_window.std(axis=0, ddof=1)
        sigma_safe = np.where(sigma < _EPSILON, _EPSILON, sigma)
        z_window = (rcc_window - mu) / sigma_safe

        # ── reg-PCA ──
        pca_result = fit_reg_pca(z_window, self._artifact.c0, LAMBDA, K, N_US)

        # ── 現在 US リターン取得 (先読み防止: as_of_date=us_signal_date のみ) ──
        rcc_u_current = self._fetch_current_us_returns(us_date)
        if rcc_u_current is None:
            return [], SkipReason.MISSING_US_PRICES

        # ── z スコア変換 ──
        z_u = (rcc_u_current - mu[:N_US]) / sigma_safe[:N_US]

        # ── シグナル計算 ──
        f_t = pca_result.V_U.T @ z_u          # (K,)
        hat_z_j = pca_result.V_J @ f_t         # (N_JP,)

        # ── ランク・サイド判定 (paper_v2 独立実装) ──
        rank_side = self._rank_and_side(hat_z_j)

        # ── SignalDaily rows 生成 ──
        rows = [
            SignalDaily(
                signal_type=SIGNAL_TYPE_PAPER_V2,
                target_ticker=jp_ticker,
                us_signal_date=us_date,
                jp_execution_date=jp_date,
                signal_score=Decimal(str(float(hat_z_j[i]))),
                signal_rank=rank_side[jp_ticker][0],
                suggested_side=rank_side[jp_ticker][1],
                input_metadata_json={
                    "c0_version": C0_VERSION,
                    "K": K,
                    "lambda": LAMBDA,
                    "n_train": int(rcc_window.shape[0]),
                    "top_k_eigenvalues": [
                        float(v) for v in pca_result.top_k_eigenvalues
                    ],
                    "condition_number": float(pca_result.condition_number),
                    "c_full_train_start": C_FULL_TRAIN_START.isoformat(),
                    "c_full_train_end": C_FULL_TRAIN_END.isoformat(),
                    "universe_size": UNIVERSE_SIZE,
                    "oos_start": PAPER_V2_OOS_START.isoformat(),
                },
            )
            for i, jp_ticker in enumerate(_ALL_JP_TICKERS)
        ]
        return rows, None

    def _build_window(
        self,
        us_signal_date: date,
        jp_execution_date: date,
    ) -> tuple[np.ndarray | None, str | None]:
        """
        paper_v2 complete-case window (28 tickers 全揃い).
        列順: _ALL_US_TICKERS (11) + _ALL_JP_TICKERS (17) = 28

        先読み防止 (paper_v1 と同じ契約):
          - training_end = jp_execution_date - 1 日
          - alignment は [buffer_start, training_end] で構築
          - 各行の us_date < jp_date ≤ training_end < jp_execution_date
          - jp_execution_date 当日は一切含めない

        JP 価格ポリシー: strict adjusted-close-only (fallback なし)

        Returns:
            (rcc_matrix | None, skip_reason | None)
            - skip_reason 非 None の場合: MISSING_US_PRICES or MISSING_JP_PRICES で即 skip
            - skip_reason None + rcc_matrix None or 行数不足: 呼び出し側で INSUFFICIENT_WINDOW
        """
        # paper_v1 と同じ契約: training_end = jp_execution_date - 1 日
        training_end = jp_execution_date - timedelta(days=1)
        buffer_start = jp_execution_date - timedelta(days=_FETCH_BUFFER_DAYS)

        # alignment: [buffer_start, training_end] の (us_date, jp_date) ペア
        alignment = self._calendar.build_date_alignment(buffer_start, training_end)
        if alignment.empty:
            return None, None  # 呼び出し側で INSUFFICIENT_WINDOW

        # ------------------------------------------------------------------
        # バッチ取得: US 価格 (CC リターン用に前営業日分も含める)
        # ------------------------------------------------------------------
        us_fetch_start = buffer_start - timedelta(days=10)

        us_price_map: dict[str, dict[date, float | None]] = {}
        us_sorted_dates: dict[str, list[date]] = {}

        for us_ticker in _ALL_US_TICKERS:
            prices = self._price_repo.get_prices_between(
                us_ticker, us_fetch_start, training_end
            )
            date_adj: dict[date, float | None] = {
                p.business_date: (
                    float(p.adjusted_close_price)
                    if p.adjusted_close_price is not None
                    else None
                )
                for p in prices
            }
            us_price_map[us_ticker] = date_adj
            us_sorted_dates[us_ticker] = sorted(date_adj.keys())

        # ------------------------------------------------------------------
        # バッチ取得: JP 価格 (strict adjusted-close-only: fallback なし)
        # ------------------------------------------------------------------
        jp_price_map: dict[str, dict[date, float | None]] = {}
        jp_sorted_dates: dict[str, list[date]] = {}

        jp_fetch_start = buffer_start - timedelta(days=10)

        for jp_ticker in _ALL_JP_TICKERS:
            prices = self._price_repo.list_by_ticker(
                jp_ticker, start=jp_fetch_start, end=training_end
            )
            # strict: adjusted_close_price のみ使用 (close_price fallback なし)
            date_adj_jp: dict[date, float | None] = {
                p.business_date: (
                    float(p.adjusted_close_price)
                    if p.adjusted_close_price is not None
                    else None
                )
                for p in prices
            }
            jp_price_map[jp_ticker] = date_adj_jp
            jp_sorted_dates[jp_ticker] = sorted(date_adj_jp.keys())

        # ------------------------------------------------------------------
        # 訓練行の構築 (complete-case: US 11 + JP 17 全て揃っている行のみ)
        # 欠損理由をカウントして skip_reason を判定する
        # ------------------------------------------------------------------
        rows: list[list[float]] = []
        us_missing_count = 0
        jp_missing_count = 0

        for _, alignment_row in alignment.iterrows():
            us_d: date = alignment_row[COL_US_SIGNAL_DATE]
            jp_d: date = alignment_row[COL_JP_EXECUTION_DATE]

            row_vals: list[float] = []
            skip_us = False
            skip_jp = False

            # ── US CC リターン (11 銘柄) ──
            for us_ticker in _ALL_US_TICKERS:
                dates = us_sorted_dates.get(us_ticker, [])
                adj_map = us_price_map.get(us_ticker, {})

                idx = bisect.bisect_right(dates, us_d) - 1
                if idx < 1 or dates[idx] != us_d:
                    skip_us = True
                    break

                curr = adj_map[us_d]
                prev = adj_map[dates[idx - 1]]
                if curr is None or prev is None or prev == 0.0:
                    skip_us = True
                    break

                row_vals.append((curr - prev) / prev)

            if skip_us:
                us_missing_count += 1
                continue

            # ── JP CC リターン (17 銘柄) ──
            for jp_ticker in _ALL_JP_TICKERS:
                dates_jp = jp_sorted_dates.get(jp_ticker, [])
                adj_map_jp = jp_price_map.get(jp_ticker, {})

                idx_jp = bisect.bisect_right(dates_jp, jp_d) - 1
                if idx_jp < 1 or dates_jp[idx_jp] != jp_d:
                    skip_jp = True
                    break

                curr_jp = adj_map_jp[jp_d]
                prev_jp = adj_map_jp[dates_jp[idx_jp - 1]]
                if curr_jp is None or prev_jp is None or prev_jp == 0.0:
                    skip_jp = True
                    break

                row_vals.append((curr_jp - prev_jp) / prev_jp)

            if skip_jp:
                jp_missing_count += 1
                continue

            rows.append(row_vals)

        if len(rows) < WINDOW_SIZE:
            # 有効行が不足: 主因を skip_reason として返す
            if us_missing_count >= jp_missing_count and us_missing_count > 0:
                return None, SkipReason.MISSING_US_PRICES
            elif jp_missing_count > 0:
                return None, SkipReason.MISSING_JP_PRICES
            else:
                return None, None  # 呼び出し側で INSUFFICIENT_WINDOW

        return np.array(rows, dtype=float), None

    def _fetch_current_us_returns(self, us_signal_date: date) -> np.ndarray | None:
        """
        現在の US リターンベクトルを取得する。

        先読み防止の核心:
          get_prices_up_to の as_of_date には us_signal_date のみを渡す。
          jp_execution_date を as_of_date として渡すことは絶対にしない。

        strict 日付チェック (P1):
          prices[-1].business_date が us_signal_date と厳密一致しない場合は None を返す。
          当日 US 価格欠損時に前日以前のデータで「現在リターン」を誤計算しないよう防止する。

        Returns:
            US CC リターンベクトル (N_US,) or None (データ不足・当日価格欠損時)
        """
        x_vals: list[float] = []

        for us_ticker in _ALL_US_TICKERS:
            prices = self._price_repo.get_prices_up_to(
                us_ticker,
                as_of_date=us_signal_date,  # ← 先読み防止: jp_execution_date は絶対に渡さない
                limit=2,
            )

            if len(prices) < 2:
                return None

            # P1: 当日 (us_signal_date) の価格であることを厳密確認
            # 欠損時は前日以前の 2 日分で誤計算しないよう None を返す
            if prices[-1].business_date != us_signal_date:
                logger.debug(
                    "paper_v2: current US return skipped: ticker=%s, "
                    "latest_business_date=%s, us_signal_date=%s",
                    us_ticker,
                    prices[-1].business_date,
                    us_signal_date,
                )
                return None

            prev_close = prices[-2].adjusted_close_price
            curr_close = prices[-1].adjusted_close_price

            if prev_close is None or curr_close is None or float(prev_close) == 0.0:
                return None

            x_vals.append((float(curr_close) - float(prev_close)) / float(prev_close))

        return np.array(x_vals, dtype=float)

    def _rank_and_side(
        self,
        signal: np.ndarray,
    ) -> dict[str, tuple[int, str]]:
        """
        paper_v2 独立実装の rank/side 付与。
        paper_v1 の _rank_and_side_paper_v1 を import せず、ここに再実装。

        仕様:
          - スコア降順でランク付け (1 = 最高スコア = 最強 long)
          - 同スコア時は ticker 昇順で安定タイブレーク
          - 上位 N_LONG=5 → long, 下位 N_SHORT=5 → short, 中間 7 → neutral
        """
        n = len(_ALL_JP_TICKERS)

        ranked_indices = sorted(
            range(n),
            key=lambda i: (-signal[i], _ALL_JP_TICKERS[i]),
        )

        result: dict[str, tuple[int, str]] = {}

        for rank_0, idx in enumerate(ranked_indices):
            rank = rank_0 + 1
            if rank <= N_LONG:
                side = SuggestedSide.LONG.value
            elif (n - rank_0) <= N_SHORT:
                side = SuggestedSide.SHORT.value
            else:
                side = SuggestedSide.NEUTRAL.value
            result[_ALL_JP_TICKERS[idx]] = (rank, side)

        return result
