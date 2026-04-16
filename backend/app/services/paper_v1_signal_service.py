"""
PaperV1SignalService: PCA + Ridge 回帰による JP 業種シグナル生成。

# 設計方針
  - signal_daily テーブルに signal_type="paper_v1" で保存する。
  - CalendarService.build_date_alignment() をループ軸とし、
    (jp_execution_date, us_signal_date) を決定する。
  - 訓練ウィンドウ: jp_execution_date の _FETCH_BUFFER_DAYS 日前〜jp_execution_date-1 日。
  - 有効行数が WINDOW_SIZE 未満の場合はその日をスキップする (fail-soft)。

# 先読み防止
  - 訓練データの末尾: jp_execution_date - 1 日 (jp_execution_date 自体を訓練に使わない)。
  - 現在 US リターン取得: get_prices_up_to(as_of_date=us_signal_date)。
    jp_execution_date を as_of_date として渡すことは絶対にしない。
  - CalendarService が jp_execution_date > us_signal_date を構造的に保証する。

# パイプライン (1 jp_execution_date 当たり)
  1. 訓練データ構築 (バッチ取得)
     X (W, p): 全 US ETF の close-to-close リターン
     Y (W, 17): 全 JP 業種の OC リターン
     完全観測行のみ使用 (complete-case)
  2. 標準化 → PCA (累積寄与率 >= 80%, 最大 5 主成分) → Ridge(alpha=1.0, fit_intercept=False)
  3. 現在 US リターン取得 (as_of_date=us_signal_date)
  4. スコア計算 (逆標準化あり)
  5. 上位 5: long / 下位 5: short / 残: neutral

# モジュールレベル関数
  _standardize / _select_n_components / _fit_model / _compute_scores /
  _rank_and_side_paper_v1 はテスト・再利用のためモジュールスコープで公開する。
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Final

import numpy as np
from sklearn.decomposition import PCA
from sklearn.linear_model import Ridge
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# モジュール定数
# ---------------------------------------------------------------------------

SIGNAL_TYPE_PAPER_V1: Final[str] = "paper_v1"

# 訓練ウィンドウの最小有効行数
WINDOW_SIZE: Final[int] = 60

# PCA パラメータ
PCA_VARIANCE_THRESHOLD: Final[float] = 0.80
PCA_MAX_COMPONENTS: Final[int] = 5

# Ridge 正則化係数
RIDGE_ALPHA: Final[float] = 1.0

# ランク判定
N_LONG: Final[int] = 5
N_SHORT: Final[int] = 5

# 訓練データ取得バッファ (カレンダー日数)
# ~120 日 ≈ 85 JP 営業日。WINDOW_SIZE=60 に対して十分なバッファ。
_FETCH_BUFFER_DAYS: Final[int] = 120

# 標準化の ゼロ除算防止用下限値
_EPSILON: Final[float] = 1e-8

# 全 US ティッカー (アルファベット昇順固定 → X の列順序を安定化)
_ALL_US_TICKERS: Final[tuple[str, ...]] = tuple(
    sorted(
        frozenset(
            ticker
            for us_list in JP_TICKER_TO_US_TICKERS.values()
            for ticker in us_list
        )
    )
)

# 全 JP ティッカー (sector_mapping 定義順 = 1617.T〜1633.T)
_ALL_JP_TICKERS: Final[tuple[str, ...]] = tuple(ALL_JP_TICKERS)


# ---------------------------------------------------------------------------
# 訓練済みモデルコンテナ
# ---------------------------------------------------------------------------


@dataclass
class PaperV1Model:
    """訓練済み PCA + Ridge モデルのパラメータ。

    Attributes:
        mu_x:        X の訓練時平均 (p,)
        sigma_x:     X の訓練時標準偏差 (p,)
        mu_y:        Y の訓練時平均 (17,)
        sigma_y:     Y の訓練時標準偏差 (17,)
        V_k:         PCA の上位 K 主成分 (p, K) — components_[:K].T
        ridge_coef:  Ridge の係数行列 (K, 17) — coef_.T
        n_components: 選択された主成分数 K
    """

    mu_x: np.ndarray
    sigma_x: np.ndarray
    mu_y: np.ndarray
    sigma_y: np.ndarray
    V_k: np.ndarray
    ridge_coef: np.ndarray
    n_components: int


# ---------------------------------------------------------------------------
# モジュールレベル関数 (純粋関数 / テストで直接呼び出し可能)
# ---------------------------------------------------------------------------


def _standardize(
    X: np.ndarray,
    *,
    mu: np.ndarray | None = None,
    sigma: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """行列 X を列ごとに標準化する。

    Args:
        X:     入力行列 (n, p) または (p,)
        mu:    既存の平均。None の場合は X の列平均を使用。
        sigma: 既存の標準偏差。None の場合は X の列標準偏差 (ddof=1) を使用。

    Returns:
        (X_std, mu, sigma) のタプル。
        mu と sigma は次回の標準化 (テスト点など) に再利用できる。

    Note:
        sigma の各要素が _EPSILON 未満の場合は _EPSILON で置換する (ゼロ除算防止)。
    """
    if mu is None:
        mu = X.mean(axis=0)
    if sigma is None:
        sigma = X.std(axis=0, ddof=1)

    sigma_safe = np.where(sigma < _EPSILON, _EPSILON, sigma)
    X_std = (X - mu) / sigma_safe
    return X_std, mu, sigma


def _select_n_components(
    explained_variance_ratio: np.ndarray,
    threshold: float = PCA_VARIANCE_THRESHOLD,
    max_k: int = PCA_MAX_COMPONENTS,
) -> int:
    """累積寄与率が threshold 以上になる最小コンポーネント数 K を返す。

    Args:
        explained_variance_ratio: PCA の explained_variance_ratio_ (各 PC の寄与率)
        threshold: 目標累積寄与率 (デフォルト 0.80)
        max_k:     最大コンポーネント数 (デフォルト 5)

    Returns:
        1 以上 max_k 以下の整数。
        累積寄与率が threshold に達しない場合でも max_k を上限とする。
    """
    cumvar = np.cumsum(explained_variance_ratio)
    # searchsorted は 0-indexed: cumvar[k-1] >= threshold を満たす最小 k
    k = int(np.searchsorted(cumvar, threshold)) + 1
    return min(k, max_k)


def _fit_model(
    X: np.ndarray,
    Y: np.ndarray,
    variance_threshold: float = PCA_VARIANCE_THRESHOLD,
    max_components: int = PCA_MAX_COMPONENTS,
    ridge_alpha: float = RIDGE_ALPHA,
) -> PaperV1Model:
    """訓練データから PCA + Ridge モデルを構築する。

    # パイプライン
      1. X を標準化 → X_std (W, p)
      2. Y を標準化 → Y_std (W, 17)
      3. PCA(n_components=min(max_k, p, W-1)).fit(X_std)
      4. _select_n_components で K を決定
      5. V_k = components_[:K].T  # (p, K)
      6. Z = X_std @ V_k           # (W, K) PC スコア
      7. Ridge(alpha, fit_intercept=False).fit(Z, Y_std)
      8. ridge_coef = coef_.T     # (K, 17)

    Args:
        X:                 訓練 US リターン行列 (W, p)
        Y:                 訓練 JP OC リターン行列 (W, 17)
        variance_threshold: PCA 累積寄与率の目標値
        max_components:    PCA の最大主成分数
        ridge_alpha:       Ridge の正則化係数

    Returns:
        訓練済み PaperV1Model
    """
    W, p = X.shape

    # 標準化
    X_std, mu_x, sigma_x = _standardize(X)
    Y_std, mu_y, sigma_y = _standardize(Y)

    # PCA: コンポーネント数はサンプル数・特徴数に合わせて上限クリップ
    n_pca = min(max_components, p, W - 1)
    pca = PCA(n_components=n_pca)
    pca.fit(X_std)

    # K 選択
    k = _select_n_components(pca.explained_variance_ratio_, variance_threshold, max_components)
    V_k = pca.components_[:k].T  # (p, k)

    # PC スコア
    Z = X_std @ V_k  # (W, k)

    # Ridge 回帰 (17 業種を一括)
    ridge = Ridge(alpha=ridge_alpha, fit_intercept=False)
    ridge.fit(Z, Y_std)
    # ridge.coef_ shape: (17, k) → 転置して (k, 17)
    ridge_coef = ridge.coef_.T  # (k, 17)

    return PaperV1Model(
        mu_x=mu_x,
        sigma_x=sigma_x,
        mu_y=mu_y,
        sigma_y=sigma_y,
        V_k=V_k,
        ridge_coef=ridge_coef,
        n_components=k,
    )


def _compute_scores(
    model: PaperV1Model,
    x_new: np.ndarray,
) -> np.ndarray:
    """訓練済みモデルで x_new から JP 業種スコア (予測 OC リターン) を計算する。

    Args:
        model: 訓練済み PaperV1Model
        x_new: 入力 US リターンベクトル (p,)

    Returns:
        JP 業種スコアベクトル (17,) — 逆標準化済みの予測 JP OC リターン
    """
    # 訓練時と同じ mu_x, sigma_x で標準化
    x_std, _, _ = _standardize(x_new.reshape(1, -1), mu=model.mu_x, sigma=model.sigma_x)
    # PC スコア
    z = x_std @ model.V_k  # (1, k)
    # Ridge 予測 (標準化済み空間)
    y_std = z @ model.ridge_coef  # (1, 17)
    # 逆標準化
    y = y_std * model.sigma_y + model.mu_y  # (1, 17)
    return y[0]  # (17,)


def _rank_and_side_paper_v1(
    scores: np.ndarray,
    tickers: tuple[str, ...],
    n_long: int = N_LONG,
    n_short: int = N_SHORT,
) -> dict[str, tuple[int, str]]:
    """スコア配列から signal_rank と suggested_side を決定する。

    # ランク付け規則
      - スコア降順でランク付け (1 = 最高スコア = 最強 long)
      - 同スコアのタイブレーク: ticker 昇順 (アルファベット順)

    # side 判定
      - rank 1〜n_long         → "long"
      - 下位 n_short 以内      → "short"
      - それ以外               → "neutral"
      long の判定が short より優先する。

    Args:
        scores:  JP 業種スコア配列 (17,)。tickers と同じ順序。
        tickers: JP ティッカータプル (scores と対応)
        n_long:  long にする上位業種数
        n_short: short にする下位業種数

    Returns:
        {jp_ticker: (rank, suggested_side)}
    """
    n = len(tickers)

    # スコア降順、同スコア時は ticker 昇順 (安定タイブレーク)
    ranked_indices = sorted(
        range(n),
        key=lambda i: (-scores[i], tickers[i]),
    )

    result: dict[str, tuple[int, str]] = {}

    for rank_0, idx in enumerate(ranked_indices):
        rank = rank_0 + 1
        if rank <= n_long:
            side = SuggestedSide.LONG.value
        elif (n - rank_0) <= n_short:
            # 0-indexed で n - 1 - rank_0 < n_short → 下位 n_short 以内
            side = SuggestedSide.SHORT.value
        else:
            side = SuggestedSide.NEUTRAL.value
        result[tickers[idx]] = (rank, side)

    return result


# ---------------------------------------------------------------------------
# PaperV1SignalService
# ---------------------------------------------------------------------------


class PaperV1SignalService:
    """PCA + Ridge 回帰による JP 業種シグナルを生成・保存するサービス。

    Args:
        session:           SQLAlchemy Session。commit は jp_date 単位で行う。
        calendar_service:  CalendarService のインスタンス。
        price_repository:  PriceRepository。None の場合は session から自動生成。
        signal_repository: SignalRepository。None の場合は session から自動生成。
    """

    def __init__(
        self,
        session: Session,
        calendar_service: CalendarService,
        price_repository: PriceRepository | None = None,
        signal_repository: SignalRepository | None = None,
    ) -> None:
        self._session = session
        self._calendar = calendar_service
        self._price_repo = price_repository or PriceRepository(session)
        self._signal_repo = signal_repository or SignalRepository(session)

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------

    def generate_signals_for_range(
        self,
        start: date,
        end: date,
    ) -> SignalGenerationResult:
        """[start, end] 内の全 jp_execution_date に対してシグナルを生成・保存する。

        jp_execution_date 単位で fail-soft commit する。
        訓練データが WINDOW_SIZE 未満の日付はスキップする。

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
                    "paper_v1 スキップ: jp_date=%s us_date=%s",
                    jp_execution_date,
                    us_signal_date,
                )
                return

            self._signal_repo.upsert_many(rows)
            self._session.commit()
            result.succeeded.append(jp_execution_date)
            result.saved_rows += len(rows)
            logger.debug("paper_v1 保存完了: jp_date=%s rows=%d", jp_execution_date, len(rows))

        except Exception as exc:
            self._session.rollback()
            result.failed[jp_execution_date] = str(exc)
            logger.warning(
                "paper_v1 生成失敗: jp_date=%s: %s",
                jp_execution_date,
                exc,
                exc_info=True,
            )

    def _generate_for_date(
        self,
        jp_execution_date: date,
        us_signal_date: date,
    ) -> list[SignalDaily]:
        """1 つの (jp_execution_date, us_signal_date) に対して 17 業種の SignalDaily を生成する。

        # 先読み防止
          _build_training_data は jp_execution_date - 1 日を末尾とする。
          _fetch_current_us_returns は as_of_date=us_signal_date を使う。
          jp_execution_date は価格取得の基準日として一切使わない。

        Returns:
            SignalDaily のリスト。スキップ条件を満たす場合は空リスト。
        """
        # 1. 訓練データ構築
        training = self._build_training_data(jp_execution_date)
        if training is None:
            logger.debug(
                "paper_v1: 訓練データ不足でスキップ jp_date=%s", jp_execution_date
            )
            return []

        X_train, Y_train = training

        # 2. モデル訓練
        model = _fit_model(X_train, Y_train)

        # 3. 現在 US リターン取得 (先読み防止: as_of_date=us_signal_date)
        x_current = self._fetch_current_us_returns(us_signal_date)
        if x_current is None:
            logger.debug(
                "paper_v1: 現在 US リターン不足でスキップ us_date=%s", us_signal_date
            )
            return []

        # 4. スコア計算 (逆標準化済み JP OC リターン予測値)
        scores = _compute_scores(model, x_current)

        # 5. ランク・サイド判定
        rank_side = _rank_and_side_paper_v1(scores, _ALL_JP_TICKERS)

        # 6. SignalDaily オブジェクト生成
        rows: list[SignalDaily] = []
        for i, jp_ticker in enumerate(_ALL_JP_TICKERS):
            rank, side = rank_side[jp_ticker]
            rows.append(
                SignalDaily(
                    signal_type=SIGNAL_TYPE_PAPER_V1,
                    target_ticker=jp_ticker,
                    us_signal_date=us_signal_date,
                    jp_execution_date=jp_execution_date,
                    signal_score=Decimal(str(float(scores[i]))),
                    signal_rank=rank,
                    suggested_side=side,
                    input_metadata_json={
                        "n_train": len(X_train),
                        "n_components": model.n_components,
                    },
                )
            )

        return rows

    def _build_training_data(
        self,
        jp_execution_date: date,
    ) -> tuple[np.ndarray, np.ndarray] | None:
        """訓練データ行列 (X, Y) を構築する。

        # 先読み防止
          訓練末尾 = jp_execution_date - 1 日。
          jp_execution_date 当日の価格は訓練データに含めない。

        # バッチ取得
          N×M 個別クエリを避けるため、全ティッカーを一括取得してから Python で集計する。

        # 完全観測行方針 (complete-case)
          X または Y に欠損のある行は除外する。

        Args:
            jp_execution_date: シグナル生成対象日。訓練はこの前日まで。

        Returns:
            (X, Y) — shape: (n_valid_rows, n_us_tickers), (n_valid_rows, 17)
            有効行数が WINDOW_SIZE 未満の場合は None。
        """
        training_end = jp_execution_date - timedelta(days=1)
        buffer_start = jp_execution_date - timedelta(days=_FETCH_BUFFER_DAYS)

        # alignment: [buffer_start, training_end] の (us_signal_date, jp_execution_date) ペア
        alignment = self._calendar.build_date_alignment(buffer_start, training_end)
        if alignment.empty:
            return None

        # ------------------------------------------------------------------
        # バッチ取得: US 価格
        # CC リターン計算には us_signal_date の前営業日の価格も必要なため
        # 開始日を少し余分に早める。
        # ------------------------------------------------------------------
        us_fetch_start = buffer_start - timedelta(days=10)

        # {us_ticker: {business_date: adjusted_close_price_float | None}}
        us_price_map: dict[str, dict[date, float | None]] = {}
        # {us_ticker: sorted list of dates} (前営業日探索用)
        us_sorted_dates: dict[str, list[date]] = {}

        for us_ticker in _ALL_US_TICKERS:
            prices = self._price_repo.get_prices_between(us_ticker, us_fetch_start, training_end)
            date_adj: dict[date, float | None] = {
                p.business_date: (
                    float(p.adjusted_close_price) if p.adjusted_close_price is not None else None
                )
                for p in prices
            }
            us_price_map[us_ticker] = date_adj
            us_sorted_dates[us_ticker] = sorted(date_adj.keys())

        # ------------------------------------------------------------------
        # バッチ取得: JP 価格
        # ------------------------------------------------------------------

        # {jp_ticker: {business_date: (open_float | None, close_float | None)}}
        jp_price_map: dict[str, dict[date, tuple[float | None, float | None]]] = {}

        for jp_ticker in _ALL_JP_TICKERS:
            prices = self._price_repo.list_by_ticker(
                jp_ticker, start=buffer_start, end=training_end
            )
            jp_price_map[jp_ticker] = {
                p.business_date: (
                    float(p.open_price) if p.open_price is not None else None,
                    float(p.close_price) if p.close_price is not None else None,
                )
                for p in prices
            }

        # ------------------------------------------------------------------
        # 訓練行の構築 (complete-case)
        # ------------------------------------------------------------------

        X_rows: list[list[float]] = []
        Y_rows: list[list[float]] = []

        for _, alignment_row in alignment.iterrows():
            us_date: date = alignment_row[COL_US_SIGNAL_DATE]
            jp_date: date = alignment_row[COL_JP_EXECUTION_DATE]

            # ── X: US close-to-close リターン ──
            x_vals: list[float] = []
            skip = False
            for us_ticker in _ALL_US_TICKERS:
                dates = us_sorted_dates.get(us_ticker, [])
                adj_map = us_price_map.get(us_ticker, {})

                # us_date 以前の最後のインデックスを探す
                idx = bisect.bisect_right(dates, us_date) - 1

                # us_date が price data に存在するか確認
                if idx < 1 or dates[idx] != us_date:
                    skip = True
                    break

                curr_adj = adj_map[us_date]
                prev_adj = adj_map[dates[idx - 1]]

                if curr_adj is None or prev_adj is None or prev_adj == 0.0:
                    skip = True
                    break

                x_vals.append((curr_adj - prev_adj) / prev_adj)

            if skip:
                continue

            # ── Y: JP open-to-close リターン ──
            y_vals: list[float] = []
            for jp_ticker in _ALL_JP_TICKERS:
                oc = jp_price_map.get(jp_ticker, {}).get(jp_date)
                if oc is None:
                    skip = True
                    break
                open_p, close_p = oc
                if open_p is None or close_p is None or open_p == 0.0:
                    skip = True
                    break
                y_vals.append((close_p - open_p) / open_p)

            if skip:
                continue

            X_rows.append(x_vals)
            Y_rows.append(y_vals)

        if len(X_rows) < WINDOW_SIZE:
            return None

        return np.array(X_rows, dtype=float), np.array(Y_rows, dtype=float)

    def _fetch_current_us_returns(self, us_signal_date: date) -> np.ndarray | None:
        """現在の US リターンベクトルを取得する。

        # 先読み防止の核心
          get_prices_up_to の as_of_date には us_signal_date のみを渡す。
          jp_execution_date を as_of_date として渡すことは絶対にしない。

        Args:
            us_signal_date: リターン計算の基準日。この日以前のデータのみ参照する。

        Returns:
            US リターンベクトル (p,)。
            いずれかの US ティッカーで 2 行未満 / close が None / prev_close が 0 の場合は None。
        """
        x_vals: list[float] = []

        for us_ticker in _ALL_US_TICKERS:
            # as_of_date=us_signal_date: この日以前の最新 2 行のみ取得 (先読み防止)
            prices = self._price_repo.get_prices_up_to(
                us_ticker,
                as_of_date=us_signal_date,  # ← 先読み防止: jp_execution_date は絶対に渡さない
                limit=2,
            )

            if len(prices) < 2:
                return None

            prev_close = prices[-2].adjusted_close_price
            curr_close = prices[-1].adjusted_close_price

            if prev_close is None or curr_close is None or float(prev_close) == 0.0:
                return None

            x_vals.append((float(curr_close) - float(prev_close)) / float(prev_close))

        return np.array(x_vals, dtype=float)
