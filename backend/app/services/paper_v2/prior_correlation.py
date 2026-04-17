"""
paper_v2-lite: prior correlation matrix C_0 builder (純粋関数).

参照: paper_v2-lite v2.2-lite + addendum-1 §5-C
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Final

import numpy as np

logger = logging.getLogger(__name__)

_MIN_ROWS_SANITY: Final[int] = 100
_EPS: Final[float] = 1e-12
_DIAG_ONE_TOL: Final[float] = 1e-6


@dataclass(frozen=True)
class C0Artifact:
    """paper_v2-lite prior correlation artifact の純粋データ型。"""

    c0: np.ndarray                  # shape (28, 28), 対称, diag ≈ 1, float64
    v0: np.ndarray                  # shape (28, 3), build_v0 出力
    d0: np.ndarray                  # shape (3, 3), 対角行列, float64
    c_full: np.ndarray              # shape (28, 28), 対称, 参照用 (np.corrcoef 結果)
    effective_rows: int             # 呼び出し側が採用した complete-case 行数
    us_tickers: tuple[str, ...]
    jp_tickers: tuple[str, ...]
    c_full_train_start: date
    c_full_train_end: date


def build_c0_from_returns(
    rcc_matrix: np.ndarray,
    us_tickers: tuple[str, ...],
    jp_tickers: tuple[str, ...],
    c_full_train_start: date,
    c_full_train_end: date,
) -> C0Artifact:
    """
    純粋関数。DB アクセスなし。complete-case フィルタ済みリターン行列を受けて
    C_0 artifact を構築する。

    Args:
        rcc_matrix: shape (n_rows, 28), dtype float64。
          列順は (us_tickers 11) + (jp_tickers 17) の concat に厳密一致することを呼び出し側が保証。
          complete-case (欠損行なし) 前提。
        us_tickers: 長さ 11
        jp_tickers: 長さ 17
        c_full_train_start/end: artifact メタ用の日付。計算には使わない。

    処理:
      1. C_full = np.corrcoef(rcc_matrix.T)  # (28, 28)
      2. V_0 = build_v0(us_tickers, jp_tickers)  # lazy import
      3. D_0 = np.diag(np.diag(V_0.T @ C_full @ V_0))  # (3, 3) 対角化
      4. C_raw_0 = V_0 @ D_0 @ V_0.T  # (28, 28)
      5. Δ = np.diag(np.diag(C_raw_0))  # (28, 28) 対角
      6. Δ_inv_sqrt = diag(1/sqrt(diag(C_raw_0)))  # ゼロ割回避 (EPS で下限クリップ)
      7. C_0 = Δ_inv_sqrt @ C_raw_0 @ Δ_inv_sqrt
      8. diag(C_0) を 1 に明示設定 (数値誤差除去)、対称化: C_0 = (C_0 + C_0.T) / 2

    Returns:
        C0Artifact

    Raises:
        ValueError:
          - rcc_matrix が 2D でない
          - rcc_matrix.shape[1] != 28
          - rcc_matrix.shape[0] < _MIN_ROWS_SANITY (= 100)
          - len(us_tickers) != 11 または len(jp_tickers) != 17
          - rcc_matrix に NaN/Inf 含む（complete-case 前提違反）
          - C_raw_0 の対角に EPS 以下の値（D_0 が degenerate）
    """
    # ---- lazy import (循環依存回避とテスト分離のため関数内 import) ----
    from app.services.paper_v2.subspace import build_v0

    # ------------------------------------------------------------------
    # バリデーション
    # ------------------------------------------------------------------
    if rcc_matrix.ndim != 2:
        raise ValueError(
            f"rcc_matrix は 2D が必要です。実際の ndim: {rcc_matrix.ndim}"
        )

    if rcc_matrix.shape[1] != 28:
        raise ValueError(
            f"rcc_matrix.shape[1] は 28 が必要です。実際: {rcc_matrix.shape[1]}"
        )

    if rcc_matrix.shape[0] < _MIN_ROWS_SANITY:
        raise ValueError(
            f"rcc_matrix の行数は {_MIN_ROWS_SANITY} 以上が必要です。"
            f" 実際: {rcc_matrix.shape[0]}"
        )

    if len(us_tickers) != 11:
        raise ValueError(
            f"us_tickers は長さ 11 が必要です。実際: {len(us_tickers)}"
        )

    if len(jp_tickers) != 17:
        raise ValueError(
            f"jp_tickers は長さ 17 が必要です。実際: {len(jp_tickers)}"
        )

    if np.any(np.isnan(rcc_matrix)):
        raise ValueError(
            "rcc_matrix に NaN が含まれています。complete-case 前提違反です。"
        )

    if np.any(np.isinf(rcc_matrix)):
        raise ValueError(
            "rcc_matrix に Inf が含まれています。complete-case 前提違反です。"
        )

    # ------------------------------------------------------------------
    # ステップ 1: C_full = np.corrcoef(rcc_matrix.T)
    # ------------------------------------------------------------------
    # ゼロ分散列の早期検出
    col_var = np.var(rcc_matrix, axis=0, ddof=1)
    zero_var_mask = col_var < _EPS
    if zero_var_mask.any():
        all_tickers = tuple(us_tickers) + tuple(jp_tickers)
        offending = [(int(i), all_tickers[i]) for i in np.where(zero_var_mask)[0]]
        raise ValueError(
            f"rcc_matrix has zero-variance column(s), would produce NaN in corrcoef: "
            f"{offending}"
        )

    c_full = np.corrcoef(rcc_matrix.T)  # (28, 28)
    if not np.isfinite(c_full).all():
        raise ValueError(
            "c_full contains NaN or Inf after np.corrcoef; "
            "check for degenerate rcc_matrix columns"
        )

    # ------------------------------------------------------------------
    # ステップ 2: V_0 = build_v0(us_tickers, jp_tickers)
    # ------------------------------------------------------------------
    v0 = build_v0(us_tickers, jp_tickers)  # (28, 3)

    # ------------------------------------------------------------------
    # ステップ 3: D_0 = diag(diag(V_0.T @ C_full @ V_0))  # (3, 3)
    # ------------------------------------------------------------------
    inner = v0.T @ c_full @ v0  # (3, 3)
    d0 = np.diag(np.diag(inner))  # keep only diagonal

    # ------------------------------------------------------------------
    # ステップ 4: C_raw_0 = V_0 @ D_0 @ V_0.T  # (28, 28)
    # ------------------------------------------------------------------
    c_raw_0 = v0 @ d0 @ v0.T  # (28, 28)

    # ------------------------------------------------------------------
    # ステップ 5–6: 正規化 (対角要素の逆平方根)
    # ------------------------------------------------------------------
    diag_vals = np.diag(c_raw_0)  # (28,)

    if np.any(diag_vals <= _EPS):
        bad_idx = np.where(diag_vals <= _EPS)[0]
        bad_info = [(int(i), float(diag_vals[i])) for i in bad_idx]
        raise ValueError(
            f"C_raw_0 degenerate: D_0 diagonal elements at indices {bad_info} <= EPS"
        )

    # EPS でクリップしてゼロ割を回避 (バリデーション済みなのでここでは安全)
    diag_clipped = np.clip(diag_vals, _EPS, None)
    delta_inv_sqrt = np.diag(1.0 / np.sqrt(diag_clipped))  # (28, 28)

    # ------------------------------------------------------------------
    # ステップ 7: C_0 = Δ_inv_sqrt @ C_raw_0 @ Δ_inv_sqrt
    # ------------------------------------------------------------------
    c0 = delta_inv_sqrt @ c_raw_0 @ delta_inv_sqrt  # (28, 28)

    # ------------------------------------------------------------------
    # ステップ 8: 対角を 1 に固定、対称化
    # ------------------------------------------------------------------
    # 数値誤差チェック
    diag_c0 = np.diag(c0)
    if not np.allclose(diag_c0, 1.0, atol=_DIAG_ONE_TOL):
        logger.warning(
            "C_0 の対角が 1 に収束していません。最大偏差: %.2e",
            np.max(np.abs(diag_c0 - 1.0)),
        )

    # 対角を正確に 1 に設定
    np.fill_diagonal(c0, 1.0)

    # 対称化
    c0 = (c0 + c0.T) / 2.0

    return C0Artifact(
        c0=c0,
        v0=v0,
        d0=d0,
        c_full=c_full,
        effective_rows=rcc_matrix.shape[0],
        us_tickers=us_tickers,
        jp_tickers=jp_tickers,
        c_full_train_start=c_full_train_start,
        c_full_train_end=c_full_train_end,
    )
