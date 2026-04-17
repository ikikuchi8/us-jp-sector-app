"""
paper_v2-lite: Subspace-regularized PCA core (純粋関数).

参照: 論文 §3.2 式 (13)–(15), v2.2-lite addendum-1 §5-C
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np

_EPS: Final[float] = 1e-12


@dataclass(frozen=True)
class RegPcaResult:
    """
    subspace-regularized PCA の結果。

    Attributes:
        V_U:  shape (n_us, k), US ブロック loadings
        V_J:  shape (N - n_us, k), JP ブロック loadings
        V_K:  shape (N, k), 完全固有ベクトル行列 [V_U; V_J]
        c_t:  shape (N, N), サンプル相関 (shrinkage 前、診断用)
        c_reg: shape (N, N), 正則化後相関 (対称化済)
        top_k_eigenvalues: shape (k,), 降順 (c_reg の上位 k 固有値)
        condition_number: c_reg の条件数 (max_eig / max(min_eig, EPS))
    """

    V_U: np.ndarray
    V_J: np.ndarray
    V_K: np.ndarray
    c_t: np.ndarray
    c_reg: np.ndarray
    top_k_eigenvalues: np.ndarray
    condition_number: float


def fit_reg_pca(
    z_window: np.ndarray,
    c_0: np.ndarray,
    lam: float,
    k: int,
    n_us: int,
) -> RegPcaResult:
    """
    subspace-regularized PCA を適用し、上位 k 主成分を US/JP に分割して返す純粋関数。

    Args:
        z_window: (L, N) 標準化済みリターン (L 行、N 列)
        c_0: (N, N) prior 相関行列 (対称、diag ≈ 1)
        lam: λ ∈ [0, 1], shrinkage 強度
        k: 主成分数、1 <= k <= min(N, L-1)
        n_us: US ブロック先頭列数、1 <= n_us < N

    Returns:
        RegPcaResult

    Raises:
        ValueError:
          - z_window が 2D でない、shape[0] < 3 (サンプル不足)
          - c_0 shape != (N, N) (z_window 列数と不一致)
          - c_0 が対称でない (atol 1e-8)
          - lam ∉ [0, 1]
          - k ∉ [1, min(N, L-1)]
          - n_us ∉ [1, N-1]
          - z_window または c_0 に NaN/Inf
          - C_t 計算後 NaN/Inf (ゼロ分散列検出 → ticker index 付きメッセージ)
    """
    # ------------------------------------------------------------------
    # バリデーション
    # ------------------------------------------------------------------
    if z_window.ndim != 2:
        raise ValueError(
            f"z_window は 2D が必要です。実際の ndim: {z_window.ndim}"
        )

    L, N = z_window.shape

    if L < 3:
        raise ValueError(
            f"z_window の行数 (サンプル数) は 3 以上が必要です。実際: {L}"
        )

    if np.any(np.isnan(z_window)):
        raise ValueError("z_window に NaN が含まれています。")

    if np.any(np.isinf(z_window)):
        raise ValueError("z_window に Inf が含まれています。")

    if c_0.ndim != 2 or c_0.shape != (N, N):
        raise ValueError(
            f"c_0 の shape は ({N}, {N}) が必要です。実際: {c_0.shape}"
        )

    if np.any(np.isnan(c_0)):
        raise ValueError("c_0 に NaN が含まれています。")

    if np.any(np.isinf(c_0)):
        raise ValueError("c_0 に Inf が含まれています。")

    if not np.allclose(c_0, c_0.T, atol=1e-8):
        raise ValueError("c_0 が対称ではありません (atol=1e-8)。")

    if not (0.0 <= lam <= 1.0):
        raise ValueError(f"lam は [0, 1] の範囲が必要です。実際: {lam}")

    k_max = min(N, L - 1)
    if not (1 <= k <= k_max):
        raise ValueError(
            f"k は [1, {k_max}] の範囲が必要です。実際: {k}"
        )

    if not (1 <= n_us <= N - 1):
        raise ValueError(
            f"n_us は [1, {N - 1}] の範囲が必要です。実際: {n_us}"
        )

    # ------------------------------------------------------------------
    # ステップ 1: C_t = np.corrcoef(z_window.T)
    # ------------------------------------------------------------------
    # ゼロ分散列の早期検出
    col_var = np.var(z_window, axis=0, ddof=1)
    zero_var_mask = col_var < _EPS
    if zero_var_mask.any():
        offending_indices = np.where(zero_var_mask)[0].tolist()
        raise ValueError(
            f"z_window に zero-variance な列があります (corrcoef で NaN になります)。"
            f" 列インデックス: {offending_indices}"
        )

    c_t = np.corrcoef(z_window.T)  # (N, N)

    if not np.isfinite(c_t).all():
        raise ValueError(
            "C_t に NaN または Inf が含まれています。z_window に縮退した列がある可能性があります。"
        )

    # ------------------------------------------------------------------
    # ステップ 2: C_reg = (1 - lam) * C_t + lam * c_0
    # ------------------------------------------------------------------
    c_reg = (1.0 - lam) * c_t + lam * c_0  # (N, N)

    # ------------------------------------------------------------------
    # ステップ 3: 対称化
    # ------------------------------------------------------------------
    c_reg = (c_reg + c_reg.T) / 2.0  # (N, N)

    # ------------------------------------------------------------------
    # ステップ 4: 固有値分解 (eigh は対称行列に特化、昇順で返す)
    # ------------------------------------------------------------------
    eigvals, eigvecs = np.linalg.eigh(c_reg)  # 昇順

    # ------------------------------------------------------------------
    # ステップ 5: 降順に並び替え
    # ------------------------------------------------------------------
    idx = np.argsort(eigvals)[::-1]
    eigvals_sorted = eigvals[idx]
    eigvecs_sorted = eigvecs[:, idx]

    # ------------------------------------------------------------------
    # ステップ 6–7: V_K 分割
    # ------------------------------------------------------------------
    V_K = eigvecs_sorted[:, :k]       # (N, k)
    V_U = V_K[:n_us, :]              # (n_us, k)
    V_J = V_K[n_us:, :]             # (N - n_us, k)

    # ------------------------------------------------------------------
    # ステップ 8: 上位 k 固有値
    # ------------------------------------------------------------------
    top_k_eigenvalues = eigvals_sorted[:k].copy()

    # ------------------------------------------------------------------
    # ステップ 9: condition number
    # 最小固有値が負になる場合 (数値誤差) は abs を取って EPS で下限クリップ
    # ------------------------------------------------------------------
    max_eig = eigvals_sorted[0]
    min_eig = eigvals_sorted[-1]
    # 負の固有値は数値誤差: abs を取った上で EPS で下限保護
    condition_number = float(max_eig / max(abs(min_eig), _EPS))

    return RegPcaResult(
        V_U=V_U,
        V_J=V_J,
        V_K=V_K,
        c_t=c_t,
        c_reg=c_reg,
        top_k_eigenvalues=top_k_eigenvalues,
        condition_number=condition_number,
    )
