"""paper_v2-lite の V_0 prior subspace 構築ユーティリティ。

# 設計方針
  - 純粋関数のみ。DB・IO 一切なし。
  - build_v0() は決定論的: 同じ引数で常に同じ結果を返す。
  - 論文 §4.2 の prior subspace 定義に準拠。v2.2-lite では固定。
"""
from __future__ import annotations

import numpy as np

from app.services.paper_v2.sector_tags import (
    CYCLICAL_JP,
    CYCLICAL_US,
    DEFENSIVE_JP,
    DEFENSIVE_US,
)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_N_US: int = 11
_N_JP: int = 17
_N_TOTAL: int = _N_US + _N_JP  # 28
_K0: int = 3  # prior subspace 次元数

_GS_COLLAPSE_THRESHOLD: float = 1e-12  # Gram-Schmidt 崩壊判定閾値


# ---------------------------------------------------------------------------
# 公開関数
# ---------------------------------------------------------------------------


def build_v0(
    us_tickers: tuple[str, ...],
    jp_tickers: tuple[str, ...],
) -> np.ndarray:
    """
    prior subspace V_0 ∈ R^(N x K0) を構築する純粋関数。

    Args:
        us_tickers: 長さ 11 の US ticker tuple (canonical 昇順)
        jp_tickers: 長さ 17 の JP ticker tuple (canonical 昇順)

    Returns:
        V_0: shape (28, 3), dtype float64
          列 0 (v_1): market factor — 全成分正、単位ベクトル
          列 1 (v_2): US-JP spread — US ブロック正、単位ベクトル、v_1 と直交
          列 2 (v_3): cyclical-defensive — cyclical 側正、単位ベクトル、v_1,v_2 と直交

        V_0.T @ V_0 ≈ I_3 (atol 1e-10)

    Raises:
        ValueError: 長さ不正、または sector_tags の ticker が入力に無い場合
    """
    # -----------------------------------------------------------------------
    # ステップ 1: 入力バリデーション
    # -----------------------------------------------------------------------
    if len(us_tickers) != _N_US or len(jp_tickers) != _N_JP:
        raise ValueError(
            f"us_tickers は長さ {_N_US}、jp_tickers は長さ {_N_JP} が必要です。"
            f" 実際: us={len(us_tickers)}, jp={len(jp_tickers)}"
        )

    if len(set(us_tickers)) != 11:
        duplicates = [t for t in us_tickers if us_tickers.count(t) > 1]
        raise ValueError(f"duplicate tickers in us_tickers: {sorted(set(duplicates))}")
    if len(set(jp_tickers)) != 17:
        duplicates = [t for t in jp_tickers if jp_tickers.count(t) > 1]
        raise ValueError(f"duplicate tickers in jp_tickers: {sorted(set(duplicates))}")

    us_set = set(us_tickers)
    jp_set = set(jp_tickers)

    # sector_tags の全 ticker が入力に含まれているか確認
    if not (CYCLICAL_US | DEFENSIVE_US).issubset(us_set):
        missing = (CYCLICAL_US | DEFENSIVE_US) - us_set
        raise ValueError(f"US sector_tags の ticker が入力に含まれていません: {missing}")

    if not (CYCLICAL_JP | DEFENSIVE_JP).issubset(jp_set):
        missing = (CYCLICAL_JP | DEFENSIVE_JP) - jp_set
        raise ValueError(f"JP sector_tags の ticker が入力に含まれていません: {missing}")

    N = _N_TOTAL  # 28

    # -----------------------------------------------------------------------
    # ステップ 3: v_1 — market factor (全成分等ウェイト)
    # -----------------------------------------------------------------------
    v1_raw = np.ones(N, dtype=np.float64)
    v_1 = _unit_norm(v1_raw)

    # -----------------------------------------------------------------------
    # ステップ 4: v_2 — US-JP spread
    # -----------------------------------------------------------------------
    v2_raw = np.concatenate([np.ones(_N_US), -np.ones(_N_JP)]).astype(np.float64)
    v2_gs = _gram_schmidt_step(v2_raw, [v_1])
    v_2 = _unit_norm(v2_gs)

    # -----------------------------------------------------------------------
    # ステップ 5: v_3 raw — cyclical-defensive
    # -----------------------------------------------------------------------
    v3_raw = np.zeros(N, dtype=np.float64)

    for i, ticker in enumerate(us_tickers):
        if ticker in CYCLICAL_US:
            v3_raw[i] = +1.0
        elif ticker in DEFENSIVE_US:
            v3_raw[i] = -1.0
        # 中立: 0 のまま

    for j, ticker in enumerate(jp_tickers):
        if ticker in CYCLICAL_JP:
            v3_raw[_N_US + j] = +1.0
        elif ticker in DEFENSIVE_JP:
            v3_raw[_N_US + j] = -1.0
        # 中立: 0 のまま

    # -----------------------------------------------------------------------
    # ステップ 6: Gram-Schmidt (v_1, v_2 を引く) → unit norm → v_3
    # -----------------------------------------------------------------------
    v3_gs = _gram_schmidt_step(v3_raw, [v_1, v_2])
    v_3 = _unit_norm(v3_gs)

    # -----------------------------------------------------------------------
    # ステップ 7: 符号規約チェック
    # -----------------------------------------------------------------------
    # v_1: 最初の成分が正になるよう符号固定
    if v_1[0] < 0:
        v_1 = -v_1

    # v_2: US ブロック先頭成分が正になるよう符号固定
    if v_2[0] < 0:
        v_2 = -v_2

    # v_3: cyclical インデックス群の平均成分が正になるよう符号固定
    cyclical_indices_us = [i for i, t in enumerate(us_tickers) if t in CYCLICAL_US]
    cyclical_indices_jp = [_N_US + j for j, t in enumerate(jp_tickers) if t in CYCLICAL_JP]
    cyclical_indices = cyclical_indices_us + cyclical_indices_jp

    if v_3[cyclical_indices].mean() < 0:
        v_3 = -v_3

    # -----------------------------------------------------------------------
    # ステップ 8: V_0 を返す
    # -----------------------------------------------------------------------
    V_0 = np.column_stack([v_1, v_2, v_3])
    return V_0


# ---------------------------------------------------------------------------
# プライベートユーティリティ
# ---------------------------------------------------------------------------


def _gram_schmidt_step(
    v: np.ndarray,
    basis: list[np.ndarray],
) -> np.ndarray:
    """v から basis の各ベクトルの射影成分を引いて直交化する (Gram-Schmidt)。

    Args:
        v:     直交化対象ベクトル (N,)
        basis: 既に正規化済みの直交基底ベクトルのリスト

    Returns:
        直交化後のベクトル (単位正規化はしない)

    Raises:
        ValueError: 直交化後のノルムが _GS_COLLAPSE_THRESHOLD 未満の場合
    """
    result = v.copy()
    for b in basis:
        result = result - np.dot(result, b) * b

    norm = np.linalg.norm(result)
    if norm < _GS_COLLAPSE_THRESHOLD:
        raise ValueError("v_k collapsed during Gram-Schmidt")

    return result


def _unit_norm(v: np.ndarray) -> np.ndarray:
    """ベクトルを単位正規化する。

    Args:
        v: 入力ベクトル (N,)

    Returns:
        単位ベクトル (N,)

    Raises:
        ValueError: ノルムが _GS_COLLAPSE_THRESHOLD 未満の場合
    """
    norm = np.linalg.norm(v)
    if norm < _GS_COLLAPSE_THRESHOLD:
        raise ValueError("v_k collapsed during Gram-Schmidt")
    return v / norm
