"""tests/paper_v2/test_subspace.py

paper_v2-lite の V_0 prior subspace ユーティリティのテスト。
"""
from __future__ import annotations

import numpy as np
import pytest

from app.seed_data.sector_mapping import ALL_JP_TICKERS, JP_TICKER_TO_US_TICKERS
from app.services.paper_v2.sector_tags import (
    CYCLICAL_JP,
    CYCLICAL_US,
    DEFENSIVE_JP,
    DEFENSIVE_US,
)
from app.services.paper_v2.subspace import build_v0

# ---------------------------------------------------------------------------
# canonical ticker tuples (sector_mapping.py 正本から取得)
# ---------------------------------------------------------------------------

_ALL_US_TICKERS: tuple[str, ...] = tuple(
    sorted({t for lst in JP_TICKER_TO_US_TICKERS.values() for t in lst})
)
_ALL_JP_TICKERS: tuple[str, ...] = tuple(ALL_JP_TICKERS)


# ---------------------------------------------------------------------------
# ヘルパー: V_0 を一度だけ構築
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def V0() -> np.ndarray:
    return build_v0(_ALL_US_TICKERS, _ALL_JP_TICKERS)


# ---------------------------------------------------------------------------
# テスト 1: shape と dtype
# ---------------------------------------------------------------------------


def test_build_v0_shape_and_dtype(V0: np.ndarray) -> None:
    assert V0.shape == (28, 3), f"Expected shape (28, 3), got {V0.shape}"
    assert V0.dtype == np.float64, f"Expected float64, got {V0.dtype}"


# ---------------------------------------------------------------------------
# テスト 2: 直交正規性 V_0.T @ V_0 ≈ I_3
# ---------------------------------------------------------------------------


def test_build_v0_orthonormal(V0: np.ndarray) -> None:
    product = V0.T @ V0
    np.testing.assert_allclose(
        product,
        np.eye(3),
        atol=1e-10,
        err_msg="V_0.T @ V_0 が単位行列に一致しない (直交正規性違反)",
    )


# ---------------------------------------------------------------------------
# テスト 3: v_1 — market factor (全成分正、1/sqrt(28))
# ---------------------------------------------------------------------------


def test_build_v0_v1_is_market(V0: np.ndarray) -> None:
    v1 = V0[:, 0]
    expected = 1.0 / np.sqrt(28)

    # 全成分が正
    assert np.all(v1 > 0), "v_1 に非正の成分がある"

    # 各成分が 1/sqrt(28) に一致 (rtol 1e-12)
    np.testing.assert_allclose(
        np.abs(v1),
        expected,
        rtol=1e-12,
        err_msg="v_1 の各成分が 1/sqrt(28) に一致しない",
    )


# ---------------------------------------------------------------------------
# テスト 4: v_2 — US ブロック > 0、JP ブロック < 0
# ---------------------------------------------------------------------------


def test_build_v0_v2_sign(V0: np.ndarray) -> None:
    v2 = V0[:, 1]
    us_mean = v2[:11].mean()
    jp_mean = v2[11:].mean()

    assert us_mean > 0, f"v_2 の US ブロック平均が非正: {us_mean}"
    assert jp_mean < 0, f"v_2 の JP ブロック平均が非負: {jp_mean}"


# ---------------------------------------------------------------------------
# テスト 5: v_3 — cyclical 正、defensive 負、中立業種は小さい
# ---------------------------------------------------------------------------


def test_build_v0_v3_cyclical_positive(V0: np.ndarray) -> None:
    v3 = V0[:, 2]

    # cyclical インデックス (US)
    cyclical_us_idx = [i for i, t in enumerate(_ALL_US_TICKERS) if t in CYCLICAL_US]
    # cyclical インデックス (JP)
    cyclical_jp_idx = [11 + j for j, t in enumerate(_ALL_JP_TICKERS) if t in CYCLICAL_JP]
    cyclical_idx = cyclical_us_idx + cyclical_jp_idx

    # defensive インデックス (US)
    defensive_us_idx = [i for i, t in enumerate(_ALL_US_TICKERS) if t in DEFENSIVE_US]
    # defensive インデックス (JP)
    defensive_jp_idx = [11 + j for j, t in enumerate(_ALL_JP_TICKERS) if t in DEFENSIVE_JP]
    defensive_idx = defensive_us_idx + defensive_jp_idx

    # 中立インデックス
    all_idx = set(range(28))
    neutral_idx = list(all_idx - set(cyclical_idx) - set(defensive_idx))

    assert v3[cyclical_idx].mean() > 0, "cyclical 側の v_3 平均が非正"
    assert v3[defensive_idx].mean() < 0, "defensive 側の v_3 平均が非負"

    if neutral_idx:
        assert np.all(np.abs(v3[neutral_idx]) < 0.3), (
            f"中立業種の v_3 絶対値が 0.3 以上: {v3[neutral_idx]}"
        )


# ---------------------------------------------------------------------------
# テスト 6: 決定論性 — 2 回呼んで完全一致
# ---------------------------------------------------------------------------


def test_build_v0_deterministic() -> None:
    V0_a = build_v0(_ALL_US_TICKERS, _ALL_JP_TICKERS)
    V0_b = build_v0(_ALL_US_TICKERS, _ALL_JP_TICKERS)
    assert np.array_equal(V0_a, V0_b), "build_v0 が同じ引数で異なる結果を返した"


# ---------------------------------------------------------------------------
# テスト 7: us_tickers 長さエラー
# ---------------------------------------------------------------------------


def test_build_v0_length_error() -> None:
    short_us = _ALL_US_TICKERS[:10]  # 11 → 10 に短縮
    with pytest.raises(ValueError):
        build_v0(short_us, _ALL_JP_TICKERS)


# ---------------------------------------------------------------------------
# テスト 8: sector_tags に含まれる ticker が入力に無い場合 ValueError
# ---------------------------------------------------------------------------


def test_build_v0_unknown_ticker() -> None:
    # CYCLICAL_US の ticker (XLB) を別の ticker に差し替え
    us_without_xlb = tuple(t if t != "XLB" else "XLZ" for t in _ALL_US_TICKERS)
    with pytest.raises(ValueError):
        build_v0(us_without_xlb, _ALL_JP_TICKERS)


# ---------------------------------------------------------------------------
# テスト 9: regression test — canonical 順で各列の符号と代表成分値を確認
# ---------------------------------------------------------------------------


def test_build_v0_ordering(V0: np.ndarray) -> None:
    """canonical ticker 順 (XLB..XLY, 1617..1633) で build した場合の
    regression test。ハードコードされた成分値で前回実行との一致を確認する。
    """
    v1, v2, v3 = V0[:, 0], V0[:, 1], V0[:, 2]

    # v_1: 全成分が 1/sqrt(28) に一致
    expected_v1 = 1.0 / np.sqrt(28)
    np.testing.assert_allclose(v1[0], expected_v1, rtol=1e-12)
    np.testing.assert_allclose(v1[-1], expected_v1, rtol=1e-12)

    # v_2: US ブロック先頭 (XLB, index=0) が正、JP ブロック先頭 (1617.T, index=11) が負
    assert v2[0] > 0, "v_2[0] (XLB) が正でない"
    assert v2[11] < 0, "v_2[11] (1617.T) が正でない"

    # v_2 の代表値 (regression)
    np.testing.assert_allclose(v2[0], 0.23493574694968236, rtol=1e-10)
    np.testing.assert_allclose(v2[11], -0.15201724802626510, rtol=1e-10)

    # v_3: CYCLICAL_US 先頭 (XLB=index 0) が正、DEFENSIVE_US 先頭 (XLK=index 5) が負
    # _ALL_US_TICKERS は昇順: XLB(0), XLC(1), XLE(2), XLF(3), XLI(4), XLK(5), XLP(6), XLRE(7), XLU(8), XLV(9), XLY(10)
    assert v3[0] > 0, "v_3[0] (XLB, cyclical) が正でない"   # XLB in CYCLICAL_US
    assert v3[5] < 0, "v_3[5] (XLK, defensive) が正でない"  # XLK in DEFENSIVE_US

    # v_3 の代表値 (regression)
    np.testing.assert_allclose(v3[0], 0.25, rtol=1e-10)   # XLB cyclical
    np.testing.assert_allclose(v3[5], -0.25, rtol=1e-10)  # XLK defensive


# ---------------------------------------------------------------------------
# テスト 9b: 重複 US ticker で ValueError
# ---------------------------------------------------------------------------


def test_build_v0_duplicate_us_ticker_raises() -> None:
    us = ("XLB", "XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")  # XLB 重複
    jp = (
        "1617.T", "1618.T", "1619.T", "1620.T", "1621.T", "1622.T", "1623.T", "1624.T",
        "1625.T", "1626.T", "1627.T", "1628.T", "1629.T", "1630.T", "1631.T", "1632.T", "1633.T",
    )
    with pytest.raises(ValueError, match="duplicate"):
        build_v0(us, jp)


# ---------------------------------------------------------------------------
# テスト 9c: 重複 JP ticker で ValueError
# ---------------------------------------------------------------------------


def test_build_v0_duplicate_jp_ticker_raises() -> None:
    us = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")
    jp = (
        "1617.T", "1617.T", "1619.T", "1620.T", "1621.T", "1622.T", "1623.T", "1624.T",
        "1625.T", "1626.T", "1627.T", "1628.T", "1629.T", "1630.T", "1631.T", "1632.T", "1633.T",
    )  # 1617.T 重複
    with pytest.raises(ValueError, match="duplicate"):
        build_v0(us, jp)


# ---------------------------------------------------------------------------
# テスト 10: sector_tags の frozenset 定義が改変されていないことを確認
# ---------------------------------------------------------------------------


def test_sector_tags_frozen() -> None:
    assert CYCLICAL_US == frozenset({"XLB", "XLE", "XLF", "XLRE"}), (
        "CYCLICAL_US が期待値と異なる"
    )
    assert DEFENSIVE_US == frozenset({"XLK", "XLP", "XLU", "XLV"}), (
        "DEFENSIVE_US が期待値と異なる"
    )
    assert CYCLICAL_JP == frozenset({"1618.T", "1625.T", "1629.T", "1631.T"}), (
        "CYCLICAL_JP が期待値と異なる"
    )
    assert DEFENSIVE_JP == frozenset({"1617.T", "1621.T", "1627.T", "1630.T"}), (
        "DEFENSIVE_JP が期待値と異なる"
    )
