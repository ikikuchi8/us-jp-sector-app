"""
paper_v2-lite: C_0 builder 純粋関数のテスト。

Task 7-1 の build_v0 は monkeypatch で差し替え、
Task 7-1 が未完成でも単体テストが走るように設計している。
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pytest

from app.services.paper_v2.prior_correlation import (
    C0Artifact,
    _MIN_ROWS_SANITY,
    build_c0_from_returns,
)


# ---------------------------------------------------------------------------
# フィクスチャ
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_v0_ortho():
    """Task 7-1 の build_v0 を模した orthonormal 28x3 行列 fixture。"""
    rng = np.random.default_rng(42)
    # ランダム行列を QR 分解して orthonormal 基底
    A = rng.standard_normal((28, 3))
    Q, _ = np.linalg.qr(A)
    return Q  # shape (28, 3)


@pytest.fixture
def patched_build_v0(monkeypatch, fake_v0_ortho):
    """subspace.build_v0 を fake で差し替える fixture。"""

    def _fake_build_v0(us_tickers, jp_tickers):
        assert len(us_tickers) == 11 and len(jp_tickers) == 17
        return fake_v0_ortho

    # lazy import されるパスに対して monkeypatch
    import sys

    # subspace モジュールが存在する場合のみパッチ
    try:
        import app.services.paper_v2.subspace as sp

        monkeypatch.setattr(sp, "build_v0", _fake_build_v0)
    except ImportError:
        # Task 7-1 未完成時: dummy モジュールを注入
        import types

        mod = types.ModuleType("app.services.paper_v2.subspace")
        mod.build_v0 = _fake_build_v0
        monkeypatch.setitem(sys.modules, "app.services.paper_v2.subspace", mod)


@pytest.fixture
def canonical_tickers():
    us = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")
    jp = (
        "1617.T",
        "1618.T",
        "1619.T",
        "1620.T",
        "1621.T",
        "1622.T",
        "1623.T",
        "1624.T",
        "1625.T",
        "1626.T",
        "1627.T",
        "1628.T",
        "1629.T",
        "1630.T",
        "1631.T",
        "1632.T",
        "1633.T",
    )
    return us, jp


@pytest.fixture
def synth_returns():
    """合成相関構造を持つリターン行列（729 行 × 28 列）。"""
    rng = np.random.default_rng(7)
    n, p = 729, 28
    # 軽い相関構造: 共通因子 + ノイズ
    f = rng.standard_normal((n, 3))
    loadings = rng.standard_normal((3, p)) * 0.3
    noise = rng.standard_normal((n, p)) * 0.01
    return f @ loadings + noise


@pytest.fixture
def train_dates():
    return date(2020, 1, 1), date(2022, 12, 31)


# ---------------------------------------------------------------------------
# ヘルパー: build_c0_from_returns を呼ぶ共通関数
# ---------------------------------------------------------------------------


def _call_build(rcc, us, jp, train_dates, patched_build_v0):
    """patched_build_v0 fixture で build_v0 をパッチした後に build_c0_from_returns を呼ぶ。"""
    # patched_build_v0 fixture を使用済みのため、そのまま呼べば OK
    return build_c0_from_returns(
        rcc_matrix=rcc,
        us_tickers=us,
        jp_tickers=jp,
        c_full_train_start=train_dates[0],
        c_full_train_end=train_dates[1],
    )


# ---------------------------------------------------------------------------
# テスト 1: shape と dtype
# ---------------------------------------------------------------------------


def test_c0_shape_and_dtype(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """C_0 (28,28) float64, V_0 (28,3), D_0 (3,3), c_full (28,28)"""
    us, jp = canonical_tickers
    art = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)

    assert isinstance(art, C0Artifact)
    assert art.c0.shape == (28, 28)
    assert art.c0.dtype == np.float64
    assert art.v0.shape == (28, 3)
    assert art.d0.shape == (3, 3)
    assert art.c_full.shape == (28, 28)


# ---------------------------------------------------------------------------
# テスト 2: C_0 対称性
# ---------------------------------------------------------------------------


def test_c0_symmetric(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """C_0 が対称行列であること。"""
    us, jp = canonical_tickers
    art = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)
    assert np.allclose(art.c0, art.c0.T, atol=1e-10)


# ---------------------------------------------------------------------------
# テスト 3: C_0 対角が 1
# ---------------------------------------------------------------------------


def test_c0_diag_one(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """C_0 の対角成分がすべて 1.0 であること。"""
    us, jp = canonical_tickers
    art = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)
    assert np.allclose(np.diag(art.c0), 1.0, atol=1e-8)


# ---------------------------------------------------------------------------
# テスト 4: c_full の対称性と対角
# ---------------------------------------------------------------------------


def test_c_full_symmetric_and_diag_one(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """c_full も対称で対角 1 (corrcoef の性質)。"""
    us, jp = canonical_tickers
    art = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)
    assert np.allclose(art.c_full, art.c_full.T, atol=1e-10)
    assert np.allclose(np.diag(art.c_full), 1.0, atol=1e-10)


# ---------------------------------------------------------------------------
# テスト 5: D_0 が対角行列
# ---------------------------------------------------------------------------


def test_d0_is_diagonal(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """D_0 の非対角成分がすべて 0 であること。"""
    us, jp = canonical_tickers
    art = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)
    off_diag = art.d0 - np.diag(np.diag(art.d0))
    assert np.allclose(off_diag, 0.0, atol=1e-12)


# ---------------------------------------------------------------------------
# テスト 6: shape バリデーション
# ---------------------------------------------------------------------------


def test_shape_validation_raises(canonical_tickers, train_dates):
    """不正な rcc_matrix shape で ValueError が発生すること。"""
    us, jp = canonical_tickers

    # 1D → ValueError
    rcc_1d = np.ones(28)
    with pytest.raises(ValueError, match="2D"):
        build_c0_from_returns(rcc_1d, us, jp, train_dates[0], train_dates[1])

    # shape[1] != 28 → ValueError
    rcc_wrong_cols = np.ones((200, 27))
    with pytest.raises(ValueError, match="28"):
        build_c0_from_returns(rcc_wrong_cols, us, jp, train_dates[0], train_dates[1])

    # shape[0] < 100 → ValueError
    rcc_too_few_rows = np.ones((99, 28))
    with pytest.raises(ValueError, match="100"):
        build_c0_from_returns(rcc_too_few_rows, us, jp, train_dates[0], train_dates[1])


# ---------------------------------------------------------------------------
# テスト 7: ticker 長さバリデーション
# ---------------------------------------------------------------------------


def test_ticker_length_validation(synth_returns, train_dates):
    """us 10 や jp 16 → ValueError。"""
    us_short = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV")  # 10
    jp_correct = (
        "1617.T", "1618.T", "1619.T", "1620.T", "1621.T", "1622.T", "1623.T",
        "1624.T", "1625.T", "1626.T", "1627.T", "1628.T", "1629.T", "1630.T",
        "1631.T", "1632.T", "1633.T",
    )
    with pytest.raises(ValueError, match="11"):
        build_c0_from_returns(synth_returns, us_short, jp_correct, train_dates[0], train_dates[1])

    us_correct = ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")
    jp_short = (
        "1617.T", "1618.T", "1619.T", "1620.T", "1621.T", "1622.T", "1623.T",
        "1624.T", "1625.T", "1626.T", "1627.T", "1628.T", "1629.T", "1630.T",
        "1631.T", "1632.T",
    )  # 16
    with pytest.raises(ValueError, match="17"):
        build_c0_from_returns(synth_returns, us_correct, jp_short, train_dates[0], train_dates[1])


# ---------------------------------------------------------------------------
# テスト 8: NaN 検出
# ---------------------------------------------------------------------------


def test_nan_detection(canonical_tickers, train_dates):
    """rcc_matrix に NaN が含まれると ValueError。"""
    us, jp = canonical_tickers
    rcc = np.ones((200, 28))
    rcc[10, 5] = np.nan
    with pytest.raises(ValueError, match="NaN"):
        build_c0_from_returns(rcc, us, jp, train_dates[0], train_dates[1])


# ---------------------------------------------------------------------------
# テスト 9: Inf 検出
# ---------------------------------------------------------------------------


def test_inf_detection(canonical_tickers, train_dates):
    """rcc_matrix に Inf が含まれると ValueError。"""
    us, jp = canonical_tickers
    rcc = np.ones((200, 28))
    rcc[5, 3] = np.inf
    with pytest.raises(ValueError, match="Inf"):
        build_c0_from_returns(rcc, us, jp, train_dates[0], train_dates[1])


# ---------------------------------------------------------------------------
# テスト 10: degenerate C_raw_0 で ValueError (既存テスト、役割明示のためリネーム)
# ---------------------------------------------------------------------------


def test_degenerate_via_all_zero_v0_raises(canonical_tickers, train_dates, monkeypatch):
    """
    補助的異常注入: build_v0 が病理的な全ゼロ V_0 を返すと、
    D_0 対角が 0 となり detect (defense in depth)。
    主契約テストは test_degenerate_constant_column_raises を参照。
    """
    us, jp = canonical_tickers

    import sys
    import types

    zero_v0 = np.zeros((28, 3))  # 全ゼロ → D_0 対角 = 0

    def _fake_zero_v0(us_tickers, jp_tickers):
        return zero_v0

    try:
        import app.services.paper_v2.subspace as sp
        monkeypatch.setattr(sp, "build_v0", _fake_zero_v0)
    except ImportError:
        mod = types.ModuleType("app.services.paper_v2.subspace")
        mod.build_v0 = _fake_zero_v0
        monkeypatch.setitem(sys.modules, "app.services.paper_v2.subspace", mod)

    rng = np.random.default_rng(99)
    rcc = rng.standard_normal((200, 28))

    with pytest.raises(ValueError, match="degenerate"):
        build_c0_from_returns(rcc, us, jp, train_dates[0], train_dates[1])


# ---------------------------------------------------------------------------
# テスト 10b: degenerate D_0 エラーメッセージに index 情報が含まれること
# ---------------------------------------------------------------------------


def test_degenerate_d0_message_includes_index(canonical_tickers, train_dates, monkeypatch):
    """degenerate D_0 ValueError が index 情報を含むこと。"""
    us, jp = canonical_tickers

    import sys
    import types

    zero_v0 = np.zeros((28, 3))  # 全ゼロ → D_0 対角 = 0 → index 情報が必要

    def _fake_zero_v0(us_tickers, jp_tickers):
        return zero_v0

    try:
        import app.services.paper_v2.subspace as sp
        monkeypatch.setattr(sp, "build_v0", _fake_zero_v0)
    except ImportError:
        mod = types.ModuleType("app.services.paper_v2.subspace")
        mod.build_v0 = _fake_zero_v0
        monkeypatch.setitem(sys.modules, "app.services.paper_v2.subspace", mod)

    rng = np.random.default_rng(99)
    rcc = rng.standard_normal((200, 28))

    with pytest.raises(ValueError) as exc_info:
        build_c0_from_returns(rcc, us, jp, train_dates[0], train_dates[1])

    msg = str(exc_info.value)
    assert "degenerate" in msg.lower() or "indices" in msg.lower()
    # index 数値が含まれる (0, 1, 2 いずれか)
    assert any(str(i) in msg for i in range(3))


# ---------------------------------------------------------------------------
# テスト 10c: 定数列 (ゼロ分散) で ValueError — 主契約テスト
# ---------------------------------------------------------------------------


def test_degenerate_constant_column_raises(canonical_tickers, patched_build_v0):
    """
    主契約テスト: 定数列 (ゼロ分散) が rcc_matrix にあると、
    np.corrcoef が NaN を生じる前に ValueError を raise し、
    ticker index を示す。
    """
    us, jp = canonical_tickers
    rng = np.random.default_rng(123)
    n = 729
    rcc = rng.standard_normal((n, 28))
    rcc[:, 5] = 0.0  # index 5 列だけ定数 (zero variance)
    with pytest.raises(ValueError) as exc_info:
        build_c0_from_returns(rcc, us, jp, date(2021, 1, 12), date(2023, 12, 31))
    msg = str(exc_info.value).lower()
    assert "zero-variance" in msg or "corrcoef" in msg
    assert "5" in str(exc_info.value)  # ticker index


# ---------------------------------------------------------------------------
# テスト 10d: 正常入力で c_full が finite であること
# ---------------------------------------------------------------------------


def test_c_full_is_finite_for_healthy_input(canonical_tickers, patched_build_v0, synth_returns):
    """正常入力で artifact.c_full が finite (NaN/Inf 無し)。"""
    us, jp = canonical_tickers
    artifact = build_c0_from_returns(synth_returns, us, jp, date(2021, 1, 12), date(2023, 12, 31))
    assert np.isfinite(artifact.c_full).all()
    assert np.isfinite(artifact.c0).all()
    assert np.isfinite(artifact.d0).all()


# ---------------------------------------------------------------------------
# テスト 11: effective_rows のパススルー
# ---------------------------------------------------------------------------


def test_effective_rows_passthrough(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """C0Artifact.effective_rows が rcc_matrix.shape[0] と一致すること。"""
    us, jp = canonical_tickers
    art = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)
    assert art.effective_rows == synth_returns.shape[0]


# ---------------------------------------------------------------------------
# テスト 12: メタデータのパススルー
# ---------------------------------------------------------------------------


def test_metadata_passthrough(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """c_full_train_start/end, us_tickers, jp_tickers が artifact に正しく入ること。"""
    us, jp = canonical_tickers
    art = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)

    assert art.c_full_train_start == train_dates[0]
    assert art.c_full_train_end == train_dates[1]
    assert art.us_tickers == us
    assert art.jp_tickers == jp


# ---------------------------------------------------------------------------
# テスト 13: 決定論性
# ---------------------------------------------------------------------------


def test_determinism(
    patched_build_v0, synth_returns, canonical_tickers, train_dates
):
    """同じ入力で 2 回呼び、すべての配列が np.array_equal であること。"""
    us, jp = canonical_tickers
    art1 = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)
    art2 = _call_build(synth_returns, us, jp, train_dates, patched_build_v0)

    assert np.array_equal(art1.c0, art2.c0)
    assert np.array_equal(art1.v0, art2.v0)
    assert np.array_equal(art1.d0, art2.d0)
    assert np.array_equal(art1.c_full, art2.c_full)
    assert art1.effective_rows == art2.effective_rows
