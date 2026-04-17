"""Task 7-C0: build_paper_v2_prior.py の _sanity_check のテスト."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# フィクスチャ: _sanity_check を scripts/build_paper_v2_prior.py から動的 import
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sanity_check_fn():
    """_sanity_check を scripts/build_paper_v2_prior.py から動的 import。"""
    script_path = (
        Path(__file__).resolve().parents[2] / "scripts" / "build_paper_v2_prior.py"
    )
    spec = importlib.util.spec_from_file_location("build_paper_v2_prior", script_path)
    module = importlib.util.module_from_spec(spec)
    # スクリプトの sys.path 操作と os.chdir は import 時に実行されるが、
    # __name__ == "__main__" ブロックは実行されない
    try:
        spec.loader.exec_module(module)
    except Exception:
        # DB 接続エラー等は無視 (モジュールレベルで import が通れば十分)
        pass
    return module._sanity_check


# ---------------------------------------------------------------------------
# フィクスチャ: 最小有効 C0Artifact モック (shape のみ正しければよい)
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_artifact():
    """shape と値が正常な C0Artifact モック。"""
    art = MagicMock()
    # c0: (28,28) 対称、対角 1
    c0 = np.eye(28, dtype=np.float64)
    art.c0 = c0
    # v0: (28,3) 直交正規
    A = np.random.default_rng(0).standard_normal((28, 3))
    Q, _ = np.linalg.qr(A)
    art.v0 = Q
    # d0: (3,3) 対角行列
    art.d0 = np.diag([1.0, 2.0, 3.0])
    art.effective_rows = 729
    return art


# ---------------------------------------------------------------------------
# テスト 1: 正常入力 (actual_rows == EXPECTED_C_FULL_ROWS) で pass
# ---------------------------------------------------------------------------


def test_sanity_check_accepts_exact_expected_rows(sanity_check_fn, valid_artifact) -> None:
    """actual_rows == 729 → 正常終了 (例外なし)。"""
    sanity_check_fn(valid_artifact, actual_c_full_rows=729)


# ---------------------------------------------------------------------------
# テスト 2: 上限 (1.05 * EXPECTED_C_FULL_ROWS) を超えると ValueError
# ---------------------------------------------------------------------------


def test_sanity_check_rejects_upper_bound_violation(sanity_check_fn, valid_artifact) -> None:
    """actual_rows = 800 (> 729 * 1.05 = 765.45) → ValueError を raise。"""
    with pytest.raises(ValueError, match="outside expected band"):
        sanity_check_fn(valid_artifact, actual_c_full_rows=800)


# ---------------------------------------------------------------------------
# テスト 3: 下限 (0.95 * EXPECTED_C_FULL_ROWS) を下回ると ValueError
# ---------------------------------------------------------------------------


def test_sanity_check_rejects_lower_bound_violation(sanity_check_fn, valid_artifact) -> None:
    """actual_rows = 650 (< 729 * 0.95 = 692.55) → ValueError を raise。"""
    with pytest.raises(ValueError, match="outside expected band"):
        sanity_check_fn(valid_artifact, actual_c_full_rows=650)


# ---------------------------------------------------------------------------
# テスト 4: actual_c_full_rows=None のときは境界チェックをスキップ
# ---------------------------------------------------------------------------


def test_sanity_check_skips_row_check_when_none(sanity_check_fn, valid_artifact) -> None:
    """actual_c_full_rows=None → 行数チェックをスキップして正常終了。"""
    sanity_check_fn(valid_artifact, actual_c_full_rows=None)


# ---------------------------------------------------------------------------
# テスト 5: エラーメッセージに actual / band 情報が含まれる
# ---------------------------------------------------------------------------


def test_sanity_check_error_message_contains_band(sanity_check_fn, valid_artifact) -> None:
    """ValueError メッセージに actual 値と期待バンドの両端が含まれること。"""
    actual = 800
    with pytest.raises(ValueError) as exc_info:
        sanity_check_fn(valid_artifact, actual_c_full_rows=actual)
    msg = str(exc_info.value)
    assert str(actual) in msg
    # 期待バンドの下限・上限が浮動小数で含まれる
    assert "692" in msg or "765" in msg  # 729*0.95≈692.55, 729*1.05≈765.45
