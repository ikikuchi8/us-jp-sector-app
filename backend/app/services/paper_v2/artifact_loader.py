"""
paper_v2-lite: artifact ローダー。

C_0 artifact (c0_v1.npz + c0_v1.meta.json) をロードし、
self-check を全て通過した場合のみ LoadedC0Artifact を返す。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np

from app.services.paper_v2.constants import (
    C0_ARTIFACT_PATH,
    C0_META_PATH,
    C_FULL_TRAIN_END,
    C_FULL_TRAIN_START,
    K,
    N_JP,
    N_US,
    UNIVERSE_SIZE,
)

# 必須 meta キー
_REQUIRED_META_KEYS: tuple[str, ...] = (
    "schema_version",
    "artifact_version",
    "c_full_train_start",
    "c_full_train_end",
    "paper_v2_oos_start",
    "universe_size",
    "K0",
    "us_tickers",
    "jp_tickers",
    "cyclical_us",
    "defensive_us",
    "cyclical_jp",
    "defensive_jp",
    "expected_c_full_rows",
    "actual_c_full_rows",
    "top3_eigenvalues_of_c_full",
    "built_at",
    "built_from_git_sha",
    "sha256_of_c0_npz",
)


@dataclass(frozen=True)
class LoadedC0Artifact:
    """artifact_loader の出力 (immutable)."""

    c0: np.ndarray          # (28, 28)
    v0: np.ndarray          # (28, 3)
    d0: np.ndarray          # (3, 3)
    c_full: np.ndarray      # (28, 28)
    us_tickers: tuple[str, ...]
    jp_tickers: tuple[str, ...]
    meta: dict              # meta.json の全内容


def load_c0_artifact(
    npz_path: Path = C0_ARTIFACT_PATH,
    meta_path: Path = C0_META_PATH,
    *,
    expected_us_tickers: tuple[str, ...],
    expected_jp_tickers: tuple[str, ...],
) -> LoadedC0Artifact:
    """
    artifact をロードし、self-check をすべて通過したものだけ返す。

    self-check 項目 (いずれか失敗で RuntimeError):
      1. npz_path, meta_path 両方存在
      2. meta.json パース成功、必須キー存在
      3. sha256(npz バイト列) == meta["sha256_of_c0_npz"]
      4. meta["us_tickers"] == list(expected_us_tickers) (完全一致)
      5. meta["jp_tickers"] == list(expected_jp_tickers) (完全一致)
      6. meta["c_full_train_start"] == C_FULL_TRAIN_START.isoformat()
      7. meta["c_full_train_end"]   == C_FULL_TRAIN_END.isoformat()
      8. meta["universe_size"] == 28, K0 == 3
      9. C_0.shape == (28, 28), 対称 (atol 1e-10), diag ≈ 1 (atol 1e-8)
     10. V_0.shape == (28, 3), V_0.T @ V_0 ≈ I_3 (atol 1e-8)
     11. np.isfinite(C_0).all(), np.isfinite(V_0).all()
     12. npz キーセット == {"C_0", "V_0", "D_0", "C_full", "us_tickers", "jp_tickers"} (完全一致)
     13. npz["us_tickers"] 配列 == expected_us_tickers (完全一致)
     14. npz["jp_tickers"] 配列 == expected_jp_tickers (完全一致)
     15. npz["us_tickers"] 配列 == meta["us_tickers"] (npz と meta の一致)
     16. npz["jp_tickers"] 配列 == meta["jp_tickers"] (npz と meta の一致)

    Raises:
        RuntimeError: いずれかのチェック失敗。メッセージに失敗項目を明示。
    """
    # ── check 1: ファイル存在確認 ──
    if not npz_path.exists():
        raise RuntimeError(f"[check 1] npz artifact が見つかりません: {npz_path}")
    if not meta_path.exists():
        raise RuntimeError(f"[check 1] meta artifact が見つかりません: {meta_path}")

    # ── check 2: meta.json パース & 必須キー確認 ──
    try:
        meta_text = meta_path.read_text(encoding="utf-8")
        meta = json.loads(meta_text)
    except Exception as exc:
        raise RuntimeError(f"[check 2] meta.json のパースに失敗しました: {exc}") from exc

    missing_keys = [k for k in _REQUIRED_META_KEYS if k not in meta]
    if missing_keys:
        raise RuntimeError(f"[check 2] meta.json に必須キーが不足しています: {missing_keys}")

    # ── check 3: SHA-256 検証 ──
    npz_bytes = npz_path.read_bytes()
    actual_sha256 = hashlib.sha256(npz_bytes).hexdigest()
    expected_sha256 = meta["sha256_of_c0_npz"]
    if actual_sha256 != expected_sha256:
        raise RuntimeError(
            f"[check 3] SHA-256 が一致しません。"
            f" expected={expected_sha256!r}, actual={actual_sha256!r}"
        )

    # ── check 4: us_tickers 完全一致 ──
    if meta["us_tickers"] != list(expected_us_tickers):
        raise RuntimeError(
            f"[check 4] us_tickers が一致しません。"
            f" meta={meta['us_tickers']}, expected={list(expected_us_tickers)}"
        )

    # ── check 5: jp_tickers 完全一致 ──
    if meta["jp_tickers"] != list(expected_jp_tickers):
        raise RuntimeError(
            f"[check 5] jp_tickers が一致しません。"
            f" meta={meta['jp_tickers']}, expected={list(expected_jp_tickers)}"
        )

    # ── check 6: c_full_train_start ──
    if meta["c_full_train_start"] != C_FULL_TRAIN_START.isoformat():
        raise RuntimeError(
            f"[check 6] c_full_train_start が一致しません。"
            f" meta={meta['c_full_train_start']!r},"
            f" expected={C_FULL_TRAIN_START.isoformat()!r}"
        )

    # ── check 7: c_full_train_end ──
    if meta["c_full_train_end"] != C_FULL_TRAIN_END.isoformat():
        raise RuntimeError(
            f"[check 7] c_full_train_end が一致しません。"
            f" meta={meta['c_full_train_end']!r},"
            f" expected={C_FULL_TRAIN_END.isoformat()!r}"
        )

    # ── check 8: universe_size / K0 ──
    if meta["universe_size"] != UNIVERSE_SIZE:
        raise RuntimeError(
            f"[check 8] universe_size が一致しません。"
            f" meta={meta['universe_size']}, expected={UNIVERSE_SIZE}"
        )
    if meta["K0"] != K:
        raise RuntimeError(
            f"[check 8] K0 が一致しません。"
            f" meta={meta['K0']}, expected={K}"
        )

    # ── npz ロード ──
    try:
        npz = np.load(npz_path, allow_pickle=False)
    except Exception as exc:
        raise RuntimeError(f"[npz load] npz のロードに失敗しました: {exc}") from exc

    # ── check 12: npz キーセットの厳密検証 ──
    EXPECTED_NPZ_KEYS = {"C_0", "V_0", "D_0", "C_full", "us_tickers", "jp_tickers"}
    actual_keys = set(npz.files)
    if actual_keys != EXPECTED_NPZ_KEYS:
        raise RuntimeError(
            f"[check 12] npz keys mismatch: "
            f"missing={EXPECTED_NPZ_KEYS - actual_keys}, "
            f"unexpected={actual_keys - EXPECTED_NPZ_KEYS}"
        )

    # npz 内の配列キーを大文字/小文字の両方に対応する
    _KEY_MAP = {
        "c0": ("c0", "C_0"),
        "v0": ("v0", "V_0"),
        "d0": ("d0", "D_0"),
        "c_full": ("c_full", "C_full"),
    }

    def _get_array(name: str) -> np.ndarray | None:
        for key in _KEY_MAP[name]:
            if key in npz:
                return npz[key]
        return None

    required_arrays = ("c0", "v0", "d0", "c_full")
    missing_arrays = [arr for arr in required_arrays if _get_array(arr) is None]
    if missing_arrays:
        raise RuntimeError(f"[npz load] npz に必須配列が不足しています: {missing_arrays}")

    c0: np.ndarray = _get_array("c0")
    v0: np.ndarray = _get_array("v0")
    d0: np.ndarray = _get_array("d0")
    c_full: np.ndarray = _get_array("c_full")

    # ── check 9: C_0 shape / 対称性 / 対角 ──
    expected_shape = (UNIVERSE_SIZE, UNIVERSE_SIZE)
    if c0.shape != expected_shape:
        raise RuntimeError(
            f"[check 9] C_0.shape が一致しません。"
            f" actual={c0.shape}, expected={expected_shape}"
        )
    if not np.allclose(c0, c0.T, atol=1e-10):
        raise RuntimeError("[check 9] C_0 が対称ではありません (atol=1e-10)。")
    diag_c0 = np.diag(c0)
    if not np.allclose(diag_c0, 1.0, atol=1e-8):
        max_dev = float(np.max(np.abs(diag_c0 - 1.0)))
        raise RuntimeError(
            f"[check 9] C_0 の対角が 1 に近くありません (atol=1e-8)。最大偏差: {max_dev:.2e}"
        )

    # ── check 10: V_0 shape / 直交正規性 ──
    expected_v0_shape = (UNIVERSE_SIZE, K)
    if v0.shape != expected_v0_shape:
        raise RuntimeError(
            f"[check 10] V_0.shape が一致しません。"
            f" actual={v0.shape}, expected={expected_v0_shape}"
        )
    vtv = v0.T @ v0
    if not np.allclose(vtv, np.eye(K), atol=1e-8):
        raise RuntimeError(
            f"[check 10] V_0.T @ V_0 が I_{K} に近くありません (atol=1e-8)。"
            f" 最大偏差: {float(np.max(np.abs(vtv - np.eye(K)))):.2e}"
        )

    # ── check 11: finite check ──
    if not np.isfinite(c0).all():
        raise RuntimeError("[check 11] C_0 に NaN または Inf が含まれています。")
    if not np.isfinite(v0).all():
        raise RuntimeError("[check 11] V_0 に NaN または Inf が含まれています。")

    # ── check 13-16: npz 内 ticker 配列の厳密検証 ──
    npz_us_tickers = tuple(str(t) for t in npz["us_tickers"])
    npz_jp_tickers = tuple(str(t) for t in npz["jp_tickers"])

    if npz_us_tickers != tuple(expected_us_tickers):
        raise RuntimeError(
            f"[check 13] npz us_tickers mismatch with expected: "
            f"npz={npz_us_tickers}, expected={tuple(expected_us_tickers)}"
        )
    if npz_jp_tickers != tuple(expected_jp_tickers):
        raise RuntimeError(
            f"[check 14] npz jp_tickers mismatch with expected: "
            f"npz={npz_jp_tickers}, expected={tuple(expected_jp_tickers)}"
        )
    # npz と meta の一致確認 (冗長だが tamper 検出強化)
    if npz_us_tickers != tuple(meta["us_tickers"]):
        raise RuntimeError(
            "[check 15] npz us_tickers differ from meta us_tickers"
        )
    if npz_jp_tickers != tuple(meta["jp_tickers"]):
        raise RuntimeError(
            "[check 16] npz jp_tickers differ from meta jp_tickers"
        )

    us_tickers = tuple(meta["us_tickers"])
    jp_tickers = tuple(meta["jp_tickers"])

    return LoadedC0Artifact(
        c0=c0,
        v0=v0,
        d0=d0,
        c_full=c_full,
        us_tickers=us_tickers,
        jp_tickers=jp_tickers,
        meta=meta,
    )
