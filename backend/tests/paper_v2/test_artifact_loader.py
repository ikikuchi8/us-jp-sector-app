"""
tests/paper_v2/test_artifact_loader.py — load_c0_artifact の単体テスト。

実 artifact (c0_v1.npz + c0_v1.meta.json) を使った正常系と、
改ざんパターンによるエラーケースを網羅する。
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pytest

from app.services.paper_v2.artifact_loader import LoadedC0Artifact, load_c0_artifact
from app.services.paper_v2.constants import (
    C0_ARTIFACT_PATH,
    C0_META_PATH,
    K,
    UNIVERSE_SIZE,
)

# canonical tickers (実 meta.json から取得)
with open(C0_META_PATH, encoding="utf-8") as _f:
    _META = json.load(_f)

_EXPECTED_US_TICKERS: tuple[str, ...] = tuple(_META["us_tickers"])
_EXPECTED_JP_TICKERS: tuple[str, ...] = tuple(_META["jp_tickers"])


def _load_real() -> LoadedC0Artifact:
    """実 artifact をロードして返す。"""
    return load_c0_artifact(
        expected_us_tickers=_EXPECTED_US_TICKERS,
        expected_jp_tickers=_EXPECTED_JP_TICKERS,
    )


def _make_tmp_npz(tmp_path: Path, **overrides: np.ndarray) -> Path:
    """
    実 artifact の配列をベースに特定の配列だけを書き換えた tmp npz を作成する。
    """
    real_npz = np.load(C0_ARTIFACT_PATH, allow_pickle=False)
    arrays = {k: real_npz[k] for k in real_npz.files}
    arrays.update(overrides)
    out_path = tmp_path / "c0_modified.npz"
    np.savez(out_path, **arrays)
    return out_path


def _make_tmp_meta(tmp_path: Path, **overrides) -> Path:
    """実 meta.json をベースに特定のキーだけを書き換えた tmp meta を作成する。"""
    meta = dict(_META)
    meta.update(overrides)
    out_path = tmp_path / "c0_modified.meta.json"
    out_path.write_text(json.dumps(meta), encoding="utf-8")
    return out_path


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestLoadRealArtifact:
    def test_load_real_artifact(self) -> None:
        """実 c0_v1.npz + meta でロード成功し、shape/型が正しいこと。"""
        artifact = _load_real()

        assert isinstance(artifact, LoadedC0Artifact)
        assert artifact.c0.shape == (UNIVERSE_SIZE, UNIVERSE_SIZE)
        assert artifact.v0.shape == (UNIVERSE_SIZE, K)
        assert artifact.d0.shape == (K, K)
        assert artifact.c_full.shape == (UNIVERSE_SIZE, UNIVERSE_SIZE)
        assert isinstance(artifact.us_tickers, tuple)
        assert isinstance(artifact.jp_tickers, tuple)
        assert len(artifact.us_tickers) == 11
        assert len(artifact.jp_tickers) == 17
        assert isinstance(artifact.meta, dict)

    def test_returned_tickers_are_tuples(self) -> None:
        """us_tickers / jp_tickers の型が tuple であること。"""
        artifact = _load_real()
        assert isinstance(artifact.us_tickers, tuple)
        assert isinstance(artifact.jp_tickers, tuple)

    def test_c0_is_symmetric(self) -> None:
        """ロードした C_0 が対称行列であること。"""
        artifact = _load_real()
        np.testing.assert_allclose(artifact.c0, artifact.c0.T, atol=1e-10)

    def test_c0_diagonal_is_one(self) -> None:
        """ロードした C_0 の対角が 1 に近いこと。"""
        artifact = _load_real()
        np.testing.assert_allclose(np.diag(artifact.c0), 1.0, atol=1e-8)

    def test_v0_is_orthonormal(self) -> None:
        """ロードした V_0 が正規直交基底であること。"""
        artifact = _load_real()
        vtv = artifact.v0.T @ artifact.v0
        np.testing.assert_allclose(vtv, np.eye(K), atol=1e-8)


class TestMissingFiles:
    def test_missing_npz_raises(self, tmp_path: Path) -> None:
        """存在しない npz パスを渡すと RuntimeError。"""
        fake_npz = tmp_path / "nonexistent.npz"
        with pytest.raises(RuntimeError, match="check 1"):
            load_c0_artifact(
                npz_path=fake_npz,
                meta_path=C0_META_PATH,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )

    def test_missing_meta_raises(self, tmp_path: Path) -> None:
        """存在しない meta パスを渡すと RuntimeError。"""
        fake_meta = tmp_path / "nonexistent.meta.json"
        with pytest.raises(RuntimeError, match="check 1"):
            load_c0_artifact(
                npz_path=C0_ARTIFACT_PATH,
                meta_path=fake_meta,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )


class TestSHA256Mismatch:
    def test_sha256_mismatch_raises(self, tmp_path: Path) -> None:
        """meta の sha256 を改ざんすると RuntimeError (check 3)。"""
        # 改ざんした meta を tmp に作成
        bad_sha = "0" * 64
        meta_path = _make_tmp_meta(tmp_path, sha256_of_c0_npz=bad_sha)

        with pytest.raises(RuntimeError, match="check 3"):
            load_c0_artifact(
                npz_path=C0_ARTIFACT_PATH,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )


class TestTickerMismatch:
    def test_ticker_mismatch_raises(self) -> None:
        """expected_us_tickers に違う値を渡すと RuntimeError (check 4)。"""
        wrong_us = ("WRONG",) + _EXPECTED_US_TICKERS[1:]
        with pytest.raises(RuntimeError, match="check 4"):
            load_c0_artifact(
                expected_us_tickers=wrong_us,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )

    def test_jp_ticker_mismatch_raises(self) -> None:
        """expected_jp_tickers に違う値を渡すと RuntimeError (check 5)。"""
        wrong_jp = ("WRONG.T",) + _EXPECTED_JP_TICKERS[1:]
        with pytest.raises(RuntimeError, match="check 5"):
            load_c0_artifact(
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=wrong_jp,
            )


class TestDateMismatch:
    def test_c_full_date_mismatch_raises(self, tmp_path: Path) -> None:
        """tmp meta で c_full_train_start を書き換えると RuntimeError (check 6)。"""
        # 正しい sha256 を使いつつ日付だけ書き換える
        sha256 = _sha256_of_file(C0_ARTIFACT_PATH)
        meta_path = _make_tmp_meta(
            tmp_path,
            sha256_of_c0_npz=sha256,
            c_full_train_start="2000-01-01",
        )
        with pytest.raises(RuntimeError, match="check 6"):
            load_c0_artifact(
                npz_path=C0_ARTIFACT_PATH,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )

    def test_c_full_train_end_mismatch_raises(self, tmp_path: Path) -> None:
        """c_full_train_end を書き換えると RuntimeError (check 7)。"""
        sha256 = _sha256_of_file(C0_ARTIFACT_PATH)
        meta_path = _make_tmp_meta(
            tmp_path,
            sha256_of_c0_npz=sha256,
            c_full_train_end="2000-12-31",
        )
        with pytest.raises(RuntimeError, match="check 7"):
            load_c0_artifact(
                npz_path=C0_ARTIFACT_PATH,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )


class TestUniverseSizeMismatch:
    def test_universe_size_mismatch_raises(self, tmp_path: Path) -> None:
        """tmp meta で universe_size を書き換えると RuntimeError (check 8)。"""
        sha256 = _sha256_of_file(C0_ARTIFACT_PATH)
        meta_path = _make_tmp_meta(
            tmp_path,
            sha256_of_c0_npz=sha256,
            universe_size=99,
        )
        with pytest.raises(RuntimeError, match="check 8"):
            load_c0_artifact(
                npz_path=C0_ARTIFACT_PATH,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )


class TestNpzKeyChecks:
    def test_npz_missing_key_raises(self, tmp_path: Path) -> None:
        """npz に必須キー "D_0" がない場合、RuntimeError (check 12)。"""
        real_npz = np.load(C0_ARTIFACT_PATH, allow_pickle=False)
        # D_0 を除いた配列で tmp npz を作成
        arrays = {k: real_npz[k] for k in real_npz.files if k != "D_0"}
        npz_path = tmp_path / "c0_no_d0.npz"
        np.savez(npz_path, **arrays)
        sha256 = _sha256_of_file(npz_path)
        meta_path = _make_tmp_meta(tmp_path, sha256_of_c0_npz=sha256)

        with pytest.raises(RuntimeError, match="check 12"):
            load_c0_artifact(
                npz_path=npz_path,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )

    def test_npz_ticker_array_tampered_raises(self, tmp_path: Path) -> None:
        """npz 内 us_tickers 配列を改変した場合、RuntimeError (check 13)。
        SHA-256 は npz に合わせて更新するが、expected_us_tickers とは不一致。
        """
        real_npz = np.load(C0_ARTIFACT_PATH, allow_pickle=False)
        arrays = {k: real_npz[k] for k in real_npz.files}
        # us_tickers 配列を改ざん (最初の要素を "TAMPERED" に変更)
        original_us = list(str(t) for t in real_npz["us_tickers"])
        tampered_us = ["TAMPERED"] + original_us[1:]
        arrays["us_tickers"] = np.array(tampered_us)
        npz_path = tmp_path / "c0_tampered.npz"
        np.savez(npz_path, **arrays)
        # SHA-256 を tampered npz に合わせて更新 → check 3 はパス
        sha256 = _sha256_of_file(npz_path)
        meta_path = _make_tmp_meta(tmp_path, sha256_of_c0_npz=sha256)

        with pytest.raises(RuntimeError, match="check 13"):
            load_c0_artifact(
                npz_path=npz_path,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )


class TestArrayChecks:
    def test_c0_non_symmetric_raises(self, tmp_path: Path) -> None:
        """tmp npz で C_0 を非対称に書き換えると RuntimeError (check 9)。"""
        real_npz = np.load(C0_ARTIFACT_PATH, allow_pickle=False)
        c0_bad = real_npz["C_0"].copy()
        # 上三角の一部を書き換えて非対称にする
        c0_bad[0, 1] = c0_bad[0, 1] + 0.5
        npz_path = _make_tmp_npz(tmp_path, C_0=c0_bad)
        sha256 = _sha256_of_file(npz_path)
        meta_path = _make_tmp_meta(tmp_path, sha256_of_c0_npz=sha256)

        with pytest.raises(RuntimeError, match="check 9"):
            load_c0_artifact(
                npz_path=npz_path,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )

    def test_v0_non_orthonormal_raises(self, tmp_path: Path) -> None:
        """tmp npz で V_0 を非直交化すると RuntimeError (check 10)。"""
        real_npz = np.load(C0_ARTIFACT_PATH, allow_pickle=False)
        v0_bad = real_npz["V_0"].copy()
        # スケールを崩す (ノルムが 1 でなくなる)
        v0_bad[:, 0] *= 2.0
        npz_path = _make_tmp_npz(tmp_path, V_0=v0_bad)
        sha256 = _sha256_of_file(npz_path)
        meta_path = _make_tmp_meta(tmp_path, sha256_of_c0_npz=sha256)

        with pytest.raises(RuntimeError, match="check 10"):
            load_c0_artifact(
                npz_path=npz_path,
                meta_path=meta_path,
                expected_us_tickers=_EXPECTED_US_TICKERS,
                expected_jp_tickers=_EXPECTED_JP_TICKERS,
            )
