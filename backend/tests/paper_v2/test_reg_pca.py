"""
Tests for paper_v2-lite: fit_reg_pca (subspace-regularized PCA core).

参照: 論文 §3.2 式 (13)–(15), v2.2-lite addendum-1 §5-C
"""

from __future__ import annotations

import numpy as np
import pytest

from app.services.paper_v2.reg_pca import RegPcaResult, fit_reg_pca


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def N():
    return 28


@pytest.fixture
def nu():
    return 11


@pytest.fixture
def k_default():
    return 3


@pytest.fixture
def synth_z_window(N):
    """L=60, N=28 の標準化済み合成リターン行列."""
    rng = np.random.default_rng(42)
    r = rng.standard_normal((60, N))
    # 列ごと標準化
    z = (r - r.mean(axis=0)) / r.std(axis=0, ddof=1)
    return z


@pytest.fixture
def synth_c0(N):
    """対称 positive semi-definite な合成 C_0 (diag=1)."""
    rng = np.random.default_rng(7)
    A = rng.standard_normal((N, 3))
    C_raw = A @ A.T
    D = np.sqrt(np.diag(C_raw))
    C = C_raw / np.outer(D, D)
    np.fill_diagonal(C, 1.0)
    return (C + C.T) / 2


@pytest.fixture
def real_c0():
    """実 artifact c0_v1.npz から C_0 をロード (統合感のある test)."""
    from pathlib import Path

    npz_path = (
        Path(__file__).resolve().parents[2]
        / "app/services/paper_v2/data/c0_v1.npz"
    )
    if not npz_path.exists():
        pytest.skip(f"artifact not found: {npz_path}")
    data = np.load(npz_path)
    return data["C_0"]


# ---------------------------------------------------------------------------
# Helper: call fit_reg_pca with default args
# ---------------------------------------------------------------------------


def _default_result(synth_z_window, synth_c0, lam=0.5, k=3, n_us=11):
    return fit_reg_pca(synth_z_window, synth_c0, lam=lam, k=k, n_us=n_us)


# ---------------------------------------------------------------------------
# Test 1: shapes and dtypes
# ---------------------------------------------------------------------------


def test_result_shapes_and_dtypes(synth_z_window, synth_c0, N, nu, k_default):
    result = fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=k_default, n_us=nu)

    assert result.V_U.shape == (nu, k_default), f"V_U shape: {result.V_U.shape}"
    assert result.V_J.shape == (N - nu, k_default), f"V_J shape: {result.V_J.shape}"
    assert result.V_K.shape == (N, k_default), f"V_K shape: {result.V_K.shape}"
    assert result.c_t.shape == (N, N), f"c_t shape: {result.c_t.shape}"
    assert result.c_reg.shape == (N, N), f"c_reg shape: {result.c_reg.shape}"
    assert result.top_k_eigenvalues.shape == (k_default,), (
        f"top_k_eigenvalues shape: {result.top_k_eigenvalues.shape}"
    )
    assert isinstance(result.condition_number, float)

    # all float64
    for name, arr in [
        ("V_U", result.V_U),
        ("V_J", result.V_J),
        ("V_K", result.V_K),
        ("c_t", result.c_t),
        ("c_reg", result.c_reg),
        ("top_k_eigenvalues", result.top_k_eigenvalues),
    ]:
        assert arr.dtype == np.float64, f"{name} dtype: {arr.dtype}"


# ---------------------------------------------------------------------------
# Test 2: V_K = vstack(V_U, V_J)
# ---------------------------------------------------------------------------


def test_V_K_is_concat_of_V_U_V_J(synth_z_window, synth_c0):
    result = _default_result(synth_z_window, synth_c0)
    reconstructed = np.vstack([result.V_U, result.V_J])
    assert np.array_equal(reconstructed, result.V_K), (
        "V_K should equal vstack([V_U, V_J])"
    )


# ---------------------------------------------------------------------------
# Test 3: V_K orthonormal
# ---------------------------------------------------------------------------


def test_V_K_orthonormal(synth_z_window, synth_c0, k_default):
    result = _default_result(synth_z_window, synth_c0, k=k_default)
    gram = result.V_K.T @ result.V_K
    np.testing.assert_allclose(
        gram, np.eye(k_default), atol=1e-10,
        err_msg="V_K.T @ V_K should be identity (orthonormal columns)"
    )


# ---------------------------------------------------------------------------
# Test 4: top_k_eigenvalues descending
# ---------------------------------------------------------------------------


def test_top_k_eigenvalues_descending(synth_z_window, synth_c0):
    result = _default_result(synth_z_window, synth_c0)
    evals = result.top_k_eigenvalues
    for i in range(len(evals) - 1):
        assert evals[i] >= evals[i + 1], (
            f"top_k_eigenvalues not descending at index {i}: {evals}"
        )


# ---------------------------------------------------------------------------
# Test 5: c_reg symmetric
# ---------------------------------------------------------------------------


def test_c_reg_symmetric(synth_z_window, synth_c0):
    result = _default_result(synth_z_window, synth_c0)
    np.testing.assert_allclose(
        result.c_reg, result.c_reg.T, atol=1e-10,
        err_msg="c_reg should be symmetric"
    )


# ---------------------------------------------------------------------------
# Test 6: lambda=0 matches plain PCA (projection matrix equivalence)
# ---------------------------------------------------------------------------


def test_lambda_0_matches_plain_pca(synth_z_window, synth_c0):
    k = 3
    n_us = 11

    # Reference: direct eigh of symmetrized corrcoef
    c_t_ref = np.corrcoef(synth_z_window.T)
    evals_ref, evecs_ref = np.linalg.eigh((c_t_ref + c_t_ref.T) / 2)
    idx_ref = np.argsort(evals_ref)[::-1][:k]
    V_ref = evecs_ref[:, idx_ref]

    result = fit_reg_pca(synth_z_window, synth_c0, lam=0.0, k=k, n_us=n_us)

    # Projection matrix equivalence (sign ambiguity absorbed)
    P_result = result.V_K @ result.V_K.T
    P_ref = V_ref @ V_ref.T
    frob_diff = np.linalg.norm(P_result - P_ref, ord="fro")
    assert frob_diff < 1e-8, (
        f"lam=0 should match plain PCA (projection). Frobenius diff: {frob_diff}"
    )


# ---------------------------------------------------------------------------
# Test 7: lambda=1 matches C_0 eigendecomp
# ---------------------------------------------------------------------------


def test_lambda_1_matches_c0_eigendecomp(synth_z_window, synth_c0):
    k = 3
    n_us = 11

    # Reference: eigh of symmetrized c_0
    evals_ref, evecs_ref = np.linalg.eigh((synth_c0 + synth_c0.T) / 2)
    idx_ref = np.argsort(evals_ref)[::-1][:k]
    V_ref = evecs_ref[:, idx_ref]

    result = fit_reg_pca(synth_z_window, synth_c0, lam=1.0, k=k, n_us=n_us)

    # Projection matrix equivalence
    P_result = result.V_K @ result.V_K.T
    P_ref = V_ref @ V_ref.T
    frob_diff = np.linalg.norm(P_result - P_ref, ord="fro")
    assert frob_diff < 1e-8, (
        f"lam=1 should match C_0 eigendecomp (projection). Frobenius diff: {frob_diff}"
    )


# ---------------------------------------------------------------------------
# Test 8: principal angles lambda=0 (alternative path)
# ---------------------------------------------------------------------------


def test_principal_angles_lambda_0(synth_z_window, synth_c0):
    k = 3
    n_us = 11

    # Reference subspace
    c_t_ref = np.corrcoef(synth_z_window.T)
    evals_ref, evecs_ref = np.linalg.eigh((c_t_ref + c_t_ref.T) / 2)
    idx_ref = np.argsort(evals_ref)[::-1][:k]
    V_ref = evecs_ref[:, idx_ref]

    result = fit_reg_pca(synth_z_window, synth_c0, lam=0.0, k=k, n_us=n_us)

    # SVD of V_K.T @ V_ref → singular values = cos(principal angles)
    sigma = np.linalg.svdvals(result.V_K.T @ V_ref)
    angles = np.arccos(np.clip(sigma, 0, 1))
    max_angle = np.max(angles)
    assert max_angle < 1e-4, (
        f"Principal angles too large for lam=0: max angle = {max_angle:.2e} rad"
    )


# ---------------------------------------------------------------------------
# Test 9: intermediate lambda
# ---------------------------------------------------------------------------


def test_intermediate_lambda(synth_z_window, synth_c0):
    lam = 0.7
    k = 3
    n_us = 11

    # Reference: manually compute C_reg and eigh
    c_reg_ref = 0.3 * np.corrcoef(synth_z_window.T) + 0.7 * synth_c0
    c_reg_ref = (c_reg_ref + c_reg_ref.T) / 2
    evals_ref, evecs_ref = np.linalg.eigh(c_reg_ref)
    idx_ref = np.argsort(evals_ref)[::-1][:k]
    V_ref = evecs_ref[:, idx_ref]

    result = fit_reg_pca(synth_z_window, synth_c0, lam=lam, k=k, n_us=n_us)

    P_result = result.V_K @ result.V_K.T
    P_ref = V_ref @ V_ref.T
    frob_diff = np.linalg.norm(P_result - P_ref, ord="fro")
    assert frob_diff < 1e-8, (
        f"lam=0.7 intermediate shrinkage mismatch. Frobenius diff: {frob_diff}"
    )


# ---------------------------------------------------------------------------
# Test 10: real C_0 artifact integration
# ---------------------------------------------------------------------------


def test_real_c0_integration(synth_z_window, real_c0, N):
    """実 artifact c0_v1.npz との統合テスト。artifact がなければ skip。"""
    result = fit_reg_pca(synth_z_window, real_c0, lam=0.9, k=3, n_us=11)

    assert result.V_K.shape == (N, 3)
    assert result.V_U.shape == (11, 3)
    assert result.V_J.shape == (N - 11, 3)
    np.testing.assert_allclose(result.c_reg, result.c_reg.T, atol=1e-10)
    assert result.condition_number > 0


# ---------------------------------------------------------------------------
# Test 11: deterministic
# ---------------------------------------------------------------------------


def test_deterministic(synth_z_window, synth_c0):
    """同じ入力で 2 回呼んで全配列一致することを確認。"""
    r1 = fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=3, n_us=11)
    r2 = fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=3, n_us=11)

    assert np.array_equal(r1.V_U, r2.V_U), "V_U not deterministic"
    assert np.array_equal(r1.V_J, r2.V_J), "V_J not deterministic"
    assert np.array_equal(r1.V_K, r2.V_K), "V_K not deterministic"
    assert np.array_equal(r1.c_t, r2.c_t), "c_t not deterministic"
    assert np.array_equal(r1.c_reg, r2.c_reg), "c_reg not deterministic"
    assert np.array_equal(r1.top_k_eigenvalues, r2.top_k_eigenvalues), (
        "top_k_eigenvalues not deterministic"
    )
    assert r1.condition_number == r2.condition_number, "condition_number not deterministic"


# ---------------------------------------------------------------------------
# Test 12: input validation
# ---------------------------------------------------------------------------


def test_input_validation(synth_z_window, synth_c0, N):
    # z_window 1D → ValueError
    with pytest.raises(ValueError, match="2D"):
        fit_reg_pca(synth_z_window[0], synth_c0, lam=0.5, k=3, n_us=11)

    # z_window.shape[1] != c_0.shape[0] → ValueError (c_0 is (N, N) but z_window has N cols)
    # Use a z_window with different N
    z_wrong = synth_z_window[:, :10]  # (60, 10)
    with pytest.raises(ValueError):
        fit_reg_pca(z_wrong, synth_c0, lam=0.5, k=3, n_us=9)

    # c_0 非対称 → ValueError
    c0_asym = synth_c0.copy()
    c0_asym[0, 1] += 0.5
    with pytest.raises(ValueError, match="対称"):
        fit_reg_pca(synth_z_window, c0_asym, lam=0.5, k=3, n_us=11)

    # lam 範囲外
    with pytest.raises(ValueError, match="lam"):
        fit_reg_pca(synth_z_window, synth_c0, lam=-0.1, k=3, n_us=11)
    with pytest.raises(ValueError, match="lam"):
        fit_reg_pca(synth_z_window, synth_c0, lam=1.1, k=3, n_us=11)

    # k = 0 → ValueError
    with pytest.raises(ValueError, match="k"):
        fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=0, n_us=11)

    # k > N → ValueError
    with pytest.raises(ValueError, match="k"):
        fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=N + 1, n_us=11)

    # n_us = 0 → ValueError
    with pytest.raises(ValueError, match="n_us"):
        fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=3, n_us=0)

    # n_us >= N → ValueError
    with pytest.raises(ValueError, match="n_us"):
        fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=3, n_us=N)

    # z_window に NaN → ValueError
    z_nan = synth_z_window.copy()
    z_nan[0, 0] = np.nan
    with pytest.raises(ValueError, match="NaN"):
        fit_reg_pca(z_nan, synth_c0, lam=0.5, k=3, n_us=11)

    # c_0 に NaN → ValueError
    c0_nan = synth_c0.copy()
    c0_nan[0, 0] = np.nan
    with pytest.raises(ValueError, match="NaN"):
        fit_reg_pca(synth_z_window, c0_nan, lam=0.5, k=3, n_us=11)


# ---------------------------------------------------------------------------
# Test 13: zero-variance column raises (Codex P1 継承)
# ---------------------------------------------------------------------------


def test_zero_variance_column_raises(synth_z_window, synth_c0):
    z = synth_z_window.copy()
    z[:, 5] = 0.0  # 定数列 → ゼロ分散
    with pytest.raises(ValueError) as exc_info:
        fit_reg_pca(z, synth_c0, 0.5, 3, 11)
    err_msg = str(exc_info.value)
    assert "5" in err_msg or "zero-variance" in err_msg.lower(), (
        f"Error message should mention column index 5 or 'zero-variance': {err_msg!r}"
    )


# ---------------------------------------------------------------------------
# Test 14: condition_number positive
# ---------------------------------------------------------------------------


def test_condition_number_positive(synth_z_window, synth_c0):
    result = _default_result(synth_z_window, synth_c0)
    assert result.condition_number > 0, (
        f"condition_number should be positive, got {result.condition_number}"
    )


# ---------------------------------------------------------------------------
# Test 15: c_t is actually the sample correlation (lam=0 uses c_t unchanged)
# ---------------------------------------------------------------------------


def test_c_t_matches_corrcoef(synth_z_window, synth_c0):
    """c_t が np.corrcoef(z_window.T) と一致することを確認。"""
    result = fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=3, n_us=11)
    c_t_ref = np.corrcoef(synth_z_window.T)
    np.testing.assert_allclose(result.c_t, c_t_ref, atol=1e-12)


# ---------------------------------------------------------------------------
# Test 16: varying k values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("k", [1, 2, 5, 10])
def test_various_k_values(synth_z_window, synth_c0, N, nu, k):
    result = fit_reg_pca(synth_z_window, synth_c0, lam=0.5, k=k, n_us=nu)
    assert result.V_K.shape == (N, k)
    assert result.V_U.shape == (nu, k)
    assert result.V_J.shape == (N - nu, k)
    assert result.top_k_eigenvalues.shape == (k,)
    gram = result.V_K.T @ result.V_K
    np.testing.assert_allclose(gram, np.eye(k), atol=1e-10)
