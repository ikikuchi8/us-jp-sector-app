"""
build_paper_v2_prior.py — paper_v2-lite prior artifact ビルドスクリプト

Generates:
  - app/services/paper_v2/data/c0_v1.npz
  - app/services/paper_v2/data/c0_v1.meta.json

Usage:
    cd backend && .venv/bin/python scripts/build_paper_v2_prior.py
    cd backend && .venv/bin/python scripts/build_paper_v2_prior.py --dry-run
    cd backend && .venv/bin/python scripts/build_paper_v2_prior.py --force
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure backend/ is on sys.path so `app.*` imports work when called as a script
# ---------------------------------------------------------------------------
_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

# Change working directory to backend/ so .env is found by pydantic_settings
os.chdir(_BACKEND_DIR)

# ---------------------------------------------------------------------------
# Silence noisy loggers BEFORE importing app modules
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
for _noisy in (
    "sqlalchemy",
    "sqlalchemy.engine",
    "sqlalchemy.engine.Engine",
    "app.services.calendar_service",
    "app.database",
):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

os.environ["APP_DEBUG"] = "false"

logger = logging.getLogger(__name__)

import numpy as np

from app.database import SessionLocal
from app.repositories.price_repository import PriceRepository
from app.seed_data.sector_mapping import ALL_JP_TICKERS, JP_TICKER_TO_US_TICKERS
from app.services.calendar_service import (
    COL_JP_EXECUTION_DATE,
    COL_US_SIGNAL_DATE,
    CalendarService,
)
from app.services.paper_v2.prior_correlation import build_c0_from_returns
from app.services.paper_v2.sector_tags import (
    CYCLICAL_JP,
    CYCLICAL_US,
    DEFENSIVE_JP,
    DEFENSIVE_US,
)

# ---------------------------------------------------------------------------
# v2.2-lite 固定値 (変更禁止)
# ---------------------------------------------------------------------------
C_FULL_TRAIN_START = date(2021, 1, 12)
C_FULL_TRAIN_END = date(2023, 12, 31)
PAPER_V2_OOS_START = date(2024, 1, 4)
UNIVERSE_SIZE = 28
K0 = 3
EXPECTED_C_FULL_ROWS = 729  # 監査値 (±5% を許容)

# ---------------------------------------------------------------------------
# Universe constants (canonical)
# ---------------------------------------------------------------------------
_ALL_US_TICKERS: tuple[str, ...] = tuple(
    sorted({t for lst in JP_TICKER_TO_US_TICKERS.values() for t in lst})
)
_ALL_JP_TICKERS: tuple[str, ...] = tuple(ALL_JP_TICKERS)


def _get_git_sha(repo_root: Path) -> str:
    """git HEAD SHA を取得。dirty なら '{sha}-dirty' を返す。"""
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(repo_root), text=True
        ).strip()
        # check if dirty
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=str(repo_root), text=True
        ).strip()
        if status:
            return f"{sha}-dirty"
        return sha
    except Exception as e:
        logger.warning("git SHA 取得失敗: %s", e)
        return "unknown"


def _build_rcc_matrix(session) -> np.ndarray:
    """
    DB から価格を取得し、complete-case CC リターン行列を構築する。
    列順: _ALL_US_TICKERS (11) + _ALL_JP_TICKERS (17) = 28 列
    Returns:
        rcc_matrix: shape (n_complete_rows, 28), dtype float64
    """
    price_repo = PriceRepository(session)

    fetch_start = C_FULL_TRAIN_START - timedelta(days=10)
    fetch_end = C_FULL_TRAIN_END

    all_tickers = list(_ALL_US_TICKERS) + list(_ALL_JP_TICKERS)
    logger.info("価格データ取得開始: %d tickers, %s .. %s", len(all_tickers), fetch_start, fetch_end)

    # US: {ticker: {business_date: float | None}}
    us_price_map: dict[str, dict[date, float | None]] = {}
    us_sorted_dates: dict[str, list[date]] = {}

    for ticker in _ALL_US_TICKERS:
        prices = price_repo.get_prices_between(ticker, fetch_start, fetch_end)
        date_adj: dict[date, float | None] = {}
        for p in prices:
            val = float(p.adjusted_close_price) if p.adjusted_close_price is not None else None
            date_adj[p.business_date] = val
        us_price_map[ticker] = date_adj
        us_sorted_dates[ticker] = sorted(date_adj.keys())
        logger.info("  US %s: %d rows", ticker, len(prices))

    # JP: {ticker: {business_date: float | None}} (adjusted_close for CC returns)
    jp_price_map: dict[str, dict[date, float | None]] = {}
    jp_sorted_dates: dict[str, list[date]] = {}

    for ticker in _ALL_JP_TICKERS:
        prices = price_repo.get_prices_between(ticker, fetch_start, fetch_end)
        date_close: dict[date, float | None] = {}
        for p in prices:
            val = float(p.adjusted_close_price) if p.adjusted_close_price is not None else None
            date_close[p.business_date] = val
        jp_price_map[ticker] = date_close
        jp_sorted_dates[ticker] = sorted(date_close.keys())
        logger.info("  JP %s: %d rows", ticker, len(prices))

    # CalendarService: JP 営業日軸でアライメントを取得
    logger.info("CalendarService 構築中...")
    calendar = CalendarService(cache_start="2020-01-01")
    alignment = calendar.build_date_alignment(C_FULL_TRAIN_START, C_FULL_TRAIN_END)
    logger.info("アライメント行数 (%s .. %s): %d", C_FULL_TRAIN_START, C_FULL_TRAIN_END, len(alignment))

    # rcc_matrix 構築 (complete-case)
    rows: list[list[float]] = []
    skipped = 0

    for _, aln_row in alignment.iterrows():
        us_date: date = aln_row[COL_US_SIGNAL_DATE]
        jp_date: date = aln_row[COL_JP_EXECUTION_DATE]

        row_vals: list[float] = []
        skip = False

        # US CC returns: adj_close[us_date] / adj_close[prev_us_biz] - 1
        for ticker in _ALL_US_TICKERS:
            dates = us_sorted_dates.get(ticker, [])
            adj_map = us_price_map.get(ticker, {})

            idx = bisect.bisect_right(dates, us_date) - 1
            if idx < 1 or dates[idx] != us_date:
                skip = True
                break

            curr_adj = adj_map.get(us_date)
            prev_adj = adj_map.get(dates[idx - 1])

            if curr_adj is None or prev_adj is None or prev_adj == 0.0:
                skip = True
                break

            row_vals.append((curr_adj - prev_adj) / prev_adj)

        if skip:
            skipped += 1
            continue

        # JP CC returns: adj_close[jp_date] / adj_close[prev_jp_biz] - 1
        for ticker in _ALL_JP_TICKERS:
            dates = jp_sorted_dates.get(ticker, [])
            adj_map = jp_price_map.get(ticker, {})

            idx = bisect.bisect_right(dates, jp_date) - 1
            if idx < 1 or dates[idx] != jp_date:
                skip = True
                break

            curr_adj = adj_map.get(jp_date)
            prev_adj = adj_map.get(dates[idx - 1])

            if curr_adj is None or prev_adj is None or prev_adj == 0.0:
                skip = True
                break

            row_vals.append((curr_adj - prev_adj) / prev_adj)

        if skip:
            skipped += 1
            continue

        rows.append(row_vals)

    actual_rows = len(rows)
    logger.info(
        "Complete-case 行数: %d (スキップ: %d / アライメント総数: %d)",
        actual_rows, skipped, len(alignment)
    )

    # ±5% チェック
    lower = EXPECTED_C_FULL_ROWS * 0.95
    upper = EXPECTED_C_FULL_ROWS * 1.05
    if not (lower <= actual_rows <= upper):
        logger.warning(
            "actual_c_full_rows=%d が期待値 %d ±5%% (%.0f .. %.0f) の範囲外です",
            actual_rows, EXPECTED_C_FULL_ROWS, lower, upper
        )

    if actual_rows == 0:
        raise ValueError("Complete-case 行数が 0 です。DB データを確認してください。")

    return np.array(rows, dtype=np.float64)


def _sanity_check(artifact, actual_c_full_rows: int | None = None) -> None:
    """サニティチェック。失敗時は AssertionError または ValueError を raise。

    モジュールトップレベルの関数として定義し、テストから直接 import 可能。

    Args:
        artifact: C0Artifact インスタンス
        actual_c_full_rows: complete-case 行数。指定した場合は ±5% の両側境界チェックを実施。
    """
    assert artifact.c0.shape == (28, 28), f"c0.shape={artifact.c0.shape} != (28, 28)"
    assert np.allclose(artifact.c0, artifact.c0.T, atol=1e-10), "c0 が対称でありません"
    assert np.allclose(np.diag(artifact.c0), 1.0, atol=1e-8), (
        f"c0 の対角が 1 でありません: max_err={np.max(np.abs(np.diag(artifact.c0) - 1.0)):.2e}"
    )
    assert artifact.v0.shape == (28, 3), f"v0.shape={artifact.v0.shape} != (28, 3)"
    assert np.allclose(artifact.v0.T @ artifact.v0, np.eye(3), atol=1e-8), (
        "v0 が正規直交でありません"
    )
    assert artifact.d0.shape == (3, 3), f"d0.shape={artifact.d0.shape} != (3, 3)"
    # d0 は対角行列: off-diagonal ≈ 0
    d0_offdiag = artifact.d0 - np.diag(np.diag(artifact.d0))
    assert np.allclose(d0_offdiag, 0.0, atol=1e-10), (
        f"d0 の非対角成分が非 0: max={np.max(np.abs(d0_offdiag)):.2e}"
    )

    # actual_c_full_rows の両側 ±5% 境界チェック
    if actual_c_full_rows is not None:
        expected_low = EXPECTED_C_FULL_ROWS * 0.95
        expected_high = EXPECTED_C_FULL_ROWS * 1.05
        actual = actual_c_full_rows
        if not (expected_low <= actual <= expected_high):
            raise ValueError(
                f"actual_c_full_rows {actual} outside expected band "
                f"[{expected_low:.1f}, {expected_high:.1f}]"
            )

    logger.info("サニティチェック: 全 pass")


def main() -> None:
    t_start = time.time()

    parser = argparse.ArgumentParser(description="paper_v2-lite prior artifact ビルド")
    parser.add_argument(
        "--output-npz",
        default="app/services/paper_v2/data/c0_v1.npz",
        help="npz 出力パス (デフォルト: app/services/paper_v2/data/c0_v1.npz)",
    )
    parser.add_argument(
        "--output-meta",
        default="app/services/paper_v2/data/c0_v1.meta.json",
        help="meta.json 出力パス (デフォルト: app/services/paper_v2/data/c0_v1.meta.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="ファイルを書かずに検証のみ実行",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="既存ファイルを上書き (デフォルトは拒否して exit 1)",
    )
    args = parser.parse_args()

    output_npz = Path(args.output_npz)
    output_meta = Path(args.output_meta)

    # パスを絶対パスに解決 (cwd = backend/)
    if not output_npz.is_absolute():
        output_npz = _BACKEND_DIR / output_npz
    if not output_meta.is_absolute():
        output_meta = _BACKEND_DIR / output_meta

    logger.info("=== build_paper_v2_prior.py 開始 ===")
    logger.info("output_npz:  %s", output_npz)
    logger.info("output_meta: %s", output_meta)
    logger.info("dry_run: %s", args.dry_run)
    logger.info("force:   %s", args.force)

    # ---------------------------------------------------------------------------
    # Step 0: 出力ファイル既存チェック
    # ---------------------------------------------------------------------------
    if not args.dry_run:
        if output_npz.exists() and not args.force:
            logger.error("出力ファイルが既に存在します: %s  (--force で上書き)", output_npz)
            sys.exit(1)
        if output_meta.exists() and not args.force:
            logger.error("出力ファイルが既に存在します: %s  (--force で上書き)", output_meta)
            sys.exit(1)

    # ---------------------------------------------------------------------------
    # Step 1: Universe アサーション
    # ---------------------------------------------------------------------------
    assert len(_ALL_US_TICKERS) == 11, f"US ticker 数: {len(_ALL_US_TICKERS)} != 11"
    assert len(_ALL_JP_TICKERS) == 17, f"JP ticker 数: {len(_ALL_JP_TICKERS)} != 17"
    logger.info("US tickers (%d): %s", len(_ALL_US_TICKERS), list(_ALL_US_TICKERS))
    logger.info("JP tickers (%d): %s", len(_ALL_JP_TICKERS), list(_ALL_JP_TICKERS))

    # ---------------------------------------------------------------------------
    # Step 2: DB セッション取得
    # ---------------------------------------------------------------------------
    logger.info("DB 接続中...")
    try:
        session = SessionLocal()
        from sqlalchemy import text
        session.execute(text("SELECT 1")).scalar()
        logger.info("DB 接続 OK")
    except Exception as e:
        logger.error("DB 接続失敗: %s", e)
        sys.exit(1)

    try:
        # ---------------------------------------------------------------------------
        # Step 3 & 4: 価格取得 + rcc_matrix 構築
        # ---------------------------------------------------------------------------
        rcc_matrix = _build_rcc_matrix(session)
        logger.info("rcc_matrix.shape: %s", rcc_matrix.shape)

        # ---------------------------------------------------------------------------
        # Step 5: build_c0_from_returns
        # ---------------------------------------------------------------------------
        logger.info("build_c0_from_returns 実行中...")
        artifact = build_c0_from_returns(
            rcc_matrix,
            _ALL_US_TICKERS,
            _ALL_JP_TICKERS,
            C_FULL_TRAIN_START,
            C_FULL_TRAIN_END,
        )
        logger.info("C0Artifact 生成完了: effective_rows=%d", artifact.effective_rows)

        # ---------------------------------------------------------------------------
        # Step 6: サニティチェック
        # ---------------------------------------------------------------------------
        try:
            _sanity_check(artifact, actual_c_full_rows=artifact.effective_rows)
        except (AssertionError, ValueError) as e:
            logger.error("サニティチェック失敗: %s", e)
            sys.exit(1)

        # ---------------------------------------------------------------------------
        # Step 9: top3 固有値
        # ---------------------------------------------------------------------------
        eigvals = np.linalg.eigvalsh(artifact.c_full)  # 昇順
        top3 = sorted(eigvals.tolist(), reverse=True)[:3]
        logger.info("top3 eigenvalues of C_full: %s", [f"{v:.6f}" for v in top3])

        # ---------------------------------------------------------------------------
        # Step 10: git SHA
        # ---------------------------------------------------------------------------
        repo_root = _BACKEND_DIR.parent
        git_sha = _get_git_sha(repo_root)
        logger.info("git SHA: %s", git_sha)

        # ---------------------------------------------------------------------------
        # dry-run: ここで終了
        # ---------------------------------------------------------------------------
        if args.dry_run:
            logger.info("=== dry-run 完了 (ファイル書き出しなし) ===")
            logger.info("actual_c_full_rows:  %d", artifact.effective_rows)
            logger.info("top3 eigenvalues:    %s", top3)
            t_end = time.time()
            logger.info("所要時間: %.2f 秒", t_end - t_start)
            return

        # ---------------------------------------------------------------------------
        # Step 7: npz 書き出し
        # ---------------------------------------------------------------------------
        output_npz.parent.mkdir(parents=True, exist_ok=True)

        np.savez(
            str(output_npz),
            C_0=artifact.c0.astype(np.float64),
            V_0=artifact.v0.astype(np.float64),
            D_0=artifact.d0.astype(np.float64),
            C_full=artifact.c_full.astype(np.float64),
            us_tickers=np.array(list(_ALL_US_TICKERS), dtype="<U8"),
            jp_tickers=np.array(list(_ALL_JP_TICKERS), dtype="<U8"),
        )
        logger.info("npz 書き出し完了: %s", output_npz)

        # ---------------------------------------------------------------------------
        # Step 8: sha256 計算
        # ---------------------------------------------------------------------------
        sha256 = hashlib.sha256(output_npz.read_bytes()).hexdigest()
        logger.info("sha256: %s", sha256)

        # ---------------------------------------------------------------------------
        # Step 11: meta.json 書き出し
        # ---------------------------------------------------------------------------
        built_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        meta = {
            "schema_version": "1.0",
            "artifact_version": "v1",
            "c_full_train_start": str(C_FULL_TRAIN_START),
            "c_full_train_end": str(C_FULL_TRAIN_END),
            "paper_v2_oos_start": str(PAPER_V2_OOS_START),
            "universe_size": UNIVERSE_SIZE,
            "K0": K0,
            "us_tickers": list(_ALL_US_TICKERS),
            "jp_tickers": list(_ALL_JP_TICKERS),
            "cyclical_us": sorted(list(CYCLICAL_US)),
            "defensive_us": sorted(list(DEFENSIVE_US)),
            "cyclical_jp": sorted(list(CYCLICAL_JP)),
            "defensive_jp": sorted(list(DEFENSIVE_JP)),
            "expected_c_full_rows": EXPECTED_C_FULL_ROWS,
            "actual_c_full_rows": artifact.effective_rows,
            "top3_eigenvalues_of_c_full": top3,
            "built_at": built_at,
            "built_from_git_sha": git_sha,
            "sha256_of_c0_npz": sha256,
        }

        output_meta.parent.mkdir(parents=True, exist_ok=True)
        output_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        logger.info("meta.json 書き出し完了: %s", output_meta)

        # ---------------------------------------------------------------------------
        # Step 12: 診断ログ
        # ---------------------------------------------------------------------------
        npz_size = output_npz.stat().st_size
        meta_size = output_meta.stat().st_size
        t_end = time.time()

        print("\n" + "=" * 60)
        print("build_paper_v2_prior.py 完了")
        print("=" * 60)
        print(f"  actual_c_full_rows:          {artifact.effective_rows}")
        print(f"  expected_c_full_rows:         {EXPECTED_C_FULL_ROWS}  (±5%: {int(EXPECTED_C_FULL_ROWS * 0.95)}..{int(EXPECTED_C_FULL_ROWS * 1.05)})")
        print(f"  top3 eigenvalues of C_full:  {[f'{v:.6f}' for v in top3]}")
        print(f"  sha256 (first 16):           {sha256[:16]}...")
        print(f"  sha256 (full):               {sha256}")
        print(f"  git SHA:                     {git_sha}")
        print(f"  npz:  {output_npz}  ({npz_size:,} bytes)")
        print(f"  meta: {output_meta}  ({meta_size:,} bytes)")
        print(f"  所要時間: {t_end - t_start:.2f} 秒")
        print("=" * 60)

        # meta.json の全内容を stdout に出力
        print("\n--- meta.json 全内容 ---")
        print(json.dumps(meta, indent=2))

    finally:
        session.close()


if __name__ == "__main__":
    main()
