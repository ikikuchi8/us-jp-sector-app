"""
run_evaluation_phase.py — paper_v2-lite 評価フェーズ実行スクリプト

3 signal_type (simple_v1 / paper_v1 / paper_v2) のシグナル生成とバックテスト実行を
オーケストレーションする。集計・レポート作成は含まない（Step C の別スクリプト責務）。

Usage:
    cd backend && .venv/bin/python scripts/run_evaluation_phase.py \\
        --start-date 2024-01-04 \\
        --end-date 2026-04-15 \\
        [--output docs/paper_v2/evaluation_runs/run_<YYYYMMDD_HHMMSS>.json] \\
        [--signal-types simple_v1,paper_v1,paper_v2] \\
        [--dry-run] \\
        [--skip-signal-generation] \\
        [--skip-backtest]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
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

# ---------------------------------------------------------------------------
# Valid signal types
# ---------------------------------------------------------------------------
_VALID_SIGNAL_TYPES = {"simple_v1", "paper_v1", "paper_v2"}
_DEFAULT_SIGNAL_TYPES = ["simple_v1", "paper_v1", "paper_v2"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_git_sha(repo_root: Path) -> str:
    """git HEAD SHA を取得。dirty なら '{sha}-dirty' を返す。"""
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(repo_root), text=True
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=str(repo_root), text=True
        ).strip()
        if status:
            return f"{sha}-dirty"
        return sha
    except Exception as e:
        logger.warning("git SHA 取得失敗: %s", e)
        return "unknown"


def _build_signal_service(signal_type: str, session):
    """signal_type に対応するシグナルサービスを構築して返す。"""
    from app.services.calendar_service import CalendarService

    calendar = CalendarService(cache_start="2020-01-01")

    if signal_type == "simple_v1":
        from app.services.signal_service import SignalService, SIGNAL_TYPE_SIMPLE_V1
        return SignalService(
            session=session,
            calendar_service=calendar,
            signal_type=SIGNAL_TYPE_SIMPLE_V1,
        )
    elif signal_type == "paper_v1":
        from app.services.paper_v1_signal_service import PaperV1SignalService
        return PaperV1SignalService(
            session=session,
            calendar_service=calendar,
        )
    elif signal_type == "paper_v2":
        from app.services.paper_v2.signal_service import PaperV2SignalService
        return PaperV2SignalService(
            session=session,
            calendar_service=calendar,
        )
    else:
        raise ValueError(f"未知の signal_type: {signal_type}")


def _signal_result_to_dict(signal_type: str, result, elapsed: float) -> dict:
    """SignalGenerationResult (or PaperV2GenerationResult) を JSON 化する。"""
    succeeded_count = len(result.succeeded)
    failed_count = len(result.failed)
    skipped_count = len(result.skipped)

    d: dict = {
        "requested": result.requested,
        "succeeded_count": succeeded_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "saved_rows": result.saved_rows,
        "skip_reasons_summary": {},
        "elapsed_seconds": round(elapsed, 3),
        "error": None,
    }

    # paper_v2 は skip_reasons を持つ
    if signal_type == "paper_v2" and hasattr(result, "skip_reasons"):
        reason_counter: Counter[str] = Counter()
        for reason_val in result.skip_reasons.values():
            reason_counter[str(reason_val)] += 1
        d["skip_reasons_summary"] = dict(reason_counter)

    return d


def _run_signal_generation(signal_type: str, session, start: date, end: date) -> dict:
    """1 signal_type のシグナル生成を実行し、結果 dict を返す。"""
    logger.info("[%s] シグナル生成 開始 (%s .. %s)", signal_type, start, end)
    t0 = time.time()
    try:
        svc = _build_signal_service(signal_type, session)
        result = svc.generate_signals_for_range(start, end)
        elapsed = time.time() - t0
        d = _signal_result_to_dict(signal_type, result, elapsed)
        logger.info(
            "[%s] シグナル生成 完了: requested=%d succeeded=%d failed=%d skipped=%d saved=%d (%.2fs)",
            signal_type,
            d["requested"],
            d["succeeded_count"],
            d["failed_count"],
            d["skipped_count"],
            d["saved_rows"],
            elapsed,
        )
        return d
    except Exception as exc:
        elapsed = time.time() - t0
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.error("[%s] シグナル生成 失敗: %s", signal_type, err_msg)
        return {
            "requested": 0,
            "succeeded_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "saved_rows": 0,
            "skip_reasons_summary": {},
            "elapsed_seconds": round(elapsed, 3),
            "error": err_msg,
        }


def _run_backtest(signal_type: str, session, start: date, end: date) -> dict:
    """1 signal_type のバックテストを実行し、結果 dict を返す。"""
    from app.services.backtest_service import BacktestService, CostParams
    from app.services.calendar_service import CalendarService

    logger.info("[%s] バックテスト 開始 (%s .. %s)", signal_type, start, end)
    t0 = time.time()
    try:
        calendar = CalendarService(cache_start="2020-01-01")
        cost_params = CostParams(commission_rate=0.0, slippage_rate=0.0)
        svc = BacktestService(session=session, calendar_service=calendar)
        result = svc.run(
            start=start,
            end=end,
            signal_type=signal_type,
            cost_params=cost_params,
        )
        elapsed = time.time() - t0
        d = {
            "run_id": result.run_id,
            "status": "SUCCEEDED",
            "daily_results_count": len(result.daily_results),
            "executed_days": result.trading_days,
            "elapsed_seconds": round(elapsed, 3),
            "error": None,
        }
        logger.info(
            "[%s] バックテスト 完了: run_id=%d trading_days=%d (%.2fs)",
            signal_type,
            result.run_id,
            result.trading_days,
            elapsed,
        )
        return d
    except Exception as exc:
        elapsed = time.time() - t0
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.error("[%s] バックテスト 失敗: %s", signal_type, err_msg)
        return {
            "run_id": None,
            "status": "FAILED",
            "daily_results_count": 0,
            "executed_days": 0,
            "elapsed_seconds": round(elapsed, 3),
            "error": err_msg,
        }


def _get_db_snapshot(session) -> dict:
    """DB スナップショット情報を取得する。"""
    from sqlalchemy import text

    try:
        price_db_row_count = session.execute(
            text("SELECT COUNT(*) FROM price_daily")
        ).scalar() or 0

        price_db_last_date_raw = session.execute(
            text("SELECT MAX(business_date) FROM price_daily")
        ).scalar()
        price_db_last_date = str(price_db_last_date_raw) if price_db_last_date_raw else None

        # 28 tickers の MIN(MAX(business_date)) を求める
        # = 全 ticker の最新日付の中で最も古いもの (28 ticker すべてが揃っている最終日)
        latest_common_raw = session.execute(
            text(
                """
                SELECT MIN(last_date)
                FROM (
                    SELECT ticker, MAX(business_date) AS last_date
                    FROM price_daily
                    GROUP BY ticker
                ) sub
                """
            )
        ).scalar()
        latest_common_date_28_tickers = str(latest_common_raw) if latest_common_raw else None

        return {
            "price_db_row_count": int(price_db_row_count),
            "price_db_last_date": price_db_last_date,
            "latest_common_date_28_tickers": latest_common_date_28_tickers,
        }
    except Exception as exc:
        logger.warning("DB スナップショット取得失敗: %s", exc)
        return {
            "price_db_row_count": None,
            "price_db_last_date": None,
            "latest_common_date_28_tickers": None,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="paper_v2-lite 評価フェーズ: シグナル生成 + バックテスト"
    )
    parser.add_argument(
        "--start-date",
        default="2024-01-04",
        help="評価期間開始日 YYYY-MM-DD (デフォルト: 2024-01-04)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="評価期間終了日 YYYY-MM-DD (必須)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="出力 JSON パス (デフォルト: docs/paper_v2/evaluation_runs/run_<YYYYMMDD_HHMMSS>.json)",
    )
    parser.add_argument(
        "--signal-types",
        default=",".join(_DEFAULT_SIGNAL_TYPES),
        help=f"カンマ区切り signal_type リスト (デフォルト: {','.join(_DEFAULT_SIGNAL_TYPES)})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実行計画のみ表示して exit 0 (DB 書き込みなし)",
    )
    parser.add_argument(
        "--skip-signal-generation",
        action="store_true",
        help="シグナル生成をスキップし、バックテストのみ実行する",
    )
    parser.add_argument(
        "--skip-backtest",
        action="store_true",
        help="バックテストをスキップし、シグナル生成のみ実行する",
    )
    return parser.parse_args(argv)


def _resolve_output_path(output_arg: str | None, ts: datetime) -> Path:
    """出力パスを解決する。未指定の場合はタイムスタンプベースのパスを返す。"""
    if output_arg:
        p = Path(output_arg)
    else:
        ts_str = ts.strftime("%Y%m%d_%H%M%S")
        repo_root = _BACKEND_DIR.parent
        p = repo_root / "docs" / "paper_v2" / "evaluation_runs" / f"run_{ts_str}.json"

    if not p.is_absolute():
        p = _BACKEND_DIR.parent / p
    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """エントリポイント。exit code を返す (0=成功, 1=失敗あり)。"""
    args = _parse_args(argv)

    # ---- 引数バリデーション ----
    signal_types = [s.strip() for s in args.signal_types.split(",") if s.strip()]
    for st in signal_types:
        if st not in _VALID_SIGNAL_TYPES:
            logger.error("無効な signal_type: %s (有効値: %s)", st, _VALID_SIGNAL_TYPES)
            return 1

    try:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    except ValueError as e:
        logger.error("日付パースエラー: %s", e)
        return 1

    run_at = datetime.now(timezone.utc)
    output_path = _resolve_output_path(args.output, run_at)

    # ---- dry-run ----
    if args.dry_run:
        print("=" * 60)
        print("run_evaluation_phase.py — dry-run 実行計画")
        print("=" * 60)
        print(f"  start_date:              {start_date}")
        print(f"  end_date:                {end_date}")
        print(f"  signal_types:            {signal_types}")
        print(f"  skip_signal_generation:  {args.skip_signal_generation}")
        print(f"  skip_backtest:           {args.skip_backtest}")
        print(f"  output:                  {output_path}")
        print("  (dry-run: DB 書き込みなし、サービス呼び出しなし)")
        print("=" * 60)
        return 0

    # ---- 本番実行 ----
    logger.info("=== run_evaluation_phase.py 開始 ===")
    logger.info("start_date: %s  end_date: %s", start_date, end_date)
    logger.info("signal_types: %s", signal_types)
    logger.info("output: %s", output_path)

    # git SHA / python version
    repo_root = _BACKEND_DIR.parent
    git_sha = _get_git_sha(repo_root)
    python_version = sys.version.split()[0]

    # 結果コンテナ
    signal_generation_results: dict[str, dict] = {}
    backtest_results: dict[str, dict] = {}
    has_any_error = False

    # ---- DB セッション ----
    from app.database import SessionLocal
    from sqlalchemy import text

    session = SessionLocal()
    try:
        # DB 接続確認
        session.execute(text("SELECT 1")).scalar()
        logger.info("DB 接続 OK")
    except Exception as e:
        logger.error("DB 接続失敗: %s", e)
        session.close()
        return 1

    try:
        # ---- シグナル生成 ----
        if not args.skip_signal_generation:
            for st in signal_types:
                d = _run_signal_generation(st, session, start_date, end_date)
                signal_generation_results[st] = d
                if d.get("error"):
                    has_any_error = True
        else:
            for st in signal_types:
                signal_generation_results[st] = {"status": "skipped"}

        # ---- バックテスト ----
        if not args.skip_backtest:
            for st in signal_types:
                d = _run_backtest(st, session, start_date, end_date)
                backtest_results[st] = d
                if d.get("error"):
                    has_any_error = True
        else:
            for st in signal_types:
                backtest_results[st] = {"status": "skipped"}

        # ---- DB スナップショット ----
        db_snapshot = _get_db_snapshot(session)

    finally:
        session.close()

    # ---- 出力 JSON 組み立て ----
    from app.services.paper_v2.constants import C0_VERSION

    output_json = {
        "run_at": run_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git_sha": git_sha,
        "python_version": python_version,
        "config": {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "cost_params": {"commission_rate": 0.0, "slippage_rate": 0.0},
            "signal_types": signal_types,
        },
        "signal_generation": signal_generation_results,
        "backtest": backtest_results,
        "db_snapshot": db_snapshot,
        "c0_version": C0_VERSION,
    }

    # ---- JSON 書き出し ----
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_json, indent=2, default=str), encoding="utf-8")
    logger.info("JSON 書き出し完了: %s", output_path)

    # ---- stdout サマリー ----
    print("\n" + "=" * 60)
    print("run_evaluation_phase.py 完了")
    print("=" * 60)
    print(f"  run_at:      {output_json['run_at']}")
    print(f"  git_sha:     {git_sha[:12]}...")
    print(f"  start_date:  {start_date}")
    print(f"  end_date:    {end_date}")
    print(f"  output:      {output_path}")
    print()
    print("  シグナル生成:")
    for st in signal_types:
        d = signal_generation_results.get(st, {})
        if d.get("status") == "skipped":
            print(f"    {st}: SKIPPED")
        elif d.get("error"):
            print(f"    {st}: ERROR — {d['error']}")
        else:
            print(
                f"    {st}: succeeded={d.get('succeeded_count')} "
                f"skipped={d.get('skipped_count')} "
                f"failed={d.get('failed_count')} "
                f"saved_rows={d.get('saved_rows')} "
                f"({d.get('elapsed_seconds'):.1f}s)"
            )
    print()
    print("  バックテスト:")
    for st in signal_types:
        d = backtest_results.get(st, {})
        if d.get("status") == "skipped":
            print(f"    {st}: SKIPPED")
        elif d.get("error"):
            print(f"    {st}: ERROR — {d['error']}")
        else:
            print(
                f"    {st}: run_id={d.get('run_id')} "
                f"trading_days={d.get('executed_days')} "
                f"({d.get('elapsed_seconds'):.1f}s)"
            )
    print()
    print(f"  DB snapshot: rows={db_snapshot.get('price_db_row_count')} "
          f"last_date={db_snapshot.get('price_db_last_date')} "
          f"common_28={db_snapshot.get('latest_common_date_28_tickers')}")
    print("=" * 60)

    return 1 if has_any_error else 0


if __name__ == "__main__":
    sys.exit(main())
