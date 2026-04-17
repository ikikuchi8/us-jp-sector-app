"""
build_evaluation_report.py — paper_v2-lite 評価レポート生成スクリプト

Step 2/B (run_evaluation_phase.py) の出力 JSON を入力として、
View A / View B の集計レポートを生成する。

evaluation_spec.md の §5-9 に厳密準拠。

Usage:
    cd backend && .venv/bin/python scripts/build_evaluation_report.py \\
        --input docs/paper_v2/evaluation_runs/run_20260417_130325.json \\
        --output-dir docs/paper_v2/evaluation_reports/report_<YYYYMMDD_HHMMSS>/ \\
        [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure backend/ is on sys.path so `app.*` imports work
# ---------------------------------------------------------------------------
_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

os.chdir(_BACKEND_DIR)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
for _noisy in (
    "sqlalchemy",
    "sqlalchemy.engine",
    "sqlalchemy.engine.Engine",
    "app.database",
):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

os.environ["APP_DEBUG"] = "false"

logger = logging.getLogger(__name__)

_SIGNAL_TYPES = ["simple_v1", "paper_v1", "paper_v2"]

# ---------------------------------------------------------------------------
# Pure metric computation functions (no DB dependency)
# ---------------------------------------------------------------------------


def compute_metrics(r_series: list[float]) -> dict:
    """View A / View B 用のメトリクスを計算する。

    Args:
        r_series: None を含まない float リスト (None は呼び出し前に 0.0 置換済み)

    Returns:
        {"AR": float, "RISK": float, "R_R": float|None, "MDD": float,
         "W_last": float}
    """
    n = len(r_series)
    if n == 0:
        return {"AR": None, "RISK": None, "R_R": None, "MDD": None, "W_last": None}

    # AR = mean(R) * 252
    mean_r = sum(r_series) / n
    ar = mean_r * 252

    # RISK = std(R, ddof=1) * sqrt(252)
    if n == 1:
        risk = 0.0
    else:
        variance = sum((r - mean_r) ** 2 for r in r_series) / (n - 1)
        risk = math.sqrt(variance) * math.sqrt(252)

    # R/R = AR / RISK
    rr = ar / risk if risk != 0 else None

    # Cumulative wealth W_t = prod(1 + R_tau)
    # MDD = min_t { W_t / max_{tau<=t}(W_tau) - 1 }
    w = 1.0
    running_max = 1.0
    mdd = 0.0
    for r in r_series:
        w *= (1.0 + r)
        if w > running_max:
            running_max = w
        dd = w / running_max - 1.0
        if dd < mdd:
            mdd = dd

    return {
        "AR": ar,
        "RISK": risk,
        "R_R": rr,
        "MDD": mdd,
        "W_last": w,
    }


def compute_cumulative_series(
    dated_returns: list[tuple[date, float | None]],
) -> list[tuple[date, float]]:
    """日次リターン系列から累積リターン系列を生成する。

    skip 日 (None) は 0 として carry-over する。

    Args:
        dated_returns: [(date, daily_return|None), ...] 日付昇順

    Returns:
        [(date, W_t - 1), ...] 累積リターン (0 ベース)
    """
    w = 1.0
    result = []
    for d, r in dated_returns:
        r_val = r if r is not None else 0.0
        w *= (1.0 + r_val)
        result.append((d, w - 1.0))
    return result


def compute_view_a(
    dated_returns: list[tuple[date, float | None]],
) -> dict:
    """View A メトリクスを計算する (全期間、skip=0 置換)。

    Args:
        dated_returns: [(date, daily_return|None), ...] 日付昇順

    Returns:
        View A メトリクス dict
    """
    alignment_days = len(dated_returns)
    executed_days = sum(1 for _, r in dated_returns if r is not None)
    skipped_days = alignment_days - executed_days
    skip_rate = skipped_days / alignment_days if alignment_days > 0 else 0.0

    # None -> 0.0 置換
    r_series = [r if r is not None else 0.0 for _, r in dated_returns]

    metrics = compute_metrics(r_series)
    return {
        **metrics,
        "alignment_days": alignment_days,
        "executed_days": executed_days,
        "skipped_days": skipped_days,
        "skip_rate": skip_rate,
    }


def compute_view_b_subset(
    dated_returns_map: dict[str, list[tuple[date, float | None]]],
    subset_types: list[str],
) -> dict:
    """View B サブセットのメトリクスを計算する。

    Args:
        dated_returns_map: {signal_type: [(date, return|None), ...]}
        subset_types: 対象シグナルタイプのリスト (intersection を取る)

    Returns:
        {"days": int, signal_type: metrics_dict, ...}
        空 intersection の場合は {"days": 0, signal_type: None, ...}
    """
    # 各 signal_type の executed_dates (daily_return is not None)
    executed_sets: dict[str, set[date]] = {}
    for st in subset_types:
        rows = dated_returns_map.get(st, [])
        executed_sets[st] = {d for d, r in rows if r is not None}

    # intersection
    if not subset_types:
        return {"days": 0}

    common_dates: set[date] = executed_sets[subset_types[0]]
    for st in subset_types[1:]:
        common_dates = common_dates & executed_sets[st]

    common_sorted = sorted(common_dates)
    n_days = len(common_sorted)

    result: dict = {"days": n_days}

    for st in _SIGNAL_TYPES:
        rows = dated_returns_map.get(st, [])
        date_to_return = {d: r for d, r in rows}

        if n_days == 0:
            result[st] = None
        else:
            # サブセット日のみのリターン系列
            r_series = [
                date_to_return[d] if date_to_return.get(d) is not None else 0.0
                for d in common_sorted
                if d in date_to_return
            ]
            result[st] = compute_metrics(r_series)

    return result


# ---------------------------------------------------------------------------
# DB access layer
# ---------------------------------------------------------------------------


def fetch_daily_results(run_id: int) -> list[tuple[date, float | None]]:
    """DB から daily_results を取得する。

    Args:
        run_id: BacktestRun の id

    Returns:
        [(jp_execution_date, daily_return|None), ...] 日付昇順
    """
    from app.database import SessionLocal
    from app.repositories.backtest_repository import BacktestRepository

    session = SessionLocal()
    try:
        repo = BacktestRepository(session)
        rows = repo.list_daily_results(run_id)
        result = []
        for row in rows:
            dr = float(row.daily_return) if row.daily_return is not None else None
            result.append((row.jp_execution_date, dr))
        return sorted(result, key=lambda x: x[0])
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def _fmt_pct(v: float | None, decimals: int = 2) -> str:
    """float を % 表示に変換する。None は 'N/A'。"""
    if v is None:
        return "N/A"
    return f"{v * 100:.{decimals}f}"


def _fmt_rr(v: float | None) -> str:
    """R/R を小数点 3 桁で表示。None は 'N/A'。"""
    if v is None:
        return "N/A"
    return f"{v:.3f}"


def build_markdown(
    eval_run_json: dict,
    view_a: dict[str, dict],
    view_b: dict[str, dict],
    eval_date_str: str,
    input_json_path: str,
) -> str:
    """Markdown レポートを生成する。

    Args:
        eval_run_json: 入力 JSON 全体
        view_a: {signal_type: view_a_metrics}
        view_b: {"3way": {...}, "simple_v1_paper_v1": {...}, ...}
        eval_date_str: レポート生成日 (YYYY-MM-DD)
        input_json_path: 入力 JSON のパス文字列

    Returns:
        Markdown 文字列
    """
    cfg = eval_run_json.get("config", {})
    bt = eval_run_json.get("backtest", {})
    db_snap = eval_run_json.get("db_snapshot", {})

    run_ids_str = ", ".join(
        f"{st}={bt.get(st, {}).get('run_id', 'N/A')}" for st in _SIGNAL_TYPES
    )

    start_date = cfg.get("start_date", "N/A")
    end_date = cfg.get("end_date", "N/A")
    c0_ver = eval_run_json.get("c0_version", "N/A")
    run_at = eval_run_json.get("run_at", "N/A")
    git_sha = eval_run_json.get("git_sha", "N/A")
    price_rows = db_snap.get("price_db_row_count", "N/A")
    price_last = db_snap.get("price_db_last_date", "N/A")
    cost_params = cfg.get("cost_params", {})

    lines = []

    # -------------------------------------------------------------------------
    # §1 実行メタデータ
    # -------------------------------------------------------------------------
    lines += [
        f"# paper_v2-lite Evaluation Report ({eval_date_str})",
        "",
        "## 1. 実行メタデータ",
        "",
        f"- run_at: {run_at}",
        f"- git_sha: {git_sha}",
        f"- backtest_run_ids: {run_ids_str}",
        f"- DB snapshot: price_daily rows {price_rows}, last_date {price_last}",
        f"- c0_version: {c0_ver}",
        f"- 評価期間: {start_date} 〜 {end_date}",
        f"- コスト: commission={cost_params.get('commission_rate', 0)}, "
        f"slippage={cost_params.get('slippage_rate', 0)}",
        "",
    ]

    # -------------------------------------------------------------------------
    # §2 View A: 全期間サマリー
    # -------------------------------------------------------------------------
    lines += [
        "## 2. View A: 全期間サマリー",
        "",
        "| signal_type | AR (%) | RISK (%) | R/R | MDD (%) | alignment_days | executed_days | skip_rate (%) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for st in _SIGNAL_TYPES:
        m = view_a.get(st, {})
        lines.append(
            f"| {st} | {_fmt_pct(m.get('AR'))} | {_fmt_pct(m.get('RISK'))} | "
            f"{_fmt_rr(m.get('R_R'))} | {_fmt_pct(m.get('MDD'))} | "
            f"{m.get('alignment_days', 'N/A')} | {m.get('executed_days', 'N/A')} | "
            f"{_fmt_pct(m.get('skip_rate'))} |"
        )
    lines.append("")

    # -------------------------------------------------------------------------
    # §3 View B: 共通実行可能日サブセット
    # -------------------------------------------------------------------------
    lines += [
        "## 3. View B: 共通実行可能日サブセット",
        "",
    ]

    # 3-way
    v3 = view_b.get("3way", {})
    n3 = v3.get("days", 0)
    lines += [
        f"### 3.1 3-way intersection ({n3} days)",
        "",
        "| signal_type | AR (%) | RISK (%) | R/R | MDD (%) |",
        "|---|---:|---:|---:|---:|",
    ]
    for st in _SIGNAL_TYPES:
        m = v3.get(st) or {}
        lines.append(
            f"| {st} | {_fmt_pct(m.get('AR'))} | {_fmt_pct(m.get('RISK'))} | "
            f"{_fmt_rr(m.get('R_R'))} | {_fmt_pct(m.get('MDD'))} |"
        )
    lines.append("")

    # Pairwise
    lines += [
        "### 3.2 Pairwise",
        "",
    ]

    pairwise_defs = [
        ("simple_v1_paper_v1", "simple_v1 ∩ paper_v1", ["simple_v1", "paper_v1"]),
        ("simple_v1_paper_v2", "simple_v1 ∩ paper_v2", ["simple_v1", "paper_v2"]),
        ("paper_v1_paper_v2", "paper_v1 ∩ paper_v2", ["paper_v1", "paper_v2"]),
    ]

    for key, label, pair_types in pairwise_defs:
        vp = view_b.get(key, {})
        np_ = vp.get("days", 0)
        lines += [
            f"#### {label} ({np_} days)",
            "",
            "| signal_type | AR (%) | RISK (%) | R/R | MDD (%) |",
            "|---|---:|---:|---:|---:|",
        ]
        for st in pair_types:
            m = vp.get(st) or {}
            lines.append(
                f"| {st} | {_fmt_pct(m.get('AR'))} | {_fmt_pct(m.get('RISK'))} | "
                f"{_fmt_rr(m.get('R_R'))} | {_fmt_pct(m.get('MDD'))} |"
            )
        lines.append("")

    # -------------------------------------------------------------------------
    # §4 スキップ構造
    # -------------------------------------------------------------------------
    lines += [
        "## 4. スキップ構造",
        "",
        "| signal_type | alignment_days | executed_days | skipped_days | skip_rate (%) | skip_reasons_summary |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for st in _SIGNAL_TYPES:
        m = view_a.get(st, {})
        sg = eval_run_json.get("signal_generation", {}).get(st, {})
        skip_reasons = sg.get("skip_reasons_summary", {})
        skip_reasons_str = str(skip_reasons) if skip_reasons else "{}"
        lines.append(
            f"| {st} | {m.get('alignment_days', 'N/A')} | "
            f"{m.get('executed_days', 'N/A')} | "
            f"{m.get('skipped_days', 'N/A')} | "
            f"{_fmt_pct(m.get('skip_rate'))} | "
            f"{skip_reasons_str} |"
        )
    lines.append("")

    # -------------------------------------------------------------------------
    # §5 累積リターン時系列
    # -------------------------------------------------------------------------
    lines += [
        "## 5. 累積リターン時系列",
        "",
        "詳細 CSV: `cumulative_returns.csv`",
        "",
        "最終累積リターン (W_last - 1):",
    ]
    for st in _SIGNAL_TYPES:
        m = view_a.get(st, {})
        w_last = m.get("W_last")
        cum_pct = _fmt_pct(w_last - 1.0 if w_last is not None else None)
        lines.append(f"- {st}: {cum_pct}%")
    lines.append("")

    # -------------------------------------------------------------------------
    # §6 所見 (factual only)
    # -------------------------------------------------------------------------
    lines += [
        "## 6. 所見 (factual only)",
        "",
    ]

    # View A で R/R の最大値
    rr_vals_a = {st: (view_a.get(st) or {}).get("R_R") for st in _SIGNAL_TYPES}
    valid_rr_a = {st: v for st, v in rr_vals_a.items() if v is not None}
    if valid_rr_a:
        best_a = max(valid_rr_a, key=lambda s: valid_rr_a[s])
        lines.append(
            f"- View A で R/R の最大値は `{best_a}` の `{_fmt_rr(valid_rr_a[best_a])}`"
        )
    else:
        lines.append("- View A で R/R の計算値なし")

    # View B 3-way intersection で R/R の最大値
    rr_vals_b = {st: (v3.get(st) or {}).get("R_R") for st in _SIGNAL_TYPES}
    valid_rr_b = {st: v for st, v in rr_vals_b.items() if v is not None}
    if valid_rr_b and n3 > 0:
        best_b = max(valid_rr_b, key=lambda s: valid_rr_b[s])
        lines.append(
            f"- View B 3-way intersection で R/R の最大値は `{best_b}` の `{_fmt_rr(valid_rr_b[best_b])}`"
        )
    else:
        lines.append("- View B 3-way intersection の日数が 0 のため R/R 計算不可")

    # paper_v2 skip 理由
    pv2_sg = eval_run_json.get("signal_generation", {}).get("paper_v2", {})
    pv2_skip_reasons = pv2_sg.get("skip_reasons_summary", {})
    lines.append(
        f"- `paper_v2` の skip 理由は `{pv2_skip_reasons}` "
        f"({'skip 無し' if not pv2_skip_reasons else 'skip あり'})"
    )
    lines.append("")

    # -------------------------------------------------------------------------
    # §7 限界事項
    # -------------------------------------------------------------------------
    alignment = next(
        (view_a[st]["alignment_days"] for st in _SIGNAL_TYPES if view_a.get(st)),
        0,
    )
    years_approx = round(alignment / 252, 1) if alignment > 0 else "N/A"

    lines += [
        "## 7. 限界事項",
        "",
        f"- 評価期間: {alignment} alignment days (年率換算で約 {years_approx} 年のサンプル)",
        f"- regime: {start_date} 以降（lite 版の制約、2010 以降ではない）",
        "- コスト: 0 固定 (実運用前提の簡略化)",
        "- ユニバース: U=28 fixed",
        "",
    ]

    return "\n".join(lines)


def build_cumulative_csv(
    dated_returns_map: dict[str, list[tuple[date, float | None]]],
) -> str:
    """cumulative_returns.csv の内容を生成する。

    skip 日は前日の累積値を carry-over する。
    """
    # 全 signal_type にわたる全日付の union
    all_dates: set[date] = set()
    for rows in dated_returns_map.values():
        all_dates.update(d for d, _ in rows)
    sorted_dates = sorted(all_dates)

    # 各 signal_type の date -> daily_return マップ
    date_to_return: dict[str, dict[date, float | None]] = {}
    for st in _SIGNAL_TYPES:
        date_to_return[st] = {d: r for d, r in dated_returns_map.get(st, [])}

    # 累積計算
    w_state: dict[str, float] = {st: 1.0 for st in _SIGNAL_TYPES}

    csv_lines = ["date,simple_v1,paper_v1,paper_v2"]
    for d in sorted_dates:
        row_vals = []
        for st in _SIGNAL_TYPES:
            r = date_to_return[st].get(d)
            r_val = r if r is not None else 0.0
            w_state[st] *= (1.0 + r_val)
            row_vals.append(f"{w_state[st] - 1.0:.8f}")
        csv_lines.append(f"{d}," + ",".join(row_vals))

    return "\n".join(csv_lines) + "\n"


def build_daily_csv(
    dated_returns_map: dict[str, list[tuple[date, float | None]]],
) -> str:
    """daily_returns.csv の内容を生成する。

    skip 日は空欄 (None) で出力する。
    """
    all_dates: set[date] = set()
    for rows in dated_returns_map.values():
        all_dates.update(d for d, _ in rows)
    sorted_dates = sorted(all_dates)

    date_to_return: dict[str, dict[date, float | None]] = {}
    for st in _SIGNAL_TYPES:
        date_to_return[st] = {d: r for d, r in dated_returns_map.get(st, [])}

    csv_lines = ["date,simple_v1,paper_v1,paper_v2"]
    for d in sorted_dates:
        row_vals = []
        for st in _SIGNAL_TYPES:
            r = date_to_return[st].get(d)
            if r is None:
                row_vals.append("")
            else:
                row_vals.append(f"{r:.8f}")
        csv_lines.append(f"{d}," + ",".join(row_vals))

    return "\n".join(csv_lines) + "\n"


def build_metadata_json(
    eval_run_json: dict,
    view_a: dict[str, dict],
    view_b: dict[str, dict],
    report_at: str,
    report_git_sha: str,
    input_json_path: str,
) -> dict:
    """metadata.json の内容を生成する。"""
    # view_a / view_b からシリアライズ可能な dict を作る
    def _clean(d: dict | None) -> dict | None:
        if d is None:
            return None
        return {k: v for k, v in d.items()}

    view_a_out = {st: _clean(view_a.get(st)) for st in _SIGNAL_TYPES}

    view_b_3way = view_b.get("3way", {})
    view_b_out = {
        "3way": {
            "days": view_b_3way.get("days", 0),
            **{st: _clean(view_b_3way.get(st)) for st in _SIGNAL_TYPES},
        },
        "simple_v1_paper_v1": view_b.get("simple_v1_paper_v1", {}),
        "simple_v1_paper_v2": view_b.get("simple_v1_paper_v2", {}),
        "paper_v1_paper_v2": view_b.get("paper_v1_paper_v2", {}),
    }

    return {
        "report_at": report_at,
        "report_git_sha": report_git_sha,
        "input_run_json": input_json_path,
        "evaluation_run": eval_run_json,
        "view_a": view_a_out,
        "view_b": view_b_out,
    }


# ---------------------------------------------------------------------------
# Git SHA helper
# ---------------------------------------------------------------------------


def _get_git_sha(repo_root: Path) -> str:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(repo_root), text=True
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=str(repo_root), text=True
        ).strip()
        return f"{sha}-dirty" if status else sha
    except Exception as e:
        logger.warning("git SHA 取得失敗: %s", e)
        return "unknown"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="paper_v2-lite 評価レポート生成"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Step 2/B の出力 JSON (run_ids を含む) のパス",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="レポート出力先ディレクトリ (未指定時は入力 JSON のベース名から自動生成)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="計算のみ実行、ファイル書き込みなし、summary を stdout に出力",
    )
    return parser.parse_args(argv)


def _resolve_output_dir(output_arg: str | None, input_path: Path, ts: datetime) -> Path:
    """出力ディレクトリを解決する。"""
    if output_arg:
        p = Path(output_arg)
        if not p.is_absolute():
            p = _BACKEND_DIR.parent / p
        return p

    ts_str = ts.strftime("%Y%m%d_%H%M%S")
    repo_root = _BACKEND_DIR.parent
    return repo_root / "docs" / "paper_v2" / "evaluation_reports" / f"report_{ts_str}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """エントリポイント。exit code を返す (0=成功, 1=失敗)。"""
    # os.chdir(_BACKEND_DIR) は import 時に実行済みなので、
    # 呼び出し元の cwd から絶対パスを復元するには _BACKEND_DIR 経由で辿る
    args = _parse_args(argv)

    # ---- 入力 JSON 読み込み ----
    input_path = Path(args.input)
    if not input_path.is_absolute():
        # スクリプト呼び出し時の cwd は backend/ なので、そこからの相対パスとして解決
        # まず backend/ 基準で試み、見つからなければ repo root 基準でも試みる
        candidate = (_BACKEND_DIR / input_path).resolve()
        if not candidate.exists():
            candidate = (_BACKEND_DIR.parent / input_path).resolve()
        if not candidate.exists():
            # フォールバック: Path.cwd() 基準 (テスト時の monkeypatch 等)
            candidate = Path(args.input).resolve()
        input_path = candidate

    if not input_path.exists():
        logger.error("入力 JSON が見つかりません: %s", input_path)
        return 1

    eval_run_json = json.loads(input_path.read_text(encoding="utf-8"))
    logger.info("入力 JSON 読み込み完了: %s", input_path)

    # ---- run_id 取得 ----
    backtest_info = eval_run_json.get("backtest", {})
    run_ids: dict[str, int] = {}
    for st in _SIGNAL_TYPES:
        bt = backtest_info.get(st, {})
        rid = bt.get("run_id")
        if rid is None:
            logger.error("run_id が見つかりません: signal_type=%s", st)
            return 1
        run_ids[st] = int(rid)

    logger.info("run_ids: %s", run_ids)

    # ---- DB から daily_results 取得 ----
    dated_returns_map: dict[str, list[tuple[date, float | None]]] = {}
    for st, rid in run_ids.items():
        logger.info("[%s] run_id=%d の daily_results を取得中...", st, rid)
        rows = fetch_daily_results(rid)
        dated_returns_map[st] = rows
        logger.info("[%s] %d 行取得", st, len(rows))

    # ---- View A 計算 ----
    view_a: dict[str, dict] = {}
    for st in _SIGNAL_TYPES:
        view_a[st] = compute_view_a(dated_returns_map[st])
        logger.info(
            "[%s] View A: AR=%.4f RISK=%.4f MDD=%.4f",
            st,
            view_a[st]["AR"] or 0,
            view_a[st]["RISK"] or 0,
            view_a[st]["MDD"] or 0,
        )

    # ---- View B 計算 ----
    view_b: dict[str, dict] = {}

    # 3-way intersection (all 3)
    view_b["3way"] = compute_view_b_subset(dated_returns_map, _SIGNAL_TYPES)

    # pairwise
    view_b["simple_v1_paper_v1"] = compute_view_b_subset(
        dated_returns_map, ["simple_v1", "paper_v1"]
    )
    view_b["simple_v1_paper_v2"] = compute_view_b_subset(
        dated_returns_map, ["simple_v1", "paper_v2"]
    )
    view_b["paper_v1_paper_v2"] = compute_view_b_subset(
        dated_returns_map, ["paper_v1", "paper_v2"]
    )

    logger.info("View B 3-way intersection: %d days", view_b["3way"]["days"])

    # ---- レポート生成 ----
    report_at_dt = datetime.now(timezone.utc)
    report_at_str = report_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    eval_date_str = report_at_dt.strftime("%Y-%m-%d")

    repo_root = _BACKEND_DIR.parent
    report_git_sha = _get_git_sha(repo_root)

    md_content = build_markdown(
        eval_run_json=eval_run_json,
        view_a=view_a,
        view_b=view_b,
        eval_date_str=eval_date_str,
        input_json_path=str(input_path),
    )

    cumulative_csv = build_cumulative_csv(dated_returns_map)
    daily_csv = build_daily_csv(dated_returns_map)

    metadata = build_metadata_json(
        eval_run_json=eval_run_json,
        view_a=view_a,
        view_b=view_b,
        report_at=report_at_str,
        report_git_sha=report_git_sha,
        input_json_path=str(input_path),
    )

    # ---- dry-run ----
    if args.dry_run:
        print("=" * 60)
        print("build_evaluation_report.py — dry-run サマリー")
        print("=" * 60)
        print(f"  input:      {input_path}")
        print(f"  report_at:  {report_at_str}")
        print()
        print("  View A:")
        for st in _SIGNAL_TYPES:
            m = view_a[st]
            print(
                f"    {st}: AR={_fmt_pct(m['AR'])}% RISK={_fmt_pct(m['RISK'])}% "
                f"R/R={_fmt_rr(m['R_R'])} MDD={_fmt_pct(m['MDD'])}% "
                f"alignment={m['alignment_days']} executed={m['executed_days']}"
            )
        print()
        print("  View B (3-way):")
        v3 = view_b["3way"]
        print(f"    days={v3['days']}")
        for st in _SIGNAL_TYPES:
            m = v3.get(st) or {}
            print(
                f"    {st}: AR={_fmt_pct(m.get('AR'))}% R/R={_fmt_rr(m.get('R_R'))} "
                f"MDD={_fmt_pct(m.get('MDD'))}%"
            )
        print()
        print("  (dry-run: ファイル書き込みなし)")
        print("=" * 60)
        return 0

    # ---- 出力ディレクトリ決定 ----
    output_dir = _resolve_output_dir(args.output_dir, input_path, report_at_dt)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("出力ディレクトリ: %s", output_dir)

    # ---- ファイル書き出し ----
    (output_dir / "evaluation_report.md").write_text(md_content, encoding="utf-8")
    logger.info("evaluation_report.md 書き出し完了")

    (output_dir / "cumulative_returns.csv").write_text(cumulative_csv, encoding="utf-8")
    logger.info("cumulative_returns.csv 書き出し完了")

    (output_dir / "daily_returns.csv").write_text(daily_csv, encoding="utf-8")
    logger.info("daily_returns.csv 書き出し完了")

    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, default=str), encoding="utf-8"
    )
    logger.info("metadata.json 書き出し完了")

    # ---- stdout サマリー ----
    print("\n" + "=" * 60)
    print("build_evaluation_report.py 完了")
    print("=" * 60)
    print(f"  output_dir: {output_dir}")
    print(f"  report_at:  {report_at_str}")
    print()
    print("  View A:")
    for st in _SIGNAL_TYPES:
        m = view_a[st]
        print(
            f"    {st}: AR={_fmt_pct(m['AR'])}% RISK={_fmt_pct(m['RISK'])}% "
            f"R/R={_fmt_rr(m['R_R'])} MDD={_fmt_pct(m['MDD'])}%"
        )
    print()
    print("  View B (3-way):")
    v3 = view_b["3way"]
    print(f"    days={v3['days']}")
    for st in _SIGNAL_TYPES:
        m = v3.get(st) or {}
        print(
            f"    {st}: AR={_fmt_pct(m.get('AR'))}% R/R={_fmt_rr(m.get('R_R'))} "
            f"MDD={_fmt_pct(m.get('MDD'))}%"
        )
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
