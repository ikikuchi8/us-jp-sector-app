"""
tests/paper_v2/test_build_evaluation_report.py — build_evaluation_report.py のユニットテスト。

# テスト方針
  - 実 DB を使わず pure function テスト中心
  - compute_metrics / compute_view_a / compute_view_b_subset は DB 非依存
  - Markdown / CSV / metadata.json の生成も pure function テスト
  - --dry-run の動作は tmp_path を使ってファイル非生成を確認

# DB 依存テスト
  - 今回はなし (fetch_daily_results は DB アクセス層で分離済み)
"""

from __future__ import annotations

import json
import math
import os
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path 設定: backend/ を追加して scripts/ の import を可能にする
# ---------------------------------------------------------------------------
_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

# ---------------------------------------------------------------------------
# テスト対象モジュールの import
# ---------------------------------------------------------------------------
from scripts.build_evaluation_report import (
    build_cumulative_csv,
    build_daily_csv,
    build_markdown,
    build_metadata_json,
    compute_metrics,
    compute_view_a,
    compute_view_b_subset,
    main,
)

# ---------------------------------------------------------------------------
# 共通フィクスチャ
# ---------------------------------------------------------------------------

_KNOWN_RETURNS = [0.01, -0.005, 0.02, 0.0, -0.01]

# 手計算済み期待値 (test_view_a_metrics_known_values で使用)
_EXPECTED_AR = 0.756
_EXPECTED_RISK = 0.19115438786488792
_EXPECTED_RR = 3.95491836961837
_EXPECTED_MDD = -0.010000000000000009


def _make_dated_returns(
    returns: list[float | None],
    start: date = date(2024, 1, 4),
) -> list[tuple[date, float | None]]:
    """テスト用に日付付きリターン系列を生成する。"""
    from datetime import timedelta

    result = []
    d = start
    for r in returns:
        result.append((d, r))
        d = d + timedelta(days=1)
    return result


def _make_minimal_eval_run_json() -> dict:
    """最小限の evaluation run JSON を返す。"""
    return {
        "run_at": "2026-04-17T13:03:25Z",
        "git_sha": "abc123",
        "python_version": "3.11.15",
        "config": {
            "start_date": "2024-01-04",
            "end_date": "2026-04-15",
            "cost_params": {"commission_rate": 0.0, "slippage_rate": 0.0},
            "signal_types": ["simple_v1", "paper_v1", "paper_v2"],
        },
        "signal_generation": {
            "simple_v1": {"skip_reasons_summary": {}},
            "paper_v1": {"skip_reasons_summary": {}},
            "paper_v2": {"skip_reasons_summary": {}},
        },
        "backtest": {
            "simple_v1": {"run_id": 54},
            "paper_v1": {"run_id": 55},
            "paper_v2": {"run_id": 56},
        },
        "db_snapshot": {
            "price_db_row_count": 36438,
            "price_db_last_date": "2026-04-17",
        },
        "c0_version": "v1",
    }


# ---------------------------------------------------------------------------
# テスト 1: View A メトリクスの数値一致 (手計算値との一致)
# ---------------------------------------------------------------------------


def test_view_a_metrics_known_values():
    """合成 daily_return [0.01, -0.005, 0.02, 0, -0.01] で AR / RISK / R/R / MDD が手計算値と一致する。"""
    m = compute_metrics(_KNOWN_RETURNS)

    assert m["AR"] == pytest.approx(_EXPECTED_AR, abs=1e-8), f"AR mismatch: {m['AR']}"
    assert m["RISK"] == pytest.approx(_EXPECTED_RISK, abs=1e-8), f"RISK mismatch: {m['RISK']}"
    assert m["R_R"] == pytest.approx(_EXPECTED_RR, abs=1e-8), f"R/R mismatch: {m['R_R']}"
    assert m["MDD"] == pytest.approx(_EXPECTED_MDD, abs=1e-8), f"MDD mismatch: {m['MDD']}"


# ---------------------------------------------------------------------------
# テスト 2: None を含む列で 0 置換が正しく動作する
# ---------------------------------------------------------------------------


def test_view_a_none_treated_as_zero():
    """None を含む列で、None を 0 として扱った場合の AR / RISK / MDD が正しく計算される。"""
    # None を含む列 (None は 0 置換)
    returns_with_none = [0.01, None, 0.02, None, -0.01]
    dated = _make_dated_returns(returns_with_none)

    result = compute_view_a(dated)

    # None は 0 置換: [0.01, 0.0, 0.02, 0.0, -0.01] で計算される
    expected = compute_metrics([0.01, 0.0, 0.02, 0.0, -0.01])

    assert result["AR"] == pytest.approx(expected["AR"], abs=1e-8)
    assert result["RISK"] == pytest.approx(expected["RISK"], abs=1e-8)
    assert result["MDD"] == pytest.approx(expected["MDD"], abs=1e-8)

    # alignment_days / executed_days / skipped_days の確認
    assert result["alignment_days"] == 5
    assert result["executed_days"] == 3
    assert result["skipped_days"] == 2
    assert result["skip_rate"] == pytest.approx(2 / 5, abs=1e-10)


# ---------------------------------------------------------------------------
# テスト 3: ランダムリターンでも MDD <= 0
# ---------------------------------------------------------------------------


def test_mdd_is_non_positive():
    """ランダムなリターン系列でも MDD は常に 0 以下。"""
    import random

    random.seed(42)
    returns = [random.gauss(0.001, 0.01) for _ in range(200)]
    m = compute_metrics(returns)

    assert m["MDD"] is not None
    assert m["MDD"] <= 0.0, f"MDD が正: {m['MDD']}"


# ---------------------------------------------------------------------------
# テスト 4: 全正リターンで MDD ≈ 0
# ---------------------------------------------------------------------------


def test_mdd_all_positive_returns_is_zero():
    """全て正のリターン系列の場合、MDD ≈ 0 (ドローダウンなし)。"""
    returns = [0.001, 0.002, 0.003, 0.0005, 0.001]
    m = compute_metrics(returns)

    assert m["MDD"] is not None
    assert m["MDD"] == pytest.approx(0.0, abs=1e-10), f"全正リターンで MDD != 0: {m['MDD']}"


# ---------------------------------------------------------------------------
# テスト 5: intersection / pairwise の集合演算が正しい
# ---------------------------------------------------------------------------


def test_subset_intersection_logic():
    """3 sets の intersection / pairwise が正しい集合演算を行う。"""
    # simple_v1: d1, d2, d3 で executed
    # paper_v1:  d2, d3, d4 で executed
    # paper_v2:  d3, d4, d5 で executed
    d1, d2, d3, d4, d5 = (
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    )

    dated_returns_map = {
        "simple_v1": [(d1, 0.01), (d2, 0.02), (d3, 0.03)],
        "paper_v1":  [(d2, 0.01), (d3, 0.02), (d4, 0.03)],
        "paper_v2":  [(d3, 0.01), (d4, 0.02), (d5, 0.03)],
    }

    # 3-way: d3 のみ
    v3 = compute_view_b_subset(dated_returns_map, ["simple_v1", "paper_v1", "paper_v2"])
    assert v3["days"] == 1, f"3-way days={v3['days']} (expected 1)"

    # simple_v1 ∩ paper_v1: d2, d3
    v12 = compute_view_b_subset(dated_returns_map, ["simple_v1", "paper_v1"])
    assert v12["days"] == 2, f"s1∩p1 days={v12['days']} (expected 2)"

    # simple_v1 ∩ paper_v2: d3
    v13 = compute_view_b_subset(dated_returns_map, ["simple_v1", "paper_v2"])
    assert v13["days"] == 1, f"s1∩p2 days={v13['days']} (expected 1)"

    # paper_v1 ∩ paper_v2: d3, d4
    v23 = compute_view_b_subset(dated_returns_map, ["paper_v1", "paper_v2"])
    assert v23["days"] == 2, f"p1∩p2 days={v23['days']} (expected 2)"


# ---------------------------------------------------------------------------
# テスト 6: 空の intersection → metrics が None または skip される
# ---------------------------------------------------------------------------


def test_empty_subset_handled():
    """交差が空の場合、days=0 かつ各 signal_type の metrics が None になる。"""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)

    dated_returns_map = {
        "simple_v1": [(d1, 0.01)],   # d1 のみ executed
        "paper_v1":  [(d2, 0.01)],   # d2 のみ executed
        "paper_v2":  [(d1, 0.01)],   # d1 のみ executed
    }

    # simple_v1 ∩ paper_v1: 空
    result = compute_view_b_subset(dated_returns_map, ["simple_v1", "paper_v1"])
    assert result["days"] == 0
    assert result.get("simple_v1") is None
    assert result.get("paper_v1") is None


# ---------------------------------------------------------------------------
# テスト 7: Markdown に必須セクションのヘッダが含まれる
# ---------------------------------------------------------------------------


def test_markdown_report_contains_required_sections():
    """生成した Markdown に §2-§7 (## 1 〜 ## 7) のヘッダが含まれる。"""
    eval_run_json = _make_minimal_eval_run_json()

    dated_returns = _make_dated_returns(_KNOWN_RETURNS)
    dated_map = {
        "simple_v1": dated_returns,
        "paper_v1": dated_returns,
        "paper_v2": dated_returns,
    }

    view_a = {st: compute_view_a(dated_map[st]) for st in ["simple_v1", "paper_v1", "paper_v2"]}

    from scripts.build_evaluation_report import _SIGNAL_TYPES

    view_b = {
        "3way": compute_view_b_subset(dated_map, _SIGNAL_TYPES),
        "simple_v1_paper_v1": compute_view_b_subset(dated_map, ["simple_v1", "paper_v1"]),
        "simple_v1_paper_v2": compute_view_b_subset(dated_map, ["simple_v1", "paper_v2"]),
        "paper_v1_paper_v2": compute_view_b_subset(dated_map, ["paper_v1", "paper_v2"]),
    }

    md = build_markdown(
        eval_run_json=eval_run_json,
        view_a=view_a,
        view_b=view_b,
        eval_date_str="2026-04-17",
        input_json_path="docs/paper_v2/evaluation_runs/run_20260417_130325.json",
    )

    required_headers = [
        "## 1. 実行メタデータ",
        "## 2. View A: 全期間サマリー",
        "## 3. View B: 共通実行可能日サブセット",
        "## 4. スキップ構造",
        "## 5. 累積リターン時系列",
        "## 6. 所見 (factual only)",
        "## 7. 限界事項",
    ]

    for header in required_headers:
        assert header in md, f"必須ヘッダが見つかりません: {header!r}"


# ---------------------------------------------------------------------------
# テスト 8: CSV のスキーマ (列順序・ヘッダ)
# ---------------------------------------------------------------------------


def test_csv_schemas():
    """cumulative_returns.csv / daily_returns.csv の列順序・ヘッダが正しい。"""
    dated_returns = _make_dated_returns([0.01, -0.005, 0.02])
    dated_map = {
        "simple_v1": dated_returns,
        "paper_v1": dated_returns,
        "paper_v2": dated_returns,
    }

    cum_csv = build_cumulative_csv(dated_map)
    daily_csv = build_daily_csv(dated_map)

    expected_header = "date,simple_v1,paper_v1,paper_v2"

    # ヘッダ確認
    cum_lines = cum_csv.strip().split("\n")
    assert cum_lines[0] == expected_header, f"cumulative_returns header: {cum_lines[0]!r}"

    daily_lines = daily_csv.strip().split("\n")
    assert daily_lines[0] == expected_header, f"daily_returns header: {daily_lines[0]!r}"

    # 行数: ヘッダ + 3 日分
    assert len(cum_lines) == 4, f"cumulative rows={len(cum_lines)} (expected 4)"
    assert len(daily_lines) == 4, f"daily rows={len(daily_lines)} (expected 4)"

    # cumulative: skip 日なしなので全行 non-empty
    for line in cum_lines[1:]:
        parts = line.split(",")
        assert len(parts) == 4, f"cumulative列数: {len(parts)}"
        for v in parts[1:]:
            assert v != "", f"cumulative に空欄: {line!r}"

    # daily: skip 日なしなので全行 non-empty
    for line in daily_lines[1:]:
        parts = line.split(",")
        assert len(parts) == 4, f"daily列数: {len(parts)}"
        for v in parts[1:]:
            assert v != "", f"daily に空欄なし: {line!r}"


def test_csv_daily_none_as_empty():
    """daily_returns.csv で skip 日 (None) は空欄で出力される。"""
    dated_map = {
        "simple_v1": _make_dated_returns([0.01, None, 0.02]),
        "paper_v1":  _make_dated_returns([0.01, None, 0.02]),
        "paper_v2":  _make_dated_returns([0.01, None, 0.02]),
    }

    daily_csv = build_daily_csv(dated_map)
    lines = daily_csv.strip().split("\n")

    # 2 行目 (index 2) は None の日 → 空欄
    parts = lines[2].split(",")
    assert parts[1] == "", f"None の日が空欄でない: {parts[1]!r}"
    assert parts[2] == "", f"None の日が空欄でない: {parts[2]!r}"
    assert parts[3] == "", f"None の日が空欄でない: {parts[3]!r}"


# ---------------------------------------------------------------------------
# テスト 9: --dry-run でファイル作成なし
# ---------------------------------------------------------------------------


def test_dry_run_no_file_writes(tmp_path, monkeypatch):
    """--dry-run オプション指定時にファイルが一切作成されない。"""
    import json as _json

    # 最小限の評価 run JSON を tmp_path に書き込む
    eval_run = _make_minimal_eval_run_json()
    input_file = tmp_path / "run_test.json"
    input_file.write_text(_json.dumps(eval_run), encoding="utf-8")

    # fetch_daily_results をモック
    from datetime import timedelta

    def _mock_fetch(run_id: int):
        base = date(2024, 1, 4)
        return [(base + timedelta(days=i), 0.01 * (i % 5 - 2)) for i in range(10)]

    import scripts.build_evaluation_report as _mod
    monkeypatch.setattr(_mod, "fetch_daily_results", _mock_fetch)

    # 出力先として tmp_path/output を指定 (dry-run なので作成されない)
    output_dir = tmp_path / "output"

    rc = main([
        "--input", str(input_file),
        "--output-dir", str(output_dir),
        "--dry-run",
    ])

    assert rc == 0, f"dry-run が 0 以外を返した: {rc}"
    # ディレクトリもファイルも作成されていないこと
    assert not output_dir.exists(), f"dry-run にもかかわらず output_dir が作成された: {output_dir}"


# ---------------------------------------------------------------------------
# テスト 10: metadata.json の必須キー存在確認
# ---------------------------------------------------------------------------


def test_metadata_json_schema():
    """metadata.json に必須キーが存在する。"""
    eval_run_json = _make_minimal_eval_run_json()

    dated_returns = _make_dated_returns(_KNOWN_RETURNS)
    dated_map = {
        "simple_v1": dated_returns,
        "paper_v1": dated_returns,
        "paper_v2": dated_returns,
    }

    view_a = {st: compute_view_a(dated_map[st]) for st in ["simple_v1", "paper_v1", "paper_v2"]}

    from scripts.build_evaluation_report import _SIGNAL_TYPES

    view_b = {
        "3way": compute_view_b_subset(dated_map, _SIGNAL_TYPES),
        "simple_v1_paper_v1": compute_view_b_subset(dated_map, ["simple_v1", "paper_v1"]),
        "simple_v1_paper_v2": compute_view_b_subset(dated_map, ["simple_v1", "paper_v2"]),
        "paper_v1_paper_v2": compute_view_b_subset(dated_map, ["paper_v1", "paper_v2"]),
    }

    meta = build_metadata_json(
        eval_run_json=eval_run_json,
        view_a=view_a,
        view_b=view_b,
        report_at="2026-04-17T00:00:00Z",
        report_git_sha="abc123",
        input_json_path="docs/paper_v2/evaluation_runs/run_test.json",
    )

    required_keys = ["report_at", "report_git_sha", "input_run_json", "evaluation_run", "view_a", "view_b"]
    for key in required_keys:
        assert key in meta, f"必須キーが見つかりません: {key!r}"

    # view_a に 3 signal_type が含まれる
    for st in ["simple_v1", "paper_v1", "paper_v2"]:
        assert st in meta["view_a"], f"view_a に {st} がない"

    # view_b に 3way / pairwise キーが含まれる
    for key in ["3way", "simple_v1_paper_v1", "simple_v1_paper_v2", "paper_v1_paper_v2"]:
        assert key in meta["view_b"], f"view_b に {key!r} がない"

    # JSON シリアライズ可能であること
    json_str = json.dumps(meta, default=str)
    restored = json.loads(json_str)
    assert restored["report_at"] == "2026-04-17T00:00:00Z"
