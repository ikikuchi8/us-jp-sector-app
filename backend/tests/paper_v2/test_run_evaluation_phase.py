"""
tests/paper_v2/test_run_evaluation_phase.py — run_evaluation_phase.py のユニットテスト。

# 方針
  - mock ベース: 実 DB 接続なし、実サービス呼び出しなし
  - subprocess または importlib 経由でスクリプトをロード
  - unittest.mock.patch で各サービスを差し替え
  - 5 必須テストを実装

# テスト一覧
  1. test_dry_run_prints_plan         — dry-run で実行計画が stdout に出力され exit 0
  2. test_json_output_schema          — mock 経由の出力 JSON が §9 schema の必須キーを含む
  3. test_signal_type_failure_is_soft — 1 系列例外でも他系列は継続、error フィールドに記録
  4. test_skip_signal_generation_flag — --skip-signal-generation でシグナル生成スキップ
  5. test_skip_backtest_flag          — --skip-backtest でバックテストスキップ
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# スクリプトのロード
# ---------------------------------------------------------------------------

_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
_SCRIPT_PATH = _BACKEND_DIR / "scripts" / "run_evaluation_phase.py"
_MODULE_NAME = "run_evaluation_phase"


def _load_script_module():
    """run_evaluation_phase.py をモジュールとしてロードする (sys.path 設定済みの状態で)。"""
    # すでにロード済みならキャッシュを返す
    if _MODULE_NAME in sys.modules:
        return sys.modules[_MODULE_NAME]

    spec = importlib.util.spec_from_file_location(
        _MODULE_NAME, str(_SCRIPT_PATH)
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # sys.modules に登録してから exec_module (patch のターゲット解決に必要)
    sys.modules[_MODULE_NAME] = mod
    spec.loader.exec_module(mod)
    return mod


# モジュールを一度だけロード (import 副作用: os.chdir / logging 設定)
_MOD = _load_script_module()


# ---------------------------------------------------------------------------
# モック用ヘルパー
# ---------------------------------------------------------------------------

def _make_signal_result(
    requested: int = 100,
    succeeded_count: int = 95,
    failed_count: int = 0,
    skipped_count: int = 5,
    saved_rows: int = 1615,
) -> MagicMock:
    """SignalGenerationResult 相当の MagicMock を返す。"""
    r = MagicMock()
    r.requested = requested
    # succeeded / skipped はリストの長さ (len) のみスクリプトが参照するため
    # 実際の date 値は不要。len() が正しく返るよう list を使う。
    r.succeeded = [None] * succeeded_count
    r.failed = {}
    r.skipped = [None] * skipped_count
    r.saved_rows = saved_rows
    return r


def _make_backtest_result(run_id: int = 42, trading_days: int = 95) -> MagicMock:
    """BacktestRunResult 相当の MagicMock を返す。"""
    r = MagicMock()
    r.run_id = run_id
    r.trading_days = trading_days
    r.daily_results = [MagicMock() for _ in range(trading_days)]
    return r


# ---------------------------------------------------------------------------
# パッチターゲット
# ---------------------------------------------------------------------------

# lazy import のため run_evaluation_phase のモジュール名ではなく
# 実際の import 元を patch する
_PATCH_SESSIONLOCAL = "app.database.SessionLocal"
_PATCH_SIGNAL_SERVICE = "run_evaluation_phase._build_signal_service"
_PATCH_BACKTEST_SERVICE_CLASS = "app.services.backtest_service.BacktestService"
_PATCH_GET_GIT_SHA = "run_evaluation_phase._get_git_sha"
_PATCH_GET_DB_SNAPSHOT = "run_evaluation_phase._get_db_snapshot"
_PATCH_C0_VERSION = "app.services.paper_v2.constants.C0_VERSION"
_PATCH_CALENDAR_SERVICE = "app.services.calendar_service.CalendarService"


def _make_session_mock():
    """DB session の MagicMock を返す。SELECT 1 / スナップショット系を処理。"""
    session_mock = MagicMock()
    # execute("SELECT 1").scalar() → 1
    session_mock.execute.return_value.scalar.return_value = 1
    return session_mock


def _make_session_local_mock(session_mock):
    """SessionLocal() が session_mock を返す MagicMock を返す。"""
    sl = MagicMock()
    sl.return_value = session_mock
    return sl


# ---------------------------------------------------------------------------
# テスト 1: dry-run
# ---------------------------------------------------------------------------


def test_dry_run_prints_plan(capsys, tmp_path):
    """--dry-run で実行計画が stdout に出力され exit 0 になること。ファイル書き込みなし。"""
    output_path = tmp_path / "run_test.json"

    ret = _MOD.main([
        "--start-date", "2024-01-04",
        "--end-date", "2026-04-15",
        "--output", str(output_path),
        "--dry-run",
    ])

    assert ret == 0, f"exit code should be 0, got {ret}"

    captured = capsys.readouterr()
    stdout = captured.out

    # 実行計画の主要要素が含まれているか
    assert "2024-01-04" in stdout
    assert "2026-04-15" in stdout
    assert "dry-run" in stdout.lower() or "dry_run" in stdout.lower() or "dry-run" in stdout

    # ファイルが書き込まれていないこと
    assert not output_path.exists(), "dry-run ではファイルを書き込んではいけない"


# ---------------------------------------------------------------------------
# テスト 2: JSON 出力 schema
# ---------------------------------------------------------------------------


def test_json_output_schema(tmp_path):
    """mock サービスを使った場合の出力 JSON が §9 schema の必須キーを含むこと。"""
    output_path = tmp_path / "run_output.json"

    signal_result = _make_signal_result()
    # paper_v2 用: skip_reasons 属性を追加
    signal_result_v2 = _make_signal_result()
    signal_result_v2.skip_reasons = {}

    backtest_result = _make_backtest_result(run_id=10)

    session_mock = _make_session_mock()

    signal_svc_mock = MagicMock()
    signal_svc_mock.generate_signals_for_range.side_effect = [
        signal_result,   # simple_v1
        signal_result,   # paper_v1
        signal_result_v2,  # paper_v2
    ]

    backtest_svc_mock = MagicMock()
    backtest_svc_mock.run.return_value = backtest_result

    with (
        patch(_PATCH_SESSIONLOCAL, _make_session_local_mock(session_mock)),
        patch(_PATCH_SIGNAL_SERVICE, return_value=signal_svc_mock),
        patch(_PATCH_BACKTEST_SERVICE_CLASS, return_value=backtest_svc_mock),
        patch(_PATCH_GET_GIT_SHA, return_value="abc1234"),
        patch(_PATCH_GET_DB_SNAPSHOT, return_value={
            "price_db_row_count": 50000,
            "price_db_last_date": "2026-04-17",
            "latest_common_date_28_tickers": "2026-04-16",
        }),
        patch(_PATCH_C0_VERSION, "v1"),
        patch(_PATCH_CALENDAR_SERVICE),
    ):
        ret = _MOD.main([
            "--start-date", "2024-01-04",
            "--end-date", "2026-04-15",
            "--output", str(output_path),
        ])

    assert ret == 0
    assert output_path.exists(), "出力 JSON が存在しないこと"

    with open(output_path, encoding="utf-8") as f:
        result = json.load(f)

    # Top-level 必須キー
    for key in ("run_at", "git_sha", "python_version", "config",
                "signal_generation", "backtest", "db_snapshot", "c0_version"):
        assert key in result, f"必須キー '{key}' が出力 JSON に含まれていない"

    # config の必須キー
    config = result["config"]
    for key in ("start_date", "end_date", "cost_params", "signal_types"):
        assert key in config, f"config.{key} が含まれていない"

    assert config["cost_params"]["commission_rate"] == 0.0
    assert config["cost_params"]["slippage_rate"] == 0.0
    assert config["signal_types"] == ["simple_v1", "paper_v1", "paper_v2"]

    # signal_generation の各系列
    sg = result["signal_generation"]
    for st in ("simple_v1", "paper_v1", "paper_v2"):
        assert st in sg, f"signal_generation.{st} が含まれていない"
        for key in ("requested", "succeeded_count", "failed_count",
                    "skipped_count", "saved_rows", "elapsed_seconds", "error"):
            assert key in sg[st], f"signal_generation.{st}.{key} が含まれていない"

    # backtest の各系列
    bt = result["backtest"]
    for st in ("simple_v1", "paper_v1", "paper_v2"):
        assert st in bt, f"backtest.{st} が含まれていない"
        for key in ("run_id", "status", "daily_results_count",
                    "executed_days", "elapsed_seconds", "error"):
            assert key in bt[st], f"backtest.{st}.{key} が含まれていない"

    # db_snapshot の必須キー
    ds = result["db_snapshot"]
    for key in ("price_db_row_count", "price_db_last_date",
                "latest_common_date_28_tickers"):
        assert key in ds, f"db_snapshot.{key} が含まれていない"

    assert result["c0_version"] == "v1"


# ---------------------------------------------------------------------------
# テスト 3: fail-soft (1 系列で例外が出ても他系列は継続)
# ---------------------------------------------------------------------------


def test_signal_type_failure_is_soft(tmp_path):
    """1 系列でシグナル生成例外が発生しても他系列は継続し、error フィールドに記録されること。"""
    output_path = tmp_path / "run_failure.json"

    signal_result_ok = _make_signal_result()
    signal_result_v2 = _make_signal_result()
    signal_result_v2.skip_reasons = {}

    backtest_result = _make_backtest_result(run_id=20)

    session_mock = _make_session_mock()

    # paper_v1 で例外を発生させる
    def _svc_factory_side_effect(signal_type, session):
        svc = MagicMock()
        if signal_type == "paper_v1":
            svc.generate_signals_for_range.side_effect = RuntimeError("paper_v1 故障テスト")
        elif signal_type == "paper_v2":
            svc.generate_signals_for_range.return_value = signal_result_v2
        else:
            svc.generate_signals_for_range.return_value = signal_result_ok
        return svc

    backtest_svc_mock = MagicMock()
    backtest_svc_mock.run.return_value = backtest_result

    with (
        patch(_PATCH_SESSIONLOCAL, _make_session_local_mock(session_mock)),
        patch(_PATCH_SIGNAL_SERVICE, side_effect=_svc_factory_side_effect),
        patch(_PATCH_BACKTEST_SERVICE_CLASS, return_value=backtest_svc_mock),
        patch(_PATCH_GET_GIT_SHA, return_value="abc1234"),
        patch(_PATCH_GET_DB_SNAPSHOT, return_value={
            "price_db_row_count": 50000,
            "price_db_last_date": "2026-04-17",
            "latest_common_date_28_tickers": "2026-04-16",
        }),
        patch(_PATCH_C0_VERSION, "v1"),
        patch(_PATCH_CALENDAR_SERVICE),
    ):
        ret = _MOD.main([
            "--start-date", "2024-01-04",
            "--end-date", "2026-04-15",
            "--output", str(output_path),
        ])

    # 1 系失敗なので exit code は 1
    assert ret == 1, f"失敗ありなので exit code=1 を期待, got {ret}"

    # JSON は書き出されている
    assert output_path.exists()
    with open(output_path, encoding="utf-8") as f:
        result = json.load(f)

    sg = result["signal_generation"]

    # simple_v1, paper_v2 は正常
    assert sg["simple_v1"]["error"] is None
    assert sg["paper_v2"]["error"] is None

    # paper_v1 は error が記録されている
    assert sg["paper_v1"]["error"] is not None
    assert "paper_v1 故障テスト" in sg["paper_v1"]["error"]

    # バックテストは全系列実行されている (paper_v1 含む)
    bt = result["backtest"]
    assert "simple_v1" in bt
    assert "paper_v1" in bt
    assert "paper_v2" in bt


# ---------------------------------------------------------------------------
# テスト 4: --skip-signal-generation フラグ
# ---------------------------------------------------------------------------


def test_skip_signal_generation_flag(tmp_path):
    """--skip-signal-generation でシグナル生成がスキップされ、バックテストのみ実行されること。"""
    output_path = tmp_path / "run_skip_sig.json"

    backtest_result = _make_backtest_result(run_id=30)

    session_mock = _make_session_mock()
    backtest_svc_mock = MagicMock()
    backtest_svc_mock.run.return_value = backtest_result

    signal_svc_mock = MagicMock()

    with (
        patch(_PATCH_SESSIONLOCAL, _make_session_local_mock(session_mock)),
        patch(_PATCH_SIGNAL_SERVICE, return_value=signal_svc_mock),
        patch(_PATCH_BACKTEST_SERVICE_CLASS, return_value=backtest_svc_mock),
        patch(_PATCH_GET_GIT_SHA, return_value="abc1234"),
        patch(_PATCH_GET_DB_SNAPSHOT, return_value={
            "price_db_row_count": 50000,
            "price_db_last_date": "2026-04-17",
            "latest_common_date_28_tickers": "2026-04-16",
        }),
        patch(_PATCH_C0_VERSION, "v1"),
        patch(_PATCH_CALENDAR_SERVICE),
    ):
        ret = _MOD.main([
            "--start-date", "2024-01-04",
            "--end-date", "2026-04-15",
            "--output", str(output_path),
            "--skip-signal-generation",
        ])

    assert ret == 0

    # シグナルサービスは呼ばれていない
    signal_svc_mock.generate_signals_for_range.assert_not_called()

    assert output_path.exists()
    with open(output_path, encoding="utf-8") as f:
        result = json.load(f)

    # signal_generation は skipped 状態
    sg = result["signal_generation"]
    for st in ("simple_v1", "paper_v1", "paper_v2"):
        assert sg[st].get("status") == "skipped", (
            f"{st} の signal_generation は 'skipped' であるべき, got {sg[st]}"
        )

    # backtest は実行されている
    bt = result["backtest"]
    for st in ("simple_v1", "paper_v1", "paper_v2"):
        assert bt[st].get("status") != "skipped", (
            f"{st} のバックテストは実行されるべき"
        )
        assert bt[st].get("run_id") == 30


# ---------------------------------------------------------------------------
# テスト 5: --skip-backtest フラグ
# ---------------------------------------------------------------------------


def test_skip_backtest_flag(tmp_path):
    """--skip-backtest でバックテストがスキップされ、シグナル生成のみ実行されること。"""
    output_path = tmp_path / "run_skip_bt.json"

    signal_result = _make_signal_result()
    signal_result_v2 = _make_signal_result()
    signal_result_v2.skip_reasons = {}

    session_mock = _make_session_mock()

    def _svc_factory(signal_type, session):
        svc = MagicMock()
        if signal_type == "paper_v2":
            svc.generate_signals_for_range.return_value = signal_result_v2
        else:
            svc.generate_signals_for_range.return_value = signal_result
        return svc

    backtest_svc_mock = MagicMock()

    with (
        patch(_PATCH_SESSIONLOCAL, _make_session_local_mock(session_mock)),
        patch(_PATCH_SIGNAL_SERVICE, side_effect=_svc_factory),
        patch(_PATCH_BACKTEST_SERVICE_CLASS, return_value=backtest_svc_mock),
        patch(_PATCH_GET_GIT_SHA, return_value="abc1234"),
        patch(_PATCH_GET_DB_SNAPSHOT, return_value={
            "price_db_row_count": 50000,
            "price_db_last_date": "2026-04-17",
            "latest_common_date_28_tickers": "2026-04-16",
        }),
        patch(_PATCH_C0_VERSION, "v1"),
        patch(_PATCH_CALENDAR_SERVICE),
    ):
        ret = _MOD.main([
            "--start-date", "2024-01-04",
            "--end-date", "2026-04-15",
            "--output", str(output_path),
            "--skip-backtest",
        ])

    assert ret == 0

    # バックテストサービスの run() は呼ばれていない
    backtest_svc_mock.run.assert_not_called()

    assert output_path.exists()
    with open(output_path, encoding="utf-8") as f:
        result = json.load(f)

    # signal_generation は実行されている
    sg = result["signal_generation"]
    for st in ("simple_v1", "paper_v1", "paper_v2"):
        assert sg[st].get("status") != "skipped", (
            f"{st} のシグナル生成は実行されるべき"
        )
        assert sg[st].get("saved_rows") == 1615

    # backtest は skipped 状態
    bt = result["backtest"]
    for st in ("simple_v1", "paper_v1", "paper_v2"):
        assert bt[st].get("status") == "skipped", (
            f"{st} のバックテストは 'skipped' であるべき, got {bt[st]}"
        )
