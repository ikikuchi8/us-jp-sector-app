"""
tests/paper_v2/test_signals_router_paper_v2.py — paper_v2 signals/backtest router テスト。

テスト戦略:
  - DB 接続なし: get_db / get_calendar_service を MagicMock でオーバーライド
  - PaperV2SignalService / BacktestService を patch
  - FastAPI TestClient で HTTP レベルで呼び出す

テスト一覧 (8 件):
  1. test_generate_paper_v2_returns_200
  2. test_generate_paper_v2_response_schema
  3. test_generate_paper_v2_oos_boundary_populates_skip_reasons
  4. test_generate_paper_v2_writes_signal_daily_rows
  5. test_generate_paper_v2_unknown_type_returns_400
  6. test_generate_simple_v1_skip_reasons_empty
  7. test_generate_paper_v1_skip_reasons_empty
  8. test_backtest_paper_v2_reads_signal_rows
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.database import get_db
from app.main import app
from app.services.calendar_service import get_calendar_service
from app.services.paper_v2.signal_service import PaperV2GenerationResult
from app.services.paper_v2.skip_reasons import SkipReason
from app.services.signal_service import SignalGenerationResult
from app.services.backtest_service import BacktestRunResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_calendar() -> MagicMock:
    return MagicMock()


@pytest.fixture
def client(mock_db: MagicMock, mock_calendar: MagicMock):
    app.dependency_overrides[get_db] = lambda: mock_db
    app.dependency_overrides[get_calendar_service] = lambda: mock_calendar
    yield TestClient(app)
    app.dependency_overrides.clear()


def _make_paper_v2_result(**kwargs) -> PaperV2GenerationResult:
    """PaperV2GenerationResult のテスト用ファクトリ。"""
    defaults: dict = dict(
        requested=5,
        saved_rows=85,
        succeeded=[date(2024, 1, 4), date(2024, 1, 5)],
        failed={},
        skipped=[],
        skip_reasons={},
    )
    defaults.update(kwargs)
    return PaperV2GenerationResult(**defaults)


def _make_signal_result(**kwargs) -> SignalGenerationResult:
    """SignalGenerationResult のテスト用ファクトリ。"""
    defaults: dict = dict(
        requested=5,
        saved_rows=85,
        succeeded=[date(2025, 1, 6), date(2025, 1, 7)],
        failed={},
        skipped=[],
    )
    defaults.update(kwargs)
    return SignalGenerationResult(**defaults)


def _make_run_result(**kwargs) -> BacktestRunResult:
    """BacktestRunResult のテスト用ファクトリ。"""
    defaults: dict = dict(
        run_id=1,
        trading_days=5,
        total_return=0.03,
        annual_return=0.08,
        annual_vol=0.06,
        sharpe_ratio=1.2,
        max_drawdown=0.02,
        win_rate=0.55,
        daily_results=[],
    )
    defaults.update(kwargs)
    return BacktestRunResult(**defaults)


# ---------------------------------------------------------------------------
# 1. test_generate_paper_v2_returns_200
# ---------------------------------------------------------------------------


def test_generate_paper_v2_returns_200(client: TestClient) -> None:
    """signal_type="paper_v2" で POST /signals/generate が 200 を返すこと。"""
    mock_result = _make_paper_v2_result()
    with patch("app.api.signals.PaperV2SignalService") as MockSvc:
        MockSvc.return_value.generate_signals_for_range.return_value = mock_result
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2024-01-04",
                "end_date": "2024-01-31",
                "signal_type": "paper_v2",
            },
        )
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# 2. test_generate_paper_v2_response_schema
# ---------------------------------------------------------------------------


def test_generate_paper_v2_response_schema(client: TestClient) -> None:
    """paper_v2 レスポンスに skip_reasons_summary / skip_reasons_detail が存在すること。"""
    mock_result = _make_paper_v2_result(
        skipped=[date(2024, 1, 2), date(2024, 1, 3)],
        skip_reasons={
            date(2024, 1, 2): SkipReason.BEFORE_OOS_START,
            date(2024, 1, 3): SkipReason.BEFORE_OOS_START,
        },
    )
    with patch("app.api.signals.PaperV2SignalService") as MockSvc:
        MockSvc.return_value.generate_signals_for_range.return_value = mock_result
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2024-01-02",
                "end_date": "2024-01-31",
                "signal_type": "paper_v2",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert "skip_reasons_summary" in body
    assert "skip_reasons_detail" in body
    # skip_reasons_summary は dict[str, int]
    assert isinstance(body["skip_reasons_summary"], dict)
    assert isinstance(body["skip_reasons_detail"], dict)


# ---------------------------------------------------------------------------
# 3. test_generate_paper_v2_oos_boundary_populates_skip_reasons
# ---------------------------------------------------------------------------


def test_generate_paper_v2_oos_boundary_populates_skip_reasons(client: TestClient) -> None:
    """OOS 境界前の日付が skip_reasons_detail に "before_oos_start" として記録されること。"""
    # 2024-01-02, 2024-01-03 は OOS 前、2024-01-04 から OOS
    oos_boundary_skips = {
        date(2023, 12, 28): SkipReason.BEFORE_OOS_START,
        date(2023, 12, 29): SkipReason.BEFORE_OOS_START,
    }
    mock_result = _make_paper_v2_result(
        requested=7,
        succeeded=[date(2024, 1, 4), date(2024, 1, 5)],
        skipped=list(oos_boundary_skips.keys()),
        skip_reasons=oos_boundary_skips,
    )
    with patch("app.api.signals.PaperV2SignalService") as MockSvc:
        MockSvc.return_value.generate_signals_for_range.return_value = mock_result
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2023-12-28",
                "end_date": "2024-01-08",
                "signal_type": "paper_v2",
            },
        )

    assert response.status_code == 200
    body = response.json()
    detail = body["skip_reasons_detail"]
    summary = body["skip_reasons_summary"]

    assert "2023-12-28" in detail
    assert detail["2023-12-28"] == SkipReason.BEFORE_OOS_START
    assert "2023-12-29" in detail
    assert detail["2023-12-29"] == SkipReason.BEFORE_OOS_START
    assert summary.get(SkipReason.BEFORE_OOS_START) == 2


# ---------------------------------------------------------------------------
# 4. test_generate_paper_v2_writes_signal_daily_rows
# ---------------------------------------------------------------------------


def test_generate_paper_v2_writes_signal_daily_rows(client: TestClient) -> None:
    """signal_type="paper_v2" の SignalDaily 行が DB に書かれること (saved_rows > 0)。"""
    mock_result = _make_paper_v2_result(
        requested=3,
        saved_rows=51,
        succeeded=[date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8)],
    )
    with patch("app.api.signals.PaperV2SignalService") as MockSvc:
        MockSvc.return_value.generate_signals_for_range.return_value = mock_result
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2024-01-04",
                "end_date": "2024-01-08",
                "signal_type": "paper_v2",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["saved_rows"] == 51
    assert body["requested"] == 3
    assert len(body["succeeded"]) == 3


# ---------------------------------------------------------------------------
# 5. test_generate_paper_v2_unknown_type_returns_400
# ---------------------------------------------------------------------------


def test_generate_paper_v2_unknown_type_returns_400(client: TestClient) -> None:
    """signal_type="bogus" の場合、Pydantic バリデーションで 422 が返ること。"""
    response = client.post(
        "/signals/generate",
        json={
            "start_date": "2024-01-04",
            "end_date": "2024-01-31",
            "signal_type": "bogus",
        },
    )
    # Pydantic の Literal バリデーション -> 422
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# 6. test_generate_simple_v1_skip_reasons_empty
# ---------------------------------------------------------------------------


def test_generate_simple_v1_skip_reasons_empty(client: TestClient) -> None:
    """simple_v1 のレスポンスで skip_reasons_summary / detail が {} (後方互換) であること。"""
    mock_result = _make_signal_result()
    with patch("app.api.signals.SignalService") as MockSvc:
        MockSvc.return_value.generate_signals_for_range.return_value = mock_result
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "signal_type": "simple_v1",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["skip_reasons_summary"] == {}
    assert body["skip_reasons_detail"] == {}


# ---------------------------------------------------------------------------
# 7. test_generate_paper_v1_skip_reasons_empty
# ---------------------------------------------------------------------------


def test_generate_paper_v1_skip_reasons_empty(client: TestClient) -> None:
    """paper_v1 のレスポンスで skip_reasons_summary / detail が {} (後方互換) であること。"""
    mock_result = _make_signal_result()
    with patch("app.api.signals.PaperV1SignalService") as MockSvc:
        MockSvc.return_value.generate_signals_for_range.return_value = mock_result
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "signal_type": "paper_v1",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["skip_reasons_summary"] == {}
    assert body["skip_reasons_detail"] == {}


# ---------------------------------------------------------------------------
# 8. test_backtest_paper_v2_reads_signal_rows
# ---------------------------------------------------------------------------


def test_backtest_paper_v2_reads_signal_rows(client: TestClient) -> None:
    """backtest で signal_type="paper_v2" が受け付けられ、BacktestService.run() に渡されること。"""
    mock_result = _make_run_result()
    with patch("app.api.backtest.BacktestService") as MockSvc:
        MockSvc.return_value.run.return_value = mock_result
        response = client.post(
            "/backtest/run",
            json={
                "start_date": "2024-01-04",
                "end_date": "2024-03-29",
                "signal_type": "paper_v2",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["signal_type"] == "paper_v2"
    # BacktestService.run() に signal_type="paper_v2" が渡されること
    call_kwargs = MockSvc.return_value.run.call_args
    assert call_kwargs.kwargs["signal_type"] == "paper_v2"
