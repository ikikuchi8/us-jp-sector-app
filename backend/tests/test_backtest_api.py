"""
tests/test_backtest_api.py — backtest API の単体テスト。

テスト戦略:
  - DB 接続なし: get_db を MagicMock でオーバーライドする
  - CalendarService なし: get_calendar_service を MagicMock でオーバーライドする
  - BacktestService なし: app.api.backtest.BacktestService を patch する
  - BacktestRepository なし: app.api.backtest.BacktestRepository を patch する
  - FastAPI TestClient でエンドポイントを HTTP レベルで呼び出す

テスト範囲:
  POST /backtest/run
    - 正常系 (200) + 全フィールドマッピング
    - start_date > end_date → 422
    - start_date == end_date → 200 (境界値)
    - signal_type が "simple_v1" 以外 → 422
    - commission_rate < 0 → 422
    - slippage_rate < 0 → 422
    - BacktestService が正しい引数で構築されること
    - CostParams が正しい値で渡されること

  GET /backtest/{run_id}
    - 正常系 (200) + 全フィールドマッピング (Decimal→float 変換含む)
    - run が存在しない → 404
    - status, started_at, finished_at が正しく返ること

  GET /backtest/{run_id}/daily
    - 正常系 (200) + 全フィールドマッピング
    - 空リスト → rows=[]
    - run が存在しない → 404
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.database import get_db
from app.main import app
from app.models.backtest import BacktestStatus
from app.services.backtest_service import BacktestRunResult, DailyResult
from app.services.calendar_service import get_calendar_service


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db() -> MagicMock:
    """DB セッションのモック。get_db 依存の差し替えに使用する。"""
    return MagicMock()


@pytest.fixture
def mock_calendar() -> MagicMock:
    """CalendarService のモック。get_calendar_service 依存の差し替えに使用する。"""
    return MagicMock()


@pytest.fixture
def client(mock_db: MagicMock, mock_calendar: MagicMock):
    """テスト用 TestClient。依存を mock でオーバーライドして返す。"""
    app.dependency_overrides[get_db] = lambda: mock_db
    app.dependency_overrides[get_calendar_service] = lambda: mock_calendar
    yield TestClient(app)
    app.dependency_overrides.clear()


def _make_run_result(**kwargs) -> BacktestRunResult:
    """BacktestRunResult のテスト用ファクトリ。デフォルト値で正常系を作る。"""
    defaults: dict = dict(
        run_id=1,
        trading_days=10,
        total_return=0.05,
        annual_return=0.12,
        annual_vol=0.08,
        sharpe_ratio=1.5,
        max_drawdown=0.03,
        win_rate=0.6,
        daily_results=[],
    )
    defaults.update(kwargs)
    return BacktestRunResult(**defaults)


def _make_daily_result(**kwargs) -> DailyResult:
    """DailyResult のテスト用ファクトリ。"""
    defaults: dict = dict(
        jp_execution_date=date(2025, 1, 6),
        daily_return=0.01,
        cumulative_return=0.01,
        long_return=0.02,
        short_return=0.00,
        long_count=3,
        short_count=3,
    )
    defaults.update(kwargs)
    return DailyResult(**defaults)


def _make_backtest_run_mock(**kwargs) -> MagicMock:
    """BacktestRun ORM オブジェクトのモックを作る。

    Decimal 型のフィールドは Decimal で設定する (ORM の実際の挙動を模倣)。
    """
    run = MagicMock()
    run.id = kwargs.get("id", 42)
    run.status = kwargs.get("status", BacktestStatus.DONE)
    run.signal_type = kwargs.get("signal_type", "simple_v1")
    run.start_date = kwargs.get("start_date", date(2025, 1, 6))
    run.end_date = kwargs.get("end_date", date(2025, 1, 10))
    run.commission_rate = kwargs.get("commission_rate", Decimal("0.001000"))
    run.slippage_rate = kwargs.get("slippage_rate", Decimal("0.000500"))
    run.trading_days = kwargs.get("trading_days", 5)
    run.total_return = kwargs.get("total_return", Decimal("0.050000"))
    run.annual_return = kwargs.get("annual_return", Decimal("0.120000"))
    run.annual_vol = kwargs.get("annual_vol", Decimal("0.080000"))
    run.sharpe_ratio = kwargs.get("sharpe_ratio", Decimal("1.500000"))
    run.max_drawdown = kwargs.get("max_drawdown", Decimal("0.030000"))
    run.win_rate = kwargs.get("win_rate", Decimal("0.600000"))
    run.started_at = kwargs.get(
        "started_at", datetime(2025, 1, 10, 9, 0, 0, tzinfo=timezone.utc)
    )
    run.finished_at = kwargs.get(
        "finished_at", datetime(2025, 1, 10, 9, 0, 5, tzinfo=timezone.utc)
    )
    return run


def _make_daily_result_orm(**kwargs) -> MagicMock:
    """BacktestResultDaily ORM オブジェクトのモックを作る。

    BacktestDailyRow.model_validate(r) が参照するフィールドを設定する。
    """
    row = MagicMock()
    row.jp_execution_date = kwargs.get("jp_execution_date", date(2025, 1, 6))
    row.daily_return = kwargs.get("daily_return", Decimal("0.010000"))
    row.cumulative_return = kwargs.get("cumulative_return", Decimal("0.010000"))
    row.long_return = kwargs.get("long_return", Decimal("0.020000"))
    row.short_return = kwargs.get("short_return", Decimal("0.000000"))
    row.long_count = kwargs.get("long_count", 3)
    row.short_count = kwargs.get("short_count", 3)
    return row


# ---------------------------------------------------------------------------
# POST /backtest/run
# ---------------------------------------------------------------------------


class TestRunBacktest:
    """POST /backtest/run のテスト群。"""

    # ------------------------------------------------------------------
    # 正常系
    # ------------------------------------------------------------------

    def test_run_returns_200(self, client: TestClient) -> None:
        """正常系: 200 が返ること。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            response = client.post(
                "/backtest/run",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                },
            )
        assert response.status_code == 200

    def test_response_maps_all_fields(self, client: TestClient) -> None:
        """BacktestRunResult の全フィールドがレスポンスに正しくマッピングされること。"""
        mock_result = _make_run_result(
            run_id=7,
            trading_days=3,
            total_return=0.05,
            annual_return=0.12,
            annual_vol=0.08,
            sharpe_ratio=1.5,
            max_drawdown=0.03,
            win_rate=0.6,
        )
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            response = client.post(
                "/backtest/run",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "commission_rate": 0.001,
                    "slippage_rate": 0.0005,
                },
            )

        assert response.status_code == 200
        body = response.json()
        assert body["run_id"] == 7
        assert body["status"] == "done"
        assert body["signal_type"] == "simple_v1"
        assert body["start_date"] == "2025-01-06"
        assert body["end_date"] == "2025-01-10"
        assert body["commission_rate"] == pytest.approx(0.001)
        assert body["slippage_rate"] == pytest.approx(0.0005)
        assert body["trading_days"] == 3
        assert body["total_return"] == pytest.approx(0.05)
        assert body["annual_return"] == pytest.approx(0.12)
        assert body["annual_vol"] == pytest.approx(0.08)
        assert body["sharpe_ratio"] == pytest.approx(1.5)
        assert body["max_drawdown"] == pytest.approx(0.03)
        assert body["win_rate"] == pytest.approx(0.6)

    def test_none_summary_stats_are_returned_as_null(self, client: TestClient) -> None:
        """取引日ゼロ時のサマリー統計 None は null で返ること。"""
        mock_result = _make_run_result(
            trading_days=0,
            total_return=None,
            annual_return=None,
            annual_vol=None,
            sharpe_ratio=None,
            max_drawdown=None,
            win_rate=None,
        )
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            response = client.post(
                "/backtest/run",
                json={"start_date": "2025-01-06", "end_date": "2025-01-10"},
            )

        body = response.json()
        assert body["trading_days"] == 0
        assert body["total_return"] is None
        assert body["annual_return"] is None
        assert body["annual_vol"] is None
        assert body["sharpe_ratio"] is None
        assert body["max_drawdown"] is None
        assert body["win_rate"] is None

    # ------------------------------------------------------------------
    # サービス構築 / 引数
    # ------------------------------------------------------------------

    def test_service_is_constructed_with_db_and_calendar(
        self, client: TestClient, mock_db: MagicMock, mock_calendar: MagicMock
    ) -> None:
        """BacktestService が db と calendar を受け取って構築されること。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            client.post(
                "/backtest/run",
                json={"start_date": "2025-01-06", "end_date": "2025-01-10"},
            )
        args, _ = MockSvc.call_args
        assert args[0] is mock_db
        assert args[1] is mock_calendar

    def test_run_called_with_correct_dates_and_signal_type(
        self, client: TestClient
    ) -> None:
        """run() が正しい start/end/signal_type で呼ばれること。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            client.post(
                "/backtest/run",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "signal_type": "simple_v1",
                },
            )
        call_kwargs = MockSvc.return_value.run.call_args
        assert call_kwargs.kwargs["start"] == date(2025, 1, 6)
        assert call_kwargs.kwargs["end"] == date(2025, 1, 10)
        assert call_kwargs.kwargs["signal_type"] == "simple_v1"

    def test_cost_params_passed_correctly(self, client: TestClient) -> None:
        """commission_rate / slippage_rate が CostParams として run() に渡されること。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            client.post(
                "/backtest/run",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "commission_rate": 0.001,
                    "slippage_rate": 0.0005,
                },
            )
        cost = MockSvc.return_value.run.call_args.kwargs["cost_params"]
        assert cost.commission_rate == pytest.approx(0.001)
        assert cost.slippage_rate == pytest.approx(0.0005)

    def test_default_cost_is_zero(self, client: TestClient) -> None:
        """commission_rate / slippage_rate 未指定時のデフォルトは 0.0 であること。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            client.post(
                "/backtest/run",
                json={"start_date": "2025-01-06", "end_date": "2025-01-10"},
            )
        cost = MockSvc.return_value.run.call_args.kwargs["cost_params"]
        assert cost.commission_rate == 0.0
        assert cost.slippage_rate == 0.0

    # ------------------------------------------------------------------
    # バリデーション
    # ------------------------------------------------------------------

    def test_start_date_after_end_date_returns_422(self, client: TestClient) -> None:
        """start_date > end_date のとき 422 が返ること。"""
        response = client.post(
            "/backtest/run",
            json={"start_date": "2025-01-10", "end_date": "2025-01-06"},
        )
        assert response.status_code == 422

    def test_start_date_equal_end_date_is_valid(self, client: TestClient) -> None:
        """start_date == end_date は有効 (1日分)。"""
        mock_result = _make_run_result(trading_days=1)
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            response = client.post(
                "/backtest/run",
                json={"start_date": "2025-01-06", "end_date": "2025-01-06"},
            )
        assert response.status_code == 200

    def test_unknown_signal_type_returns_422(self, client: TestClient) -> None:
        """不正な signal_type は Pydantic バリデーションで 422 が返ること。"""
        response = client.post(
            "/backtest/run",
            json={
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "signal_type": "unknown_v99",
            },
        )
        assert response.status_code == 422

    def test_paper_v1_signal_type_is_accepted(self, client: TestClient) -> None:
        """signal_type="paper_v1" は有効で 200 が返ること。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            response = client.post(
                "/backtest/run",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "signal_type": "paper_v1",
                },
            )
        assert response.status_code == 200

    def test_paper_v1_signal_type_passed_to_service(self, client: TestClient) -> None:
        """signal_type="paper_v1" が BacktestService.run() に渡されること。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            client.post(
                "/backtest/run",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "signal_type": "paper_v1",
                },
            )
        call_kwargs = MockSvc.return_value.run.call_args
        assert call_kwargs.kwargs["signal_type"] == "paper_v1"

    def test_negative_commission_rate_returns_422(self, client: TestClient) -> None:
        """commission_rate < 0 のとき 422 が返ること。"""
        response = client.post(
            "/backtest/run",
            json={
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "commission_rate": -0.001,
            },
        )
        assert response.status_code == 422

    def test_negative_slippage_rate_returns_422(self, client: TestClient) -> None:
        """slippage_rate < 0 のとき 422 が返ること。"""
        response = client.post(
            "/backtest/run",
            json={
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "slippage_rate": -0.001,
            },
        )
        assert response.status_code == 422

    def test_zero_commission_and_slippage_are_valid(self, client: TestClient) -> None:
        """commission_rate = 0.0 / slippage_rate = 0.0 は有効 (境界値)。"""
        mock_result = _make_run_result()
        with patch("app.api.backtest.BacktestService") as MockSvc:
            MockSvc.return_value.run.return_value = mock_result
            response = client.post(
                "/backtest/run",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "commission_rate": 0.0,
                    "slippage_rate": 0.0,
                },
            )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# GET /backtest/{run_id}
# ---------------------------------------------------------------------------


class TestGetRun:
    """GET /backtest/{run_id} のテスト群。"""

    # ------------------------------------------------------------------
    # 正常系
    # ------------------------------------------------------------------

    def test_get_run_returns_200(self, client: TestClient) -> None:
        """正常系: 200 が返ること。"""
        mock_run = _make_backtest_run_mock()
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            response = client.get("/backtest/42")
        assert response.status_code == 200

    def test_response_maps_all_fields(self, client: TestClient) -> None:
        """BacktestRun ORM の全フィールドがレスポンスに正しくマッピングされること。

        Decimal → float 変換が正しく行われることも確認する。
        """
        mock_run = _make_backtest_run_mock(
            id=42,
            status=BacktestStatus.DONE,
            signal_type="simple_v1",
            start_date=date(2025, 1, 6),
            end_date=date(2025, 1, 10),
            commission_rate=Decimal("0.001000"),
            slippage_rate=Decimal("0.000500"),
            trading_days=5,
            total_return=Decimal("0.050000"),
            annual_return=Decimal("0.120000"),
            annual_vol=Decimal("0.080000"),
            sharpe_ratio=Decimal("1.500000"),
            max_drawdown=Decimal("0.030000"),
            win_rate=Decimal("0.600000"),
            started_at=datetime(2025, 1, 10, 9, 0, 0, tzinfo=timezone.utc),
            finished_at=datetime(2025, 1, 10, 9, 0, 5, tzinfo=timezone.utc),
        )
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            response = client.get("/backtest/42")

        assert response.status_code == 200
        body = response.json()
        assert body["run_id"] == 42
        assert body["status"] == "done"
        assert body["signal_type"] == "simple_v1"
        assert body["start_date"] == "2025-01-06"
        assert body["end_date"] == "2025-01-10"
        assert body["commission_rate"] == pytest.approx(0.001)
        assert body["slippage_rate"] == pytest.approx(0.0005)
        assert body["trading_days"] == 5
        assert body["total_return"] == pytest.approx(0.05)
        assert body["annual_return"] == pytest.approx(0.12)
        assert body["annual_vol"] == pytest.approx(0.08)
        assert body["sharpe_ratio"] == pytest.approx(1.5)
        assert body["max_drawdown"] == pytest.approx(0.03)
        assert body["win_rate"] == pytest.approx(0.6)
        assert "started_at" in body
        assert "finished_at" in body

    def test_null_summary_stats_returned_as_null(self, client: TestClient) -> None:
        """サマリー統計が None の場合は null で返ること (status=running の途中 run)。"""
        mock_run = _make_backtest_run_mock(
            status=BacktestStatus.RUNNING,
            trading_days=None,
            total_return=None,
            annual_return=None,
            annual_vol=None,
            sharpe_ratio=None,
            max_drawdown=None,
            win_rate=None,
            finished_at=None,
        )
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            response = client.get("/backtest/42")

        body = response.json()
        assert body["status"] == "running"
        assert body["trading_days"] is None
        assert body["total_return"] is None
        assert body["finished_at"] is None

    def test_decimal_to_float_conversion(self, client: TestClient) -> None:
        """Decimal フィールドが float として返ること (型の確認)。"""
        mock_run = _make_backtest_run_mock(
            total_return=Decimal("0.123456"),
        )
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            response = client.get("/backtest/42")

        body = response.json()
        # JSON では float として返る。pytest.approx で精度確認
        assert body["total_return"] == pytest.approx(0.123456)
        assert isinstance(body["total_return"], float)

    # ------------------------------------------------------------------
    # 404
    # ------------------------------------------------------------------

    def test_run_not_found_returns_404(self, client: TestClient) -> None:
        """存在しない run_id のとき 404 が返ること。"""
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = None
            response = client.get("/backtest/999")
        assert response.status_code == 404

    def test_404_detail_contains_run_id(self, client: TestClient) -> None:
        """404 レスポンスの detail に run_id が含まれること。"""
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = None
            response = client.get("/backtest/999")
        assert "999" in response.json()["detail"]


# ---------------------------------------------------------------------------
# GET /backtest/{run_id}/daily
# ---------------------------------------------------------------------------


class TestGetDaily:
    """GET /backtest/{run_id}/daily のテスト群。"""

    # ------------------------------------------------------------------
    # 正常系
    # ------------------------------------------------------------------

    def test_get_daily_returns_200(self, client: TestClient) -> None:
        """正常系: 200 が返ること。"""
        mock_run = _make_backtest_run_mock()
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            MockRepo.return_value.list_daily_results.return_value = []
            response = client.get("/backtest/42/daily")
        assert response.status_code == 200

    def test_response_contains_run_id_and_rows(self, client: TestClient) -> None:
        """レスポンスに run_id と rows が含まれること。"""
        mock_run = _make_backtest_run_mock(id=42)
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            MockRepo.return_value.list_daily_results.return_value = []
            response = client.get("/backtest/42/daily")
        body = response.json()
        assert body["run_id"] == 42
        assert body["rows"] == []

    def test_daily_rows_are_mapped_correctly(self, client: TestClient) -> None:
        """日次結果の全フィールドがレスポンスに正しくマッピングされること。"""
        orm_row = _make_daily_result_orm(
            jp_execution_date=date(2025, 1, 6),
            daily_return=Decimal("0.010000"),
            cumulative_return=Decimal("0.010000"),
            long_return=Decimal("0.020000"),
            short_return=Decimal("0.000000"),
            long_count=3,
            short_count=3,
        )
        mock_run = _make_backtest_run_mock(id=42)
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            MockRepo.return_value.list_daily_results.return_value = [orm_row]
            response = client.get("/backtest/42/daily")

        assert response.status_code == 200
        rows = response.json()["rows"]
        assert len(rows) == 1
        row = rows[0]
        assert row["jp_execution_date"] == "2025-01-06"
        assert row["long_count"] == 3
        assert row["short_count"] == 3

    def test_daily_null_returns_are_returned_as_null(self, client: TestClient) -> None:
        """daily_return / long_return / short_return が None の行は null で返ること。"""
        orm_row = _make_daily_result_orm(
            daily_return=None,
            cumulative_return=Decimal("0.010000"),
            long_return=None,
            short_return=None,
            long_count=0,
            short_count=0,
        )
        mock_run = _make_backtest_run_mock(id=42)
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            MockRepo.return_value.list_daily_results.return_value = [orm_row]
            response = client.get("/backtest/42/daily")

        row = response.json()["rows"][0]
        assert row["daily_return"] is None
        assert row["long_return"] is None
        assert row["short_return"] is None

    def test_multiple_rows_returned_in_order(self, client: TestClient) -> None:
        """複数行が jp_execution_date 順 (リポジトリ返却順) で返ること。"""
        orm_rows = [
            _make_daily_result_orm(
                jp_execution_date=date(2025, 1, 6),
                cumulative_return=Decimal("0.010000"),
            ),
            _make_daily_result_orm(
                jp_execution_date=date(2025, 1, 7),
                cumulative_return=Decimal("0.021000"),
            ),
            _make_daily_result_orm(
                jp_execution_date=date(2025, 1, 8),
                cumulative_return=Decimal("0.030000"),
            ),
        ]
        mock_run = _make_backtest_run_mock(id=42)
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            MockRepo.return_value.list_daily_results.return_value = orm_rows
            response = client.get("/backtest/42/daily")

        rows = response.json()["rows"]
        assert len(rows) == 3
        assert rows[0]["jp_execution_date"] == "2025-01-06"
        assert rows[1]["jp_execution_date"] == "2025-01-07"
        assert rows[2]["jp_execution_date"] == "2025-01-08"

    def test_list_daily_results_called_with_correct_run_id(
        self, client: TestClient
    ) -> None:
        """list_daily_results が正しい run_id で呼ばれること。"""
        mock_run = _make_backtest_run_mock(id=42)
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = mock_run
            MockRepo.return_value.list_daily_results.return_value = []
            client.get("/backtest/42/daily")
        MockRepo.return_value.list_daily_results.assert_called_once_with(42)

    # ------------------------------------------------------------------
    # 404
    # ------------------------------------------------------------------

    def test_daily_run_not_found_returns_404(self, client: TestClient) -> None:
        """存在しない run_id のとき 404 が返ること。"""
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = None
            response = client.get("/backtest/999/daily")
        assert response.status_code == 404

    def test_daily_404_detail_contains_run_id(self, client: TestClient) -> None:
        """404 レスポンスの detail に run_id が含まれること。"""
        with patch("app.api.backtest.BacktestRepository") as MockRepo:
            MockRepo.return_value.get_run.return_value = None
            response = client.get("/backtest/999/daily")
        assert "999" in response.json()["detail"]
