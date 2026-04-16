"""
tests/test_signals_api.py — signals API の単体テスト。

テスト戦略:
  - DB 接続なし: get_db を MagicMock でオーバーライドする
  - CalendarService なし: get_calendar_service を MagicMock でオーバーライドする
  - SignalService なし: app.api.signals.SignalService を patch する
  - FastAPI TestClient でエンドポイントを HTTP レベルで呼び出す

テスト範囲:
  POST /signals/generate
    - 正常系 (200)
    - SignalGenerationResult がレスポンスに正しくマッピングされること
    - start_date > end_date → 422
    - start_date == end_date → 200 (境界値)
    - signal_type が渡されること
    - has_failure が True / False になること
    - SignalService が正しい引数で構築されること

  GET /signals/latest
    - 正常系 (200)
    - レスポンスに全フィールドが含まれること
    - データなし → jp_execution_date=null, signals=[]
    - signal_type クエリパラメータが渡されること
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.database import get_db
from app.main import app
from app.services.calendar_service import get_calendar_service
from app.services.signal_service import SignalGenerationResult


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


def _make_result(**kwargs) -> SignalGenerationResult:
    """SignalGenerationResult のテスト用ファクトリ。デフォルト値で正常系を作る。"""
    defaults: dict = dict(
        requested=5,
        saved_rows=85,
        succeeded=[date(2025, 1, 6), date(2025, 1, 7)],
        failed={},
        skipped=[],
    )
    defaults.update(kwargs)
    return SignalGenerationResult(**defaults)


def _make_signal_row(**kwargs) -> MagicMock:
    """SignalDaily ORM オブジェクトのモックを作る。"""
    row = MagicMock()
    row.target_ticker = kwargs.get("target_ticker", "1617.T")
    row.us_signal_date = kwargs.get("us_signal_date", date(2025, 1, 3))
    row.jp_execution_date = kwargs.get("jp_execution_date", date(2025, 1, 6))
    row.signal_score = kwargs.get("signal_score", Decimal("0.012345"))
    row.signal_rank = kwargs.get("signal_rank", 1)
    row.suggested_side = kwargs.get("suggested_side", "long")
    return row


# ---------------------------------------------------------------------------
# POST /signals/generate
# ---------------------------------------------------------------------------


class TestGenerateSignals:
    """POST /signals/generate のテスト群。"""

    # ------------------------------------------------------------------
    # 正常系
    # ------------------------------------------------------------------

    def test_generate_returns_200(self, client: TestClient) -> None:
        """正常系: 200 が返ること。"""
        mock_result = _make_result()
        with patch("app.api.signals.SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            response = client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                },
            )
        assert response.status_code == 200

    def test_response_maps_all_result_fields(self, client: TestClient) -> None:
        """SignalGenerationResult の全フィールドがレスポンスに正しくマッピングされること。"""
        mock_result = _make_result(
            requested=5,
            saved_rows=85,
            succeeded=[date(2025, 1, 6), date(2025, 1, 7)],
            failed={date(2025, 1, 8): "DB エラー"},
            skipped=[date(2025, 1, 9)],
        )
        with patch("app.api.signals.SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            response = client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                },
            )

        assert response.status_code == 200
        body = response.json()
        assert body["requested"] == 5
        assert body["saved_rows"] == 85
        assert body["succeeded"] == ["2025-01-06", "2025-01-07"]
        assert body["failed"] == {"2025-01-08": "DB エラー"}
        assert body["skipped"] == ["2025-01-09"]
        assert body["has_failure"] is True

    def test_has_failure_false_when_no_failures(self, client: TestClient) -> None:
        """failed が空のとき has_failure は False になること。"""
        mock_result = _make_result(failed={})
        with patch("app.api.signals.SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            response = client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                },
            )
        assert response.json()["has_failure"] is False

    # ------------------------------------------------------------------
    # signal_type / サービス構築
    # ------------------------------------------------------------------

    def test_default_signal_type_is_simple_v1(self, client: TestClient) -> None:
        """signal_type 未指定時は "simple_v1" が使われること。"""
        mock_result = _make_result()
        with patch("app.api.signals.SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                },
            )
        _, kwargs = MockSvc.call_args
        assert kwargs.get("signal_type", "simple_v1") == "simple_v1"

    def test_simple_v1_signal_type_uses_signal_service(self, client: TestClient) -> None:
        """signal_type="simple_v1" では SignalService が使われること。"""
        mock_result = _make_result()
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
        _, kwargs = MockSvc.call_args
        assert kwargs.get("signal_type") == "simple_v1"

    def test_invalid_signal_type_returns_422(self, client: TestClient) -> None:
        """不正な signal_type は Pydantic バリデーションで 422 になること。"""
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2025-01-06",
                "end_date": "2025-01-10",
                "signal_type": "unknown_v99",
            },
        )
        assert response.status_code == 422

    def test_service_is_constructed_with_db_and_calendar(
        self, client: TestClient, mock_db: MagicMock, mock_calendar: MagicMock
    ) -> None:
        """SignalService が db と calendar を受け取って構築されること。"""
        mock_result = _make_result()
        with patch("app.api.signals.SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                },
            )
        args, kwargs = MockSvc.call_args
        assert args[0] is mock_db
        assert args[1] is mock_calendar

    def test_generate_signals_called_with_correct_dates(
        self, client: TestClient
    ) -> None:
        """generate_signals_for_range が正しい日付引数で呼ばれること。"""
        mock_result = _make_result()
        with patch("app.api.signals.SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                },
            )
        MockSvc.return_value.generate_signals_for_range.assert_called_once_with(
            date(2025, 1, 6), date(2025, 1, 10)
        )

    # ------------------------------------------------------------------
    # バリデーション
    # ------------------------------------------------------------------

    def test_start_date_after_end_date_returns_422(self, client: TestClient) -> None:
        """start_date > end_date のとき 422 が返ること。"""
        response = client.post(
            "/signals/generate",
            json={
                "start_date": "2025-01-10",
                "end_date": "2025-01-06",
            },
        )
        assert response.status_code == 422

    def test_start_date_equal_end_date_is_valid(self, client: TestClient) -> None:
        """start_date == end_date は有効 (1日分の生成)。"""
        mock_result = _make_result(requested=1, saved_rows=17)
        with patch("app.api.signals.SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            response = client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-06",
                },
            )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# GET /signals/latest
# ---------------------------------------------------------------------------


class TestGetLatestSignals:
    """GET /signals/latest のテスト群。"""

    # ------------------------------------------------------------------
    # 正常系
    # ------------------------------------------------------------------

    def test_latest_returns_200(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """正常系: 200 が返ること。"""
        mock_db.scalar.return_value = date(2025, 1, 10)
        with patch("app.api.signals.SignalRepository") as MockRepo:
            MockRepo.return_value.list_by_jp_execution_date.return_value = []
            response = client.get("/signals/latest")
        assert response.status_code == 200

    def test_latest_response_contains_required_fields(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """レスポンスに必要なフィールドが含まれること。"""
        mock_db.scalar.return_value = date(2025, 1, 10)
        with patch("app.api.signals.SignalRepository") as MockRepo:
            MockRepo.return_value.list_by_jp_execution_date.return_value = []
            response = client.get("/signals/latest")
        body = response.json()
        assert "jp_execution_date" in body
        assert "signal_type" in body
        assert "signals" in body

    def test_latest_returns_correct_date_and_signals(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """最新日付と各業種のシグナルが正しく返ること。"""
        jp_date = date(2025, 1, 10)
        mock_db.scalar.return_value = jp_date

        row1 = _make_signal_row(
            target_ticker="1617.T",
            us_signal_date=date(2025, 1, 9),
            jp_execution_date=jp_date,
            signal_score=Decimal("0.005000"),
            signal_rank=1,
            suggested_side="long",
        )
        row2 = _make_signal_row(
            target_ticker="1618.T",
            us_signal_date=date(2025, 1, 9),
            jp_execution_date=jp_date,
            signal_score=Decimal("-0.003000"),
            signal_rank=17,
            suggested_side="short",
        )

        with patch("app.api.signals.SignalRepository") as MockRepo:
            MockRepo.return_value.list_by_jp_execution_date.return_value = [row1, row2]
            response = client.get("/signals/latest")

        assert response.status_code == 200
        body = response.json()
        assert body["jp_execution_date"] == "2025-01-10"
        assert body["signal_type"] == "simple_v1"
        assert len(body["signals"]) == 2

        first = body["signals"][0]
        assert first["target_ticker"] == "1617.T"
        assert first["us_signal_date"] == "2025-01-09"
        assert first["jp_execution_date"] == "2025-01-10"
        assert first["signal_rank"] == 1
        assert first["suggested_side"] == "long"

    def test_latest_signal_score_null_when_none(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """signal_score が None の行は null で返ること。"""
        mock_db.scalar.return_value = date(2025, 1, 10)
        row = _make_signal_row(
            signal_score=None,
            signal_rank=None,
            suggested_side="neutral",
        )
        with patch("app.api.signals.SignalRepository") as MockRepo:
            MockRepo.return_value.list_by_jp_execution_date.return_value = [row]
            response = client.get("/signals/latest")

        sig = response.json()["signals"][0]
        assert sig["signal_score"] is None
        assert sig["signal_rank"] is None
        assert sig["suggested_side"] == "neutral"

    # ------------------------------------------------------------------
    # データなし
    # ------------------------------------------------------------------

    def test_latest_returns_empty_when_no_data(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """signal_daily にデータがない場合、jp_execution_date=null, signals=[] が返ること。"""
        mock_db.scalar.return_value = None
        response = client.get("/signals/latest")

        assert response.status_code == 200
        body = response.json()
        assert body["jp_execution_date"] is None
        assert body["signals"] == []

    # ------------------------------------------------------------------
    # signal_type クエリパラメータ
    # ------------------------------------------------------------------

    def test_signal_type_query_param_is_passed(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """signal_type クエリパラメータが SignalRepository に渡されること。"""
        mock_db.scalar.return_value = date(2025, 1, 10)
        with patch("app.api.signals.SignalRepository") as MockRepo:
            MockRepo.return_value.list_by_jp_execution_date.return_value = []
            client.get("/signals/latest?signal_type=pca_v1")

        MockRepo.return_value.list_by_jp_execution_date.assert_called_once_with(
            "pca_v1", date(2025, 1, 10)
        )

    def test_default_signal_type_is_simple_v1_for_latest(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """signal_type 未指定時は "simple_v1" が使われること。"""
        mock_db.scalar.return_value = None
        response = client.get("/signals/latest")
        assert response.json()["signal_type"] == "simple_v1"


# ---------------------------------------------------------------------------
# POST /signals/generate — paper_v1 分岐
# ---------------------------------------------------------------------------


class TestPaperV1SignalType:
    """signal_type="paper_v1" の分岐テスト。"""

    def test_paper_v1_returns_200(self, client: TestClient) -> None:
        """signal_type="paper_v1" で 200 が返ること。"""
        mock_result = _make_result()
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

    def test_paper_v1_uses_paper_v1_signal_service(
        self, client: TestClient, mock_db: MagicMock, mock_calendar: MagicMock
    ) -> None:
        """signal_type="paper_v1" のとき PaperV1SignalService が使われること。"""
        mock_result = _make_result()
        with patch("app.api.signals.PaperV1SignalService") as MockSvc, \
             patch("app.api.signals.SignalService") as MockSimpleSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "signal_type": "paper_v1",
                },
            )
        MockSvc.assert_called_once()
        MockSimpleSvc.assert_not_called()

    def test_paper_v1_service_constructed_with_db_and_calendar(
        self, client: TestClient, mock_db: MagicMock, mock_calendar: MagicMock
    ) -> None:
        """PaperV1SignalService が db と calendar を受け取って構築されること。"""
        mock_result = _make_result()
        with patch("app.api.signals.PaperV1SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "signal_type": "paper_v1",
                },
            )
        args, _ = MockSvc.call_args
        assert args[0] is mock_db
        assert args[1] is mock_calendar

    def test_simple_v1_does_not_use_paper_v1_service(self, client: TestClient) -> None:
        """signal_type="simple_v1" のとき PaperV1SignalService は使われないこと。"""
        mock_result = _make_result()
        with patch("app.api.signals.SignalService") as MockSvc, \
             patch("app.api.signals.PaperV1SignalService") as MockPaperSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-10",
                    "signal_type": "simple_v1",
                },
            )
        MockPaperSvc.assert_not_called()
        MockSvc.assert_called_once()

    def test_paper_v1_response_maps_result_fields(self, client: TestClient) -> None:
        """paper_v1 でも SignalGenerationResult の全フィールドが正しくマッピングされること。"""
        mock_result = _make_result(
            requested=3,
            saved_rows=51,
            succeeded=[date(2025, 1, 6)],
            failed={},
            skipped=[date(2025, 1, 7)],
        )
        with patch("app.api.signals.PaperV1SignalService") as MockSvc:
            MockSvc.return_value.generate_signals_for_range.return_value = mock_result
            response = client.post(
                "/signals/generate",
                json={
                    "start_date": "2025-01-06",
                    "end_date": "2025-01-08",
                    "signal_type": "paper_v1",
                },
            )
        body = response.json()
        assert body["requested"] == 3
        assert body["saved_rows"] == 51
        assert body["succeeded"] == ["2025-01-06"]
        assert body["skipped"] == ["2025-01-07"]
        assert body["has_failure"] is False
