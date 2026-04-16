"""
tests/test_prices_api.py — prices API の単体テスト。

テスト戦略:
  - DB 接続なし: get_db を MagicMock でオーバーライドする
  - PriceService なし: app.api.prices.PriceService を patch する
  - FastAPI TestClient でエンドポイントを HTTP レベルで呼び出す

テスト範囲:
  POST /prices/update
    - scope の分岐 (us / jp / all → 対応するサービスメソッド)
    - FetchResult がレスポンスに正しくマッピングされること
    - scope 不正値 → 422
    - start_date > end_date → 422
    - start_date == end_date → 200 (境界値)

  GET /prices/status
    - 正常応答 (200)
    - レスポンスに必要フィールドが含まれること
    - price_daily にデータがない場合の null 応答
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.database import get_db
from app.main import app
from app.services.price_service import FetchResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db() -> MagicMock:
    """DB セッションのモック。get_db 依存の差し替えに使用する。"""
    return MagicMock()


@pytest.fixture
def client(mock_db: MagicMock):
    """テスト用 TestClient。get_db を mock_db でオーバーライドして返す。"""
    app.dependency_overrides[get_db] = lambda: mock_db
    yield TestClient(app)
    app.dependency_overrides.clear()


def _make_result(**kwargs) -> FetchResult:
    """FetchResult のテスト用ファクトリ。デフォルト値で正常系を作る。"""
    defaults: dict = dict(
        requested=11,
        saved_rows=110,
        succeeded=["XLB"],
        failed={},
        empty=[],
    )
    defaults.update(kwargs)
    return FetchResult(**defaults)


# ---------------------------------------------------------------------------
# POST /prices/update
# ---------------------------------------------------------------------------


class TestUpdatePrices:
    """POST /prices/update のテスト群。"""

    # ------------------------------------------------------------------
    # scope の分岐
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "scope, expected_method",
        [
            ("us", "update_us_prices"),
            ("jp", "update_jp_prices"),
            ("all", "update_all_prices"),
        ],
    )
    def test_scope_routes_to_correct_service_method(
        self, client: TestClient, scope: str, expected_method: str
    ) -> None:
        """scope の値に応じて PriceService の正しいメソッドが呼ばれること。"""
        mock_result = _make_result()

        with patch("app.api.prices.PriceService") as MockSvc:
            getattr(MockSvc.return_value, expected_method).return_value = mock_result
            response = client.post(
                "/prices/update",
                json={
                    "scope": scope,
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-10",
                },
            )

        assert response.status_code == 200
        # 対応するサービスメソッドが正しい引数で呼ばれていること
        getattr(MockSvc.return_value, expected_method).assert_called_once_with(
            date(2025, 1, 1), date(2025, 1, 10)
        )
        # 他のメソッドは呼ばれていないこと
        for other in {"update_us_prices", "update_jp_prices", "update_all_prices"} - {
            expected_method
        }:
            getattr(MockSvc.return_value, other).assert_not_called()

    # ------------------------------------------------------------------
    # レスポンスのフィールドマッピング
    # ------------------------------------------------------------------

    def test_response_maps_all_fetch_result_fields(
        self, client: TestClient
    ) -> None:
        """FetchResult の全フィールドがレスポンスに正しくマッピングされること。"""
        mock_result = _make_result(
            requested=11,
            saved_rows=110,
            succeeded=["XLB", "XLC"],
            failed={"XLE": "fetch エラー"},
            empty=["XLU"],
        )
        with patch("app.api.prices.PriceService") as MockSvc:
            MockSvc.return_value.update_us_prices.return_value = mock_result
            response = client.post(
                "/prices/update",
                json={
                    "scope": "us",
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-10",
                },
            )

        assert response.status_code == 200
        body = response.json()
        assert body["requested"] == 11
        assert body["saved_rows"] == 110
        assert body["succeeded"] == ["XLB", "XLC"]
        assert body["failed"] == {"XLE": "fetch エラー"}
        assert body["empty"] == ["XLU"]
        assert body["has_failure"] is True

    def test_has_failure_false_when_no_failures(self, client: TestClient) -> None:
        """failed が空のとき has_failure は False になること。"""
        mock_result = _make_result(failed={})
        with patch("app.api.prices.PriceService") as MockSvc:
            MockSvc.return_value.update_us_prices.return_value = mock_result
            response = client.post(
                "/prices/update",
                json={
                    "scope": "us",
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-10",
                },
            )

        assert response.json()["has_failure"] is False

    # ------------------------------------------------------------------
    # バリデーション
    # ------------------------------------------------------------------

    def test_invalid_scope_returns_422(self, client: TestClient) -> None:
        """scope に不正値を渡すと 422 が返ること。"""
        response = client.post(
            "/prices/update",
            json={
                "scope": "invalid",
                "start_date": "2025-01-01",
                "end_date": "2025-01-10",
            },
        )
        assert response.status_code == 422

    def test_start_date_after_end_date_returns_422(self, client: TestClient) -> None:
        """start_date > end_date のとき 422 が返ること。"""
        response = client.post(
            "/prices/update",
            json={
                "scope": "us",
                "start_date": "2025-01-10",
                "end_date": "2025-01-01",
            },
        )
        assert response.status_code == 422

    def test_start_date_equal_end_date_is_valid(self, client: TestClient) -> None:
        """start_date == end_date は有効 (1日分の取得)。"""
        mock_result = _make_result(requested=11, saved_rows=11)
        with patch("app.api.prices.PriceService") as MockSvc:
            MockSvc.return_value.update_us_prices.return_value = mock_result
            response = client.post(
                "/prices/update",
                json={
                    "scope": "us",
                    "start_date": "2025-01-10",
                    "end_date": "2025-01-10",
                },
            )

        assert response.status_code == 200

    def test_service_is_constructed_with_db_session(self, client: TestClient, mock_db: MagicMock) -> None:
        """PriceService が DB セッションを受け取って構築されること。"""
        mock_result = _make_result()
        with patch("app.api.prices.PriceService") as MockSvc:
            MockSvc.return_value.update_us_prices.return_value = mock_result
            client.post(
                "/prices/update",
                json={
                    "scope": "us",
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-10",
                },
            )

        MockSvc.assert_called_once_with(mock_db)


# ---------------------------------------------------------------------------
# GET /prices/status
# ---------------------------------------------------------------------------


class TestGetPricesStatus:
    """GET /prices/status のテスト群。"""

    def test_status_returns_200(self, client: TestClient, mock_db: MagicMock) -> None:
        """正常系: 200 とステータス情報が返ること。"""
        mock_db.scalar.side_effect = [28, 1000, date(2025, 1, 10), date(2025, 1, 10)]
        response = client.get("/prices/status")
        assert response.status_code == 200

    def test_status_response_contains_required_fields(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """レスポンスに必要なフィールドが含まれること。"""
        mock_db.scalar.side_effect = [28, 1000, date(2025, 1, 10), date(2025, 1, 9)]
        response = client.get("/prices/status")
        body = response.json()
        assert "instrument_count" in body
        assert "price_count" in body
        assert "latest_us_date" in body
        assert "latest_jp_date" in body

    def test_status_response_values(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """DB から取得した値がレスポンスに反映されること。"""
        mock_db.scalar.side_effect = [28, 500, date(2025, 1, 10), date(2025, 1, 9)]
        response = client.get("/prices/status")
        body = response.json()
        assert body["instrument_count"] == 28
        assert body["price_count"] == 500
        assert body["latest_us_date"] == "2025-01-10"
        assert body["latest_jp_date"] == "2025-01-09"

    def test_status_with_no_price_data_returns_null_dates(
        self, client: TestClient, mock_db: MagicMock
    ) -> None:
        """price_daily にデータがない場合、latest_*_date は null になること。"""
        mock_db.scalar.side_effect = [28, 0, None, None]
        response = client.get("/prices/status")
        body = response.json()
        assert body["price_count"] == 0
        assert body["latest_us_date"] is None
        assert body["latest_jp_date"] is None
