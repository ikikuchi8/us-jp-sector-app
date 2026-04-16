"""
prices router: 価格データ更新・状態確認 API。

POST /prices/update  — scope / 期間を指定して価格データを取得・保存する
GET  /prices/status  — instrument_master / price_daily の状態サマリーを返す
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter
from sqlalchemy import func, select

from app.database import DbSession
from app.models.instrument import InstrumentMaster
from app.models.price import PriceDaily
from app.schemas.price import (
    PricesStatusResponse,
    PricesUpdateRequest,
    PricesUpdateResponse,
)
from app.services.price_service import PriceService

router = APIRouter()


@router.post("/update", response_model=PricesUpdateResponse)
def update_prices(body: PricesUpdateRequest, db: DbSession) -> PricesUpdateResponse:
    """scope に応じて US / JP / 全 ticker の価格データを更新する。

    scope に対応する PriceService メソッドを呼び出し、FetchResult をそのまま返す。

    Args:
        body.scope:      "us" → update_us_prices
                         "jp" → update_jp_prices
                         "all" → update_all_prices
        body.start_date: 取得開始日 (inclusive)
        body.end_date:   取得終了日 (inclusive)

    Returns:
        FetchResult の各フィールドを含む PricesUpdateResponse。

    Raises:
        422: scope 不正値 / start_date > end_date (Pydantic バリデーション)
    """
    svc = PriceService(db)

    if body.scope == "us":
        result = svc.update_us_prices(body.start_date, body.end_date)
    elif body.scope == "jp":
        result = svc.update_jp_prices(body.start_date, body.end_date)
    else:  # "all"
        result = svc.update_all_prices(body.start_date, body.end_date)

    return PricesUpdateResponse(
        requested=result.requested,
        saved_rows=result.saved_rows,
        succeeded=result.succeeded,
        failed=result.failed,
        empty=result.empty,
        has_failure=result.has_failure,
    )


@router.get("/status", response_model=PricesStatusResponse)
def get_prices_status(db: DbSession) -> PricesStatusResponse:
    """instrument_master / price_daily の件数と最新 business_date を返す。

    ヘルス確認・データ投入状況の把握に使用する。

    Returns:
        instrument_count: instrument_master の全件数
        price_count:      price_daily の全件数
        latest_us_date:   US price_daily の最新 business_date (データなし → null)
        latest_jp_date:   JP price_daily の最新 business_date (データなし → null)
    """
    instrument_count: int = db.scalar(
        select(func.count()).select_from(InstrumentMaster)
    ) or 0
    price_count: int = db.scalar(
        select(func.count()).select_from(PriceDaily)
    ) or 0
    latest_us_date: date | None = db.scalar(
        select(func.max(PriceDaily.business_date)).where(PriceDaily.market == "US")
    )
    latest_jp_date: date | None = db.scalar(
        select(func.max(PriceDaily.business_date)).where(PriceDaily.market == "JP")
    )

    return PricesStatusResponse(
        instrument_count=instrument_count,
        price_count=price_count,
        latest_us_date=latest_us_date,
        latest_jp_date=latest_jp_date,
    )
