"""
価格 API の Pydantic リクエスト / レスポンス スキーマ。

PricesUpdateRequest  — POST /prices/update のリクエストボディ
PricesUpdateResponse — POST /prices/update のレスポンス
PricesStatusResponse — GET  /prices/status  のレスポンス
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, model_validator


class PricesUpdateRequest(BaseModel):
    """POST /prices/update リクエストボディ。

    Attributes:
        scope:      更新対象市場。"us" / "jp" / "all" のいずれか。
        start_date: 取得開始日 (inclusive)。
        end_date:   取得終了日 (inclusive)。

    Raises:
        ValidationError (422): scope が不正値、または start_date > end_date の場合。
    """

    scope: Literal["us", "jp", "all"]
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_date_range(self) -> "PricesUpdateRequest":
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) は end_date ({self.end_date}) 以前"
                " である必要があります"
            )
        return self


class PricesUpdateResponse(BaseModel):
    """POST /prices/update レスポンス。

    PriceService.FetchResult の各フィールドをそのまま公開する。

    Attributes:
        requested:   要求 ticker 数。
        saved_rows:  DB に保存した行数。
        succeeded:   正常保存できた ticker リスト。
        failed:      失敗した ticker → エラーメッセージ の dict。
        empty:       データなし ticker リスト (エラーではない)。
        has_failure: failed が 1 件以上あれば True。
    """

    requested: int
    saved_rows: int
    succeeded: list[str]
    failed: dict[str, str]
    empty: list[str]
    has_failure: bool


class PricesStatusResponse(BaseModel):
    """GET /prices/status レスポンス。

    Attributes:
        instrument_count: instrument_master の全件数。
        price_count:      price_daily の全件数。
        latest_us_date:   US price_daily の最新 business_date。データなしは null。
        latest_jp_date:   JP price_daily の最新 business_date。データなしは null。
    """

    instrument_count: int
    price_count: int
    latest_us_date: date | None
    latest_jp_date: date | None
