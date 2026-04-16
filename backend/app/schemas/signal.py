"""
signals API の Pydantic リクエスト / レスポンス スキーマ。

SignalsGenerateRequest  — POST /signals/generate のリクエストボディ
SignalsGenerateResponse — POST /signals/generate のレスポンス
SignalRow               — シグナル 1 行分のデータ (GET /signals/latest で使用)
SignalsLatestResponse   — GET /signals/latest のレスポンス
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator


class SignalsGenerateRequest(BaseModel):
    """POST /signals/generate リクエストボディ。

    Attributes:
        start_date:  jp_execution_date の開始日 (inclusive)。
        end_date:    jp_execution_date の終了日 (inclusive)。
        signal_type: シグナル種別。"simple_v1" または "paper_v1"。デフォルト "simple_v1"。

    Raises:
        ValidationError (422): start_date > end_date の場合、または不正な signal_type の場合。
    """

    start_date: date
    end_date: date
    signal_type: Literal["simple_v1", "paper_v1"] = "simple_v1"

    @model_validator(mode="after")
    def validate_date_range(self) -> "SignalsGenerateRequest":
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) は end_date ({self.end_date}) 以前"
                " である必要があります"
            )
        return self


class SignalsGenerateResponse(BaseModel):
    """POST /signals/generate レスポンス。

    SignalGenerationResult の各フィールドをそのまま公開する。

    Attributes:
        requested:   処理対象の jp_execution_date 数。
        saved_rows:  DB に保存した行数。
        succeeded:   正常完了した jp_execution_date のリスト。
        failed:      失敗した jp_execution_date → エラーメッセージ の dict。
        skipped:     US 価格が全欠損でスキップした jp_execution_date のリスト。
        has_failure: failed が 1 件以上あれば True。
    """

    requested: int
    saved_rows: int
    succeeded: list[date]
    failed: dict[str, str]
    skipped: list[date]
    has_failure: bool


class SignalRow(BaseModel):
    """シグナル 1 行分のデータ。GET /signals/latest のレスポンス要素。

    Attributes:
        target_ticker:     JP 業種 ticker (例: "1617.T")
        us_signal_date:    米国側シグナル生成日
        jp_execution_date: 日本市場執行日
        signal_score:      シグナルスコア。全 US 銘柄欠損時は null。
        signal_rank:       業種内ランク (1 = 最強 long)。score が null の場合は null。
        suggested_side:    売買推奨方向 ("long" | "short" | "neutral")
    """

    target_ticker: str
    us_signal_date: date
    jp_execution_date: date
    signal_score: Decimal | None
    signal_rank: int | None
    suggested_side: str

    model_config = ConfigDict(from_attributes=True)


class SignalsLatestResponse(BaseModel):
    """GET /signals/latest レスポンス。

    Attributes:
        jp_execution_date: 最新 jp_execution_date (対象日なしは null)
        signal_type:       シグナル種別
        signals:           17 業種分のシグナル行 (signal_rank 昇順、null は末尾)
    """

    jp_execution_date: date | None
    signal_type: str
    signals: list[SignalRow]
