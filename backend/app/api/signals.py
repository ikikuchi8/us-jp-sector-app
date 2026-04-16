"""
signals router: シグナル生成・取得 API。

POST /signals/generate — 指定期間の jp_execution_date に対してシグナルを生成・保存する
GET  /signals/latest   — 最新 jp_execution_date のシグナルを返す
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select, func

from app.database import DbSession
from app.models.signal import SignalDaily
from app.repositories.signal_repository import SignalRepository
from app.schemas.signal import (
    SignalRow,
    SignalsGenerateRequest,
    SignalsGenerateResponse,
    SignalsLatestResponse,
)
from app.services.calendar_service import CalendarService, get_calendar_service
from app.services.paper_v1_signal_service import (
    SIGNAL_TYPE_PAPER_V1,
    PaperV1SignalService,
)
from app.services.signal_service import SIGNAL_TYPE_SIMPLE_V1, SignalService

router = APIRouter()

CalendarDep = Annotated[CalendarService, Depends(get_calendar_service)]


@router.post("/generate", response_model=SignalsGenerateResponse)
def generate_signals(
    body: SignalsGenerateRequest,
    db: DbSession,
    calendar: CalendarDep,
) -> SignalsGenerateResponse:
    """指定期間の jp_execution_date に対してシグナルを生成・保存する。

    CalendarService.build_date_alignment() を使って (jp_execution_date, us_signal_date)
    のペアを決定し、SignalService が先読みを防止しながら US リターンを計算する。

    Args:
        body.start_date:  jp_execution_date の開始日 (inclusive)
        body.end_date:    jp_execution_date の終了日 (inclusive)
        body.signal_type: シグナル種別 ("simple_v1" または "paper_v1")

    Returns:
        SignalsGenerateResponse — 処理件数・成功・失敗・スキップの各リスト

    Raises:
        422: start_date > end_date または不正な signal_type (Pydantic バリデーション)
    """
    if body.signal_type == SIGNAL_TYPE_PAPER_V1:
        svc: SignalService | PaperV1SignalService = PaperV1SignalService(db, calendar)
    else:
        svc = SignalService(db, calendar, signal_type=body.signal_type)
    result = svc.generate_signals_for_range(body.start_date, body.end_date)

    return SignalsGenerateResponse(
        requested=result.requested,
        saved_rows=result.saved_rows,
        succeeded=result.succeeded,
        failed={str(k): v for k, v in result.failed.items()},
        skipped=result.skipped,
        has_failure=result.has_failure,
    )


@router.get("/latest", response_model=SignalsLatestResponse)
def get_latest_signals(
    db: DbSession,
    signal_type: str = SIGNAL_TYPE_SIMPLE_V1,
) -> SignalsLatestResponse:
    """最新 jp_execution_date のシグナルを全業種分返す。

    signal_daily テーブルから最新の jp_execution_date を特定し、
    その日付の全業種シグナルを signal_rank 昇順 (null は末尾) で返す。
    データがない場合は jp_execution_date=null, signals=[] を返す。

    Args:
        signal_type: シグナル種別 (クエリパラメータ、デフォルト "simple_v1")

    Returns:
        SignalsLatestResponse — 最新日付と 17 業種分のシグナルリスト
    """
    latest_date = db.scalar(
        select(func.max(SignalDaily.jp_execution_date)).where(
            SignalDaily.signal_type == signal_type
        )
    )

    if latest_date is None:
        return SignalsLatestResponse(
            jp_execution_date=None,
            signal_type=signal_type,
            signals=[],
        )

    repo = SignalRepository(db)
    rows = repo.list_by_jp_execution_date(signal_type, latest_date)

    return SignalsLatestResponse(
        jp_execution_date=latest_date,
        signal_type=signal_type,
        signals=[SignalRow.model_validate(r) for r in rows],
    )
