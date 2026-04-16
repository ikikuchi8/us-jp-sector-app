"""
backtest router: バックテスト実行・取得 API。

POST /backtest/run          — バックテストを実行し結果サマリーを返す
GET  /backtest/{run_id}     — 保存済みバックテストのサマリーを返す
GET  /backtest/{run_id}/daily — 保存済みバックテストの日次結果を全件返す

# Decimal→float 変換方針
  BacktestRun ORM の Numeric カラムは Decimal で返ってくる。
  このモジュールで明示的に float() キャストして BacktestRunSummaryResponse に渡す。
  BacktestRunResult (BacktestService 返り値) は既に float なので変換不要。
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from app.database import DbSession
from app.repositories.backtest_repository import BacktestRepository
from app.schemas.backtest import (
    BacktestDailyResponse,
    BacktestDailyRow,
    BacktestRunRequest,
    BacktestRunResponse,
    BacktestRunSummaryResponse,
)
from app.services.backtest_service import BacktestService, CostParams
from app.services.calendar_service import CalendarService, get_calendar_service

router = APIRouter()

CalendarDep = Annotated[CalendarService, Depends(get_calendar_service)]


@router.post("/run", response_model=BacktestRunResponse)
def run_backtest(
    body: BacktestRunRequest,
    db: DbSession,
    calendar: CalendarDep,
) -> BacktestRunResponse:
    """バックテストを実行し、完了後にサマリーを返す。

    BacktestService.run() を呼び出し、DB に結果を保存した後、
    実行サマリーを BacktestRunResponse として返す。

    Args:
        body.start_date:      バックテスト開始日 (jp_execution_date 軸)
        body.end_date:        バックテスト終了日 (jp_execution_date 軸)
        body.signal_type:     シグナル種別 ("simple_v1" のみ有効)
        body.commission_rate: 手数料率
        body.slippage_rate:   スリッページ率

    Returns:
        BacktestRunResponse — 実行完了後のサマリー

    Raises:
        422: バリデーションエラー (start_date > end_date, 不正な signal_type 等)
    """
    cost = CostParams(
        commission_rate=body.commission_rate,
        slippage_rate=body.slippage_rate,
    )
    svc = BacktestService(db, calendar)
    result = svc.run(
        start=body.start_date,
        end=body.end_date,
        signal_type=body.signal_type,
        cost_params=cost,
    )

    return BacktestRunResponse(
        run_id=result.run_id,
        status="done",
        signal_type=body.signal_type,
        start_date=body.start_date,
        end_date=body.end_date,
        commission_rate=body.commission_rate,
        slippage_rate=body.slippage_rate,
        trading_days=result.trading_days,
        total_return=result.total_return,
        annual_return=result.annual_return,
        annual_vol=result.annual_vol,
        sharpe_ratio=result.sharpe_ratio,
        max_drawdown=result.max_drawdown,
        win_rate=result.win_rate,
    )


@router.get("/{run_id}", response_model=BacktestRunSummaryResponse)
def get_run(run_id: int, db: DbSession) -> BacktestRunSummaryResponse:
    """保存済みバックテストのサマリーを返す。

    BacktestRun ORM から取得し、Decimal→float 変換を明示的に行って返す。

    Args:
        run_id: 対象の BacktestRun.id

    Returns:
        BacktestRunSummaryResponse — バックテストのサマリー情報

    Raises:
        404: 指定した run_id が存在しない場合
    """
    repo = BacktestRepository(db)
    run = repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"run_id={run_id} が見つかりません")

    # Decimal→float 変換: BacktestRun の Numeric カラムは Decimal で返ってくる
    def _to_float(v) -> float | None:
        return float(v) if v is not None else None

    return BacktestRunSummaryResponse(
        run_id=run.id,
        status=run.status.value,
        signal_type=run.signal_type,
        start_date=run.start_date,
        end_date=run.end_date,
        commission_rate=_to_float(run.commission_rate),
        slippage_rate=_to_float(run.slippage_rate),
        trading_days=run.trading_days,
        total_return=_to_float(run.total_return),
        annual_return=_to_float(run.annual_return),
        annual_vol=_to_float(run.annual_vol),
        sharpe_ratio=_to_float(run.sharpe_ratio),
        max_drawdown=_to_float(run.max_drawdown),
        win_rate=_to_float(run.win_rate),
        started_at=run.started_at,
        finished_at=run.finished_at,
    )


@router.get("/{run_id}/daily", response_model=BacktestDailyResponse)
def get_daily(run_id: int, db: DbSession) -> BacktestDailyResponse:
    """保存済みバックテストの日次結果を全件返す。

    v0_01 ではページネーションなし。全件を jp_execution_date 昇順で返す。

    Args:
        run_id: 対象の BacktestRun.id

    Returns:
        BacktestDailyResponse — 日次結果リスト (run_id と rows)

    Raises:
        404: 指定した run_id が存在しない場合
    """
    repo = BacktestRepository(db)
    if repo.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail=f"run_id={run_id} が見つかりません")

    rows = repo.list_daily_results(run_id)
    return BacktestDailyResponse(
        run_id=run_id,
        rows=[BacktestDailyRow.model_validate(r) for r in rows],
    )
