"""
バックテスト API スキーマ定義。

# スキーマ設計方針
  - BacktestRunRequest  : POST /backtest/run のリクエストボディ
  - BacktestRunResponse : POST /backtest/run のレスポンス (実行完了後サマリー)
  - BacktestRunSummaryResponse : GET /backtest/{run_id} のレスポンス (ORM から構築)
  - BacktestDailyRow   : GET /backtest/{run_id}/daily の 1 行
  - BacktestDailyResponse : GET /backtest/{run_id}/daily のレスポンス

# signal_type 制約
  "simple_v1" と "paper_v1" を受け付ける。
  Literal["simple_v1", "paper_v1"] を使うことで、不正な値は Pydantic が 422 で弾く。

# Decimal→float 変換
  ORM (BacktestRun) は Numeric を Decimal で返す。
  スキーマ層には float で渡すこと。変換は router 層で明示的に行う。
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class BacktestRunRequest(BaseModel):
    """POST /backtest/run リクエストボディ。

    Attributes:
        start_date:      バックテスト開始日 (jp_execution_date 軸、inclusive)
        end_date:        バックテスト終了日 (jp_execution_date 軸、inclusive)
        signal_type:     シグナル種別。"simple_v1" または "paper_v1"。
        commission_rate: ラウンドトリップ手数料率 (0.0 以上)
        slippage_rate:   片道スリッページ率 (0.0 以上)
        run_name:        実行名 (省略可、DB には未保存、ログ用途)
    """

    start_date: date
    end_date: date
    signal_type: Literal["simple_v1", "paper_v1", "paper_v2"] = "simple_v1"
    commission_rate: float = 0.0
    slippage_rate: float = 0.0
    run_name: str | None = None

    @model_validator(mode="after")
    def validate_date_range(self) -> "BacktestRunRequest":
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) は end_date ({self.end_date}) 以前でなければなりません"
            )
        return self

    @field_validator("commission_rate", "slippage_rate")
    @classmethod
    def validate_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("commission_rate / slippage_rate は 0.0 以上でなければなりません")
        return v


class BacktestRunResponse(BaseModel):
    """POST /backtest/run レスポンス。

    BacktestService.run() の結果から直接構築する。
    Decimal→float 変換は router 層で済んでいる前提。

    Attributes:
        run_id:        DB に保存された BacktestRun.id
        status:        "done" 固定 (実行完了後に返すため)
        signal_type:   使用したシグナル種別
        start_date:    バックテスト開始日
        end_date:      バックテスト終了日
        commission_rate: 手数料率
        slippage_rate:   スリッページ率
        trading_days:  有効日次リターンがあった日数
        total_return:  最終累積リターン (取引日ゼロ時は None)
        annual_return: 年率リターン
        annual_vol:    年率ボラティリティ
        sharpe_ratio:  シャープレシオ
        max_drawdown:  最大ドローダウン (正値)
        win_rate:      勝率
    """

    run_id: int
    status: str
    signal_type: str
    start_date: date
    end_date: date
    commission_rate: float
    slippage_rate: float
    trading_days: int
    total_return: float | None
    annual_return: float | None
    annual_vol: float | None
    sharpe_ratio: float | None
    max_drawdown: float | None
    win_rate: float | None


class BacktestRunSummaryResponse(BaseModel):
    """GET /backtest/{run_id} レスポンス。

    BacktestRun ORM オブジェクトから構築する。
    Decimal→float 変換は router 層で明示的に行う。

    Attributes:
        run_id:      BacktestRun.id
        status:      "running" | "done" | "failed"
        signal_type: シグナル種別
        start_date:  開始日 (nullable — モデル側が Optional のため)
        end_date:    終了日 (nullable)
        commission_rate: 手数料率 (nullable)
        slippage_rate:   スリッページ率 (nullable)
        trading_days:    有効取引日数 (nullable)
        total_return:    最終累積リターン (nullable)
        annual_return:   年率リターン (nullable)
        annual_vol:      年率ボラティリティ (nullable)
        sharpe_ratio:    シャープレシオ (nullable)
        max_drawdown:    最大ドローダウン (nullable)
        win_rate:        勝率 (nullable)
        started_at:      実行開始時刻 (UTC)
        finished_at:     実行完了時刻 (UTC、実行中は None)
    """

    run_id: int
    status: str
    signal_type: str
    start_date: date | None
    end_date: date | None
    commission_rate: float | None
    slippage_rate: float | None
    trading_days: int | None
    total_return: float | None
    annual_return: float | None
    annual_vol: float | None
    sharpe_ratio: float | None
    max_drawdown: float | None
    win_rate: float | None
    started_at: datetime
    finished_at: datetime | None


class BacktestDailyRow(BaseModel):
    """GET /backtest/{run_id}/daily の 1 行分。

    BacktestResultDaily ORM オブジェクトから model_validate で構築する。

    Attributes:
        jp_execution_date: JP 執行日
        daily_return:      日次リターン (シグナルまたは価格欠損時は None)
        cumulative_return: 累積リターン (None 日はキャリーオーバー)
        long_return:       long ブック平均リターン (欠損時は None)
        short_return:      short ブック平均リターン (欠損時は None)
        long_count:        有効 long ポジション数 (欠損時は None)
        short_count:       有効 short ポジション数 (欠損時は None)
    """

    jp_execution_date: date
    daily_return: float | None
    cumulative_return: float | None
    long_return: float | None
    short_return: float | None
    long_count: int | None
    short_count: int | None

    model_config = ConfigDict(from_attributes=True)


class BacktestDailyResponse(BaseModel):
    """GET /backtest/{run_id}/daily レスポンス。

    v0_01 では全件返却 (ページネーションなし)。

    Attributes:
        run_id: 対象の BacktestRun.id
        rows:   jp_execution_date 昇順の日次結果リスト
    """

    run_id: int
    rows: list[BacktestDailyRow]
