// backend: app/schemas/backtest.py に対応する TypeScript 型定義

import type { SignalType } from "@/lib/types/signals";

export interface BacktestRunRequest {
  start_date: string;       // "YYYY-MM-DD"
  end_date: string;
  signal_type?: SignalType;
  commission_rate?: number;
  slippage_rate?: number;
}

export interface BacktestRunResponse {
  run_id: number;
  status: string;
  signal_type: string;
  start_date: string;
  end_date: string;
  commission_rate: number;
  slippage_rate: number;
  trading_days: number;
  total_return: number | null;
  annual_return: number | null;
  annual_vol: number | null;
  sharpe_ratio: number | null;
  max_drawdown: number | null;
  win_rate: number | null;
}

export interface BacktestDailyRow {
  jp_execution_date: string;
  daily_return: number | null;
  cumulative_return: number | null;
  long_return: number | null;
  short_return: number | null;
  long_count: number | null;
  short_count: number | null;
}

export interface BacktestDailyResponse {
  run_id: number;
  rows: BacktestDailyRow[];
}
