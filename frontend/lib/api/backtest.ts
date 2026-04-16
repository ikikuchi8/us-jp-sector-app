import { apiFetch } from "./client";
import type {
  BacktestDailyResponse,
  BacktestRunRequest,
  BacktestRunResponse,
} from "@/lib/types/backtest";

export function runBacktest(
  body: BacktestRunRequest
): Promise<BacktestRunResponse> {
  return apiFetch<BacktestRunResponse>("/backtest/run", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getDailyResults(runId: number): Promise<BacktestDailyResponse> {
  return apiFetch<BacktestDailyResponse>(`/backtest/${runId}/daily`);
}
