"use client";

import { useState } from "react";
import { runBacktest, getDailyResults } from "@/lib/api/backtest";
import type { BacktestRunRequest, BacktestRunResponse, BacktestDailyRow } from "@/lib/types/backtest";

type BacktestPhase = "idle" | "running" | "done" | "error";

interface BacktestDoneState {
  phase: "done";
  runId: number;
  request: BacktestRunRequest;
  summary: BacktestRunResponse;
  daily: BacktestDailyRow[];
}

type BacktestState =
  | { phase: "idle" }
  | { phase: "running" }
  | BacktestDoneState
  | { phase: "error"; message: string };

interface UseBacktestResult {
  state: BacktestState;
  execute: (req: BacktestRunRequest) => Promise<void>;
  reset: () => void;
}

export function useBacktest(): UseBacktestResult {
  const [state, setState] = useState<BacktestState>({ phase: "idle" });

  async function execute(req: BacktestRunRequest): Promise<void> {
    setState({ phase: "running" });
    try {
      const summary = await runBacktest(req);
      const dailyRes = await getDailyResults(summary.run_id);
      setState({ phase: "done", runId: summary.run_id, request: req, summary, daily: dailyRes.rows });
    } catch (e: unknown) {
      setState({
        phase: "error",
        message: e instanceof Error ? e.message : "バックテストに失敗しました",
      });
    }
  }

  function reset(): void {
    setState({ phase: "idle" });
  }

  return { state, execute, reset };
}
