"use client";

import { useState, useEffect } from "react";
import { useBacktest } from "@/hooks/useBacktest";
import { BacktestForm } from "@/components/backtest/BacktestForm";
import type { BacktestFormValues } from "@/components/backtest/BacktestForm";
import { BacktestSummaryCard } from "@/components/backtest/BacktestSummaryCard";
import { CumulativeReturnChart } from "@/components/backtest/CumulativeReturnChart";
import { DailyResultTable } from "@/components/backtest/DailyResultTable";
import { ErrorBanner } from "@/components/common/ErrorBanner";
import { EmptyState } from "@/components/common/EmptyState";
import type { BacktestRunRequest } from "@/lib/types/backtest";

const SESSION_KEY = "backtest_form";

function buildDefaultValues(): BacktestFormValues {
  const end = new Date();
  const start = new Date();
  start.setMonth(start.getMonth() - 3);
  return {
    startDate: start.toISOString().slice(0, 10),
    endDate: end.toISOString().slice(0, 10),
    signalType: "paper_v1",
    commissionRate: "0.001",
    slippageRate: "0.001",
  };
}

export default function BacktestPage() {
  // SSR-safe: lazy initializer にはデフォルト値のみ使用。
  // sessionStorage の復元は useEffect で行う（hydration mismatch を防ぐ）。
  const [formValues, setFormValues] = useState<BacktestFormValues>(buildDefaultValues);
  const { state, execute, reset } = useBacktest();
  const isRunning = state.phase === "running";

  // mount 後に sessionStorage から前回の入力値を復元
  useEffect(() => {
    try {
      const raw = sessionStorage.getItem(SESSION_KEY);
      if (raw) {
        const saved = JSON.parse(raw) as Partial<BacktestFormValues>;
        setFormValues((prev) => ({ ...prev, ...saved }));
      }
    } catch {
      // sessionStorage 読み取り失敗 — デフォルトのまま続行
    }
  }, []);

  function handleValuesChange(v: BacktestFormValues) {
    setFormValues(v);
    try {
      sessionStorage.setItem(SESSION_KEY, JSON.stringify(v));
    } catch {
      // sessionStorage 書き込み失敗 — 無視
    }
  }

  async function handleSubmit(req: BacktestRunRequest) {
    await execute(req);
  }

  return (
    <div className="flex flex-col gap-6">
      <BacktestForm
        values={formValues}
        onChange={handleValuesChange}
        onSubmit={handleSubmit}
        disabled={isRunning}
      />

      {state.phase === "error" && (
        <ErrorBanner
          message={`バックテストエラー: ${state.message}`}
          onRetry={reset}
        />
      )}

      {state.phase === "done" && (
        <>
          <BacktestSummaryCard summary={state.summary} request={state.request} />
          {state.daily.length === 0 ? (
            <EmptyState
              message="日次結果がありません"
              hint="指定期間に JP 営業日が存在しないか、シグナルが未生成の可能性があります"
            />
          ) : (
            <>
              <CumulativeReturnChart rows={state.daily} />
              <DailyResultTable rows={state.daily} />
            </>
          )}
        </>
      )}
    </div>
  );
}
