"use client";

import { useState } from "react";
import type { BacktestRunRequest } from "@/lib/types/backtest";
import type { SignalType } from "@/lib/types/signals";
import { SIGNAL_TYPE_LABELS } from "@/lib/types/signals";

export interface BacktestFormValues {
  startDate: string;
  endDate: string;
  signalType: SignalType;
  commissionRate: string;
  slippageRate: string;
}

interface BacktestFormProps {
  values: BacktestFormValues;
  onChange: (v: BacktestFormValues) => void;
  onSubmit: (req: BacktestRunRequest) => void;
  disabled?: boolean;
}

function set<K extends keyof BacktestFormValues>(
  values: BacktestFormValues,
  onChange: (v: BacktestFormValues) => void,
  key: K,
  val: BacktestFormValues[K],
) {
  onChange({ ...values, [key]: val });
}

export function BacktestForm({ values, onChange, onSubmit, disabled }: BacktestFormProps) {
  const [error, setError] = useState<string | null>(null);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (values.startDate > values.endDate) {
      setError("開始日は終了日以前でなければなりません");
      return;
    }
    const commission = parseFloat(values.commissionRate);
    const slippage = parseFloat(values.slippageRate);
    if (isNaN(commission) || commission < 0) {
      setError("手数料率は 0 以上の数値を入力してください");
      return;
    }
    if (isNaN(slippage) || slippage < 0) {
      setError("スリッページ率は 0 以上の数値を入力してください");
      return;
    }
    onSubmit({
      start_date: values.startDate,
      end_date: values.endDate,
      signal_type: values.signalType,
      commission_rate: commission,
      slippage_rate: slippage,
    });
  }

  return (
    <form onSubmit={handleSubmit} className="rounded-lg border border-gray-200 bg-white p-6">
      <h2 className="text-base font-semibold text-gray-700 mb-4">バックテスト実行</h2>
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <div>
          <label className="block text-xs text-gray-500 mb-1">開始日</label>
          <input
            type="date"
            value={values.startDate}
            onChange={(e) => set(values, onChange, "startDate", e.target.value)}
            disabled={disabled}
            className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm
                       focus:outline-none focus:ring-1 focus:ring-blue-400 disabled:opacity-50"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">終了日</label>
          <input
            type="date"
            value={values.endDate}
            onChange={(e) => set(values, onChange, "endDate", e.target.value)}
            disabled={disabled}
            className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm
                       focus:outline-none focus:ring-1 focus:ring-blue-400 disabled:opacity-50"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">手数料率</label>
          <input
            type="number"
            step="0.0001"
            min="0"
            value={values.commissionRate}
            onChange={(e) => set(values, onChange, "commissionRate", e.target.value)}
            disabled={disabled}
            className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm
                       focus:outline-none focus:ring-1 focus:ring-blue-400 disabled:opacity-50"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">スリッページ率</label>
          <input
            type="number"
            step="0.0001"
            min="0"
            value={values.slippageRate}
            onChange={(e) => set(values, onChange, "slippageRate", e.target.value)}
            disabled={disabled}
            className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm
                       focus:outline-none focus:ring-1 focus:ring-blue-400 disabled:opacity-50"
          />
        </div>
      </div>
      <div className="mt-3">
        <label className="block text-xs text-gray-500 mb-1">シグナル種別</label>
        <select
          value={values.signalType}
          onChange={(e) => set(values, onChange, "signalType", e.target.value as SignalType)}
          disabled={disabled}
          className="border border-gray-300 rounded px-2 py-1.5 text-sm text-gray-700 bg-white
                     focus:outline-none focus:ring-1 focus:ring-blue-400 disabled:opacity-50"
        >
          {(Object.keys(SIGNAL_TYPE_LABELS) as SignalType[]).map((t) => (
            <option key={t} value={t}>
              {SIGNAL_TYPE_LABELS[t]}
            </option>
          ))}
        </select>
      </div>
      {error && (
        <p className="mt-2 text-xs text-red-600">{error}</p>
      )}
      <div className="mt-4">
        <button
          type="submit"
          disabled={disabled}
          className="px-4 py-2 rounded bg-blue-600 text-white text-sm font-medium
                     hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed
                     flex items-center gap-2 transition-colors"
        >
          {disabled && (
            <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"/>
            </svg>
          )}
          {disabled ? "実行中..." : "実行する"}
        </button>
      </div>
    </form>
  );
}
