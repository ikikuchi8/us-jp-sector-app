"use client";

import { useState } from "react";
import { generateSignals } from "@/lib/api/signals";
import type { SignalType } from "@/lib/types/signals";
import { SIGNAL_TYPE_LABELS } from "@/lib/types/signals";

interface GenerateSignalsFormProps {
  signalType: SignalType;
  onSignalTypeChange: (t: SignalType) => void;
  onSuccess?: () => void;
}

function todayStr(): string {
  return new Date().toISOString().slice(0, 10);
}
function nDaysAgoStr(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

export function GenerateSignalsForm({
  signalType,
  onSignalTypeChange,
  onSuccess,
}: GenerateSignalsFormProps) {
  const [start, setStart] = useState(nDaysAgoStr(7));
  const [end, setEnd] = useState(todayStr());
  const [loading, setLoading] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setToast(null);
    try {
      const result = await generateSignals({
        start_date: start,
        end_date: end,
        signal_type: signalType,
      });
      const allSkipped =
        result.saved_rows === 0 &&
        result.skipped.length > 0 &&
        result.skipped.length === result.requested;
      const msg = result.has_failure
        ? `生成完了 (失敗 ${Object.keys(result.failed).length}件 / ${result.requested}日)`
        : allSkipped
        ? `生成完了 (0行保存 / ${result.requested}日 — 全日スキップ。価格履歴が不足している可能性があります)`
        : `生成完了 (${result.saved_rows}行保存 / ${result.requested}日)`;
      setToast(msg);
      onSuccess?.();
    } catch (e: unknown) {
      setToast(e instanceof Error ? e.message : "生成に失敗しました");
    } finally {
      setLoading(false);
      setTimeout(() => setToast(null), 4000);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="flex items-center gap-2 flex-wrap">
      <input
        type="date"
        value={start}
        onChange={(e) => setStart(e.target.value)}
        className="border border-gray-300 rounded px-2 py-1 text-sm text-gray-700
                   focus:outline-none focus:ring-1 focus:ring-blue-400"
      />
      <span className="text-gray-400 text-sm">〜</span>
      <input
        type="date"
        value={end}
        onChange={(e) => setEnd(e.target.value)}
        className="border border-gray-300 rounded px-2 py-1 text-sm text-gray-700
                   focus:outline-none focus:ring-1 focus:ring-blue-400"
      />
      <select
        value={signalType}
        onChange={(e) => onSignalTypeChange(e.target.value as SignalType)}
        className="border border-gray-300 rounded px-2 py-1 text-sm text-gray-700
                   focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white"
      >
        {(Object.keys(SIGNAL_TYPE_LABELS) as SignalType[]).map((t) => (
          <option key={t} value={t}>
            {SIGNAL_TYPE_LABELS[t]}
          </option>
        ))}
      </select>
      <div className="relative">
        <button
          type="submit"
          disabled={loading}
          className="px-3 py-1.5 rounded bg-green-600 text-white text-sm font-medium
                     hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed
                     flex items-center gap-1.5 transition-colors"
        >
          {loading && (
            <svg className="animate-spin h-3.5 w-3.5" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"/>
            </svg>
          )}
          シグナル生成
        </button>
        {toast && (
          <div className="absolute top-full mt-1 left-0 z-20 bg-gray-800 text-white text-xs
                          px-3 py-1.5 rounded shadow-lg whitespace-nowrap">
            {toast}
          </div>
        )}
      </div>
    </form>
  );
}
