"use client";

import { useState } from "react";
import { updatePrices } from "@/lib/api/prices";

interface UpdatePricesButtonProps {
  onSuccess?: () => void;
}

// 直近 7 日間を更新する (v0_01 デフォルト)
function getDateRange(): { start: string; end: string } {
  const end = new Date();
  const start = new Date();
  start.setDate(end.getDate() - 7);
  const fmt = (d: Date) => d.toISOString().slice(0, 10);
  return { start: fmt(start), end: fmt(end) };
}

export function UpdatePricesButton({ onSuccess }: UpdatePricesButtonProps) {
  const [loading, setLoading] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  async function handleClick() {
    setLoading(true);
    setToast(null);
    const { start, end } = getDateRange();
    try {
      const result = await updatePrices({ scope: "all", start_date: start, end_date: end });
      const msg = result.has_failure
        ? `更新完了 (一部失敗: ${Object.keys(result.failed).length}件)`
        : `更新完了 (${result.saved_rows}行保存)`;
      setToast(msg);
      onSuccess?.();
    } catch (e: unknown) {
      setToast(e instanceof Error ? e.message : "更新に失敗しました");
    } finally {
      setLoading(false);
      setTimeout(() => setToast(null), 4000);
    }
  }

  return (
    <div className="relative inline-block">
      <button
        onClick={handleClick}
        disabled={loading}
        className="px-3 py-1.5 rounded bg-blue-600 text-white text-sm font-medium
                   hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed
                   flex items-center gap-1.5 transition-colors"
      >
        {loading && (
          <svg className="animate-spin h-3.5 w-3.5" viewBox="0 0 24 24" fill="none">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"/>
          </svg>
        )}
        価格更新
      </button>
      {toast && (
        <div className="absolute top-full mt-1 left-0 z-20 bg-gray-800 text-white text-xs
                        px-3 py-1.5 rounded shadow-lg whitespace-nowrap">
          {toast}
        </div>
      )}
    </div>
  );
}
