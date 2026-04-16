import type { SignalRow } from "@/lib/types/signals";
import { SignalBadge } from "@/components/signals/SignalBadge";

interface SectorSignalCardProps {
  signal: SignalRow;
}

export function SectorSignalCard({ signal }: SectorSignalCardProps) {
  const score =
    signal.signal_score !== null ? parseFloat(signal.signal_score) : null;
  const scoreStr =
    score !== null ? (score >= 0 ? `+${score.toFixed(6)}` : score.toFixed(6)) : "—";
  const scoreColor =
    score === null ? "text-gray-400" : score > 0 ? "text-green-700" : "text-red-700";

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-4">
        最新シグナル
      </h2>
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <div>
          <p className="text-xs text-gray-500">JP執行日</p>
          <p className="text-base font-semibold text-gray-800 mt-0.5">
            {signal.jp_execution_date}
          </p>
        </div>
        <div>
          <p className="text-xs text-gray-500">US基準日</p>
          <p className="text-base font-semibold text-gray-800 mt-0.5">
            {signal.us_signal_date}
          </p>
        </div>
        <div>
          <p className="text-xs text-gray-500">スコア</p>
          <p className={`text-base font-semibold font-mono mt-0.5 ${scoreColor}`}>
            {scoreStr}
          </p>
        </div>
        <div>
          <p className="text-xs text-gray-500">ランク / 推奨</p>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-base font-semibold text-gray-800">
              {signal.signal_rank ?? "—"}
              <span className="text-xs text-gray-400"> / 17</span>
            </span>
            <SignalBadge side={signal.suggested_side} />
          </div>
        </div>
      </div>
    </div>
  );
}
