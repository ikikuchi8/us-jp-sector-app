import type { SignalRow } from "@/lib/types/signals";
import { SignalTableRow } from "./SignalTableRow";

interface SignalTableProps {
  signals: SignalRow[];
  jpExecutionDate: string | null;
  usSignalDate: string | null;
  signalType?: string;
}

export function SignalTable({
  signals,
  jpExecutionDate,
  usSignalDate,
  signalType,
}: SignalTableProps) {
  return (
    <div>
      <div className="flex items-baseline gap-4 mb-3 flex-wrap">
        <h2 className="text-base font-semibold text-gray-700">シグナル一覧</h2>
        {signalType && (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium
                           bg-blue-50 text-blue-700 border border-blue-200">
            {signalType}
          </span>
        )}
        {jpExecutionDate && (
          <span className="text-sm text-gray-500">
            JP執行日: <span className="font-medium text-gray-700">{jpExecutionDate}</span>
            {usSignalDate && (
              <span className="ml-2">(US基準日: {usSignalDate})</span>
            )}
          </span>
        )}
      </div>
      <div className="overflow-x-auto rounded-lg border border-gray-200">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-100 text-gray-600 text-xs uppercase tracking-wide">
              <th className="px-4 py-2 text-center w-12">Rank</th>
              <th className="px-4 py-2 text-left">業種</th>
              <th className="px-4 py-2 text-left">US ETF</th>
              <th className="px-4 py-2 text-right">Score</th>
              <th className="px-4 py-2 text-center">Side</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {signals.map((row, i) => (
              <SignalTableRow key={row.target_ticker} row={row} index={i} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
