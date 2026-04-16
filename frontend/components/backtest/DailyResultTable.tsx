import type { BacktestDailyRow } from "@/lib/types/backtest";

interface DailyResultTableProps {
  rows: BacktestDailyRow[];
}

function pct(v: number | null): string {
  if (v === null) return "—";
  return (v * 100).toFixed(3) + "%";
}

function retColor(v: number | null): string {
  if (v === null) return "text-gray-400";
  return v >= 0 ? "text-green-700" : "text-red-700";
}

export function DailyResultTable({ rows }: DailyResultTableProps) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      <h2 className="text-base font-semibold text-gray-700 mb-3">日次結果</h2>
      <div className="overflow-x-auto max-h-96 overflow-y-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-gray-100">
            <tr className="text-gray-500 uppercase tracking-wide">
              <th className="px-3 py-2 text-left">日付</th>
              <th className="px-3 py-2 text-right">日次リターン</th>
              <th className="px-3 py-2 text-right">累積リターン</th>
              <th className="px-3 py-2 text-right">Long</th>
              <th className="px-3 py-2 text-right">Short</th>
              <th className="px-3 py-2 text-center">L数</th>
              <th className="px-3 py-2 text-center">S数</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {rows.map((row) => (
              <tr
                key={row.jp_execution_date}
                className={`hover:bg-gray-50 ${row.daily_return === null ? "opacity-50" : ""}`}
              >
                <td className="px-3 py-1.5 text-gray-600 tabular-nums">
                  {row.jp_execution_date}
                </td>
                <td className={`px-3 py-1.5 text-right tabular-nums font-mono ${retColor(row.daily_return)}`}>
                  {pct(row.daily_return)}
                </td>
                <td className={`px-3 py-1.5 text-right tabular-nums font-mono ${retColor(row.cumulative_return)}`}>
                  {pct(row.cumulative_return)}
                </td>
                <td className={`px-3 py-1.5 text-right tabular-nums font-mono ${retColor(row.long_return)}`}>
                  {pct(row.long_return)}
                </td>
                <td className={`px-3 py-1.5 text-right tabular-nums font-mono ${retColor(row.short_return)}`}>
                  {pct(row.short_return)}
                </td>
                <td className="px-3 py-1.5 text-center text-gray-500">
                  {row.long_count ?? "—"}
                </td>
                <td className="px-3 py-1.5 text-center text-gray-500">
                  {row.short_count ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-gray-400 mt-2">
        全 {rows.length} 日 / daily_return が null の行はシグナル・価格欠損日
      </p>
    </div>
  );
}
