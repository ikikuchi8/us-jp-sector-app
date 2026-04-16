"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import type { BacktestDailyRow } from "@/lib/types/backtest";

interface CumulativeReturnChartProps {
  rows: BacktestDailyRow[];
}

interface ChartPoint {
  date: string;
  value: number | null;
}

function formatPct(v: number) {
  return (v * 100).toFixed(2) + "%";
}

export function CumulativeReturnChart({ rows }: CumulativeReturnChartProps) {
  const data: ChartPoint[] = rows.map((r) => ({
    date: r.jp_execution_date,
    value: r.cumulative_return,
  }));

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      <h2 className="text-base font-semibold text-gray-700 mb-4">累積リターン推移</h2>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={data} margin={{ top: 4, right: 16, left: 8, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 11, fill: "#9ca3af" }}
            tickLine={false}
            // 日付が多い場合は間引き表示
            interval="preserveStartEnd"
          />
          <YAxis
            tickFormatter={(v: number) => formatPct(v)}
            tick={{ fontSize: 11, fill: "#9ca3af" }}
            tickLine={false}
            axisLine={false}
            width={60}
          />
          <Tooltip
            formatter={(value: unknown) =>
              typeof value === "number" ? formatPct(value) : "—"
            }
            labelStyle={{ fontSize: 12, color: "#374151" }}
            contentStyle={{ fontSize: 12 }}
          />
          <ReferenceLine y={0} stroke="#d1d5db" strokeWidth={1} />
          <Line
            type="monotone"
            dataKey="value"
            stroke="#3b82f6"
            strokeWidth={1.5}
            dot={false}
            connectNulls={false}
          />
        </LineChart>
      </ResponsiveContainer>
      <p className="text-xs text-gray-400 mt-2">
        ※ daily_return が null の日 (シグナル・価格欠損) はギャップ表示
      </p>
    </div>
  );
}
