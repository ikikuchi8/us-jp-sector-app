import type { BacktestRunRequest, BacktestRunResponse } from "@/lib/types/backtest";
import { StatItem } from "@/components/common/StatItem";

interface BacktestSummaryCardProps {
  summary: BacktestRunResponse;
  request: BacktestRunRequest;
}

function pct(v: number | null, digits = 2): string | null {
  if (v === null) return null;
  return (v * 100).toFixed(digits) + "%";
}
function fmt(v: number | null, digits = 3): string | null {
  if (v === null) return null;
  return v.toFixed(digits);
}

export function BacktestSummaryCard({ summary, request }: BacktestSummaryCardProps) {
  const totalReturnPct = pct(summary.total_return);
  const totalColor =
    summary.total_return === null
      ? undefined
      : summary.total_return >= 0
      ? "text-green-700"
      : "text-red-700";

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      {/* ヘッダー: run_id と実行条件を明示 */}
      <div className="mb-4">
        <div className="flex items-center gap-3 flex-wrap mb-2">
          <h2 className="text-base font-semibold text-gray-700">サマリー</h2>
          <span className="text-xs font-mono text-gray-400">run_id: {summary.run_id}</span>
        </div>

        {/* 実行条件ブロック — 送信したリクエスト値をそのまま表示 */}
        <div className="rounded border border-gray-100 bg-gray-50 px-3 py-2 text-sm flex flex-wrap gap-x-4 gap-y-1">
          <span>
            <span className="text-gray-400 text-xs">期間</span>{" "}
            <span className="font-medium text-gray-800 tabular-nums">
              {request.start_date} 〜 {request.end_date}
            </span>
          </span>
          <span>
            <span className="text-gray-400 text-xs">種別</span>{" "}
            <span className="font-medium text-gray-800">{request.signal_type ?? "simple_v1"}</span>
          </span>
          <span>
            <span className="text-gray-400 text-xs">手数料</span>{" "}
            <span className="font-medium text-gray-800 tabular-nums">
              {((request.commission_rate ?? 0) * 100).toFixed(3)}%
            </span>
          </span>
          <span>
            <span className="text-gray-400 text-xs">スリッページ</span>{" "}
            <span className="font-medium text-gray-800 tabular-nums">
              {((request.slippage_rate ?? 0) * 100).toFixed(3)}%
            </span>
          </span>
        </div>
      </div>

      {/* 計算結果統計 */}
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
        <StatItem label="取引日数" value={summary.trading_days} unit="日" />
        <StatItem
          label="累積リターン"
          value={totalReturnPct}
          valueClass={totalColor}
        />
        <StatItem label="年率リターン" value={pct(summary.annual_return)} />
        <StatItem label="年率Vola" value={pct(summary.annual_vol)} />
        <StatItem label="シャープレシオ" value={fmt(summary.sharpe_ratio)} />
        <StatItem label="最大DD" value={pct(summary.max_drawdown)} />
      </div>
      <div className="mt-3 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
        <StatItem label="勝率" value={pct(summary.win_rate)} />
      </div>
    </div>
  );
}
