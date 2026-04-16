import type { PricesStatusResponse } from "@/lib/types/prices";

interface PriceStatusBarProps {
  status: PricesStatusResponse | null;
  loading: boolean;
}

// v0_01 固定値: US=11銘柄、JP=17銘柄
const US_TICKER_COUNT = 11;
const JP_TICKER_COUNT = 17;

export function PriceStatusBar({ status, loading }: PriceStatusBarProps) {
  if (loading) {
    return (
      <div className="text-xs text-gray-400 animate-pulse">データ状況を確認中...</div>
    );
  }

  return (
    <div className="flex flex-wrap gap-6 text-sm">
      <div>
        <span className="text-gray-500">価格データ (US):</span>{" "}
        <span className="font-medium text-gray-700">
          {US_TICKER_COUNT}銘柄
          {status?.latest_us_date ? ` / 最終取得 ${status.latest_us_date}` : " / データなし"}
        </span>
      </div>
      <div>
        <span className="text-gray-500">価格データ (JP):</span>{" "}
        <span className="font-medium text-gray-700">
          {JP_TICKER_COUNT}銘柄
          {status?.latest_jp_date ? ` / 最終取得 ${status.latest_jp_date}` : " / データなし"}
        </span>
      </div>
      {status && (
        <div>
          <span className="text-gray-500">総レコード数:</span>{" "}
          <span className="font-medium text-gray-700">
            {status.price_count.toLocaleString()}行
          </span>
        </div>
      )}
    </div>
  );
}
