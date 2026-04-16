// backend: app/schemas/price.py に対応する TypeScript 型定義

export interface PricesStatusResponse {
  instrument_count: number;
  price_count: number;
  latest_us_date: string | null; // "YYYY-MM-DD"
  latest_jp_date: string | null;
}

export interface PricesUpdateRequest {
  scope: "us" | "jp" | "all";
  start_date: string; // "YYYY-MM-DD"
  end_date: string;
}

export interface PricesUpdateResponse {
  requested: number;
  saved_rows: number;
  succeeded: string[];
  failed: Record<string, string>;
  empty: string[];
  has_failure: boolean;
}
