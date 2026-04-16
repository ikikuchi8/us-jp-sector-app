// backend: app/schemas/signal.py に対応する TypeScript 型定義

/** 有効なシグナル種別。backend の Literal["simple_v1", "paper_v1"] に対応。 */
export type SignalType = "simple_v1" | "paper_v1";

/** SignalType の表示ラベル。UI の select / badge で使用する。 */
export const SIGNAL_TYPE_LABELS: Record<SignalType, string> = {
  simple_v1: "simple_v1",
  paper_v1: "paper_v1 (PCA+Ridge)",
};

export interface SignalRow {
  target_ticker: string;       // "1617.T" など
  us_signal_date: string;      // "YYYY-MM-DD"
  jp_execution_date: string;   // "YYYY-MM-DD"
  signal_score: string | null; // Decimal → string (JSON シリアライズ)
  signal_rank: number | null;
  suggested_side: "long" | "short" | "neutral";
}

export interface SignalsLatestResponse {
  jp_execution_date: string | null;
  signal_type: string;
  signals: SignalRow[];
}

export interface SignalsGenerateRequest {
  start_date: string;
  end_date: string;
  signal_type?: SignalType;
}

export interface SignalsGenerateResponse {
  requested: number;
  saved_rows: number;
  succeeded: string[];
  failed: Record<string, string>;
  skipped: string[];
  has_failure: boolean;
}
