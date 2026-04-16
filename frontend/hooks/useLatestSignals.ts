"use client";

import { useEffect, useState } from "react";
import { getLatestSignals } from "@/lib/api/signals";
import type { SignalsLatestResponse } from "@/lib/types/signals";

interface UseLatestSignalsResult {
  data: SignalsLatestResponse | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useLatestSignals(
  signalType = "simple_v1"
): UseLatestSignalsResult {
  const [data, setData] = useState<SignalsLatestResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    getLatestSignals(signalType)
      .then((res) => {
        if (!cancelled) setData(res);
      })
      .catch((e: unknown) => {
        if (!cancelled)
          setError(e instanceof Error ? e.message : "取得に失敗しました");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [tick, signalType]);

  return { data, loading, error, refetch: () => setTick((t) => t + 1) };
}
