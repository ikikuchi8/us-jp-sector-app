"use client";

import { useEffect, useState } from "react";
import { getPricesStatus } from "@/lib/api/prices";
import type { PricesStatusResponse } from "@/lib/types/prices";

interface UsePriceStatusResult {
  data: PricesStatusResponse | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function usePriceStatus(): UsePriceStatusResult {
  const [data, setData] = useState<PricesStatusResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    getPricesStatus()
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
  }, [tick]);

  return { data, loading, error, refetch: () => setTick((t) => t + 1) };
}
