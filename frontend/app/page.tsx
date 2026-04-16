"use client";

import { useState } from "react";
import { usePriceStatus } from "@/hooks/usePriceStatus";
import { useLatestSignals } from "@/hooks/useLatestSignals";
import { PriceStatusBar } from "@/components/prices/PriceStatusBar";
import { UpdatePricesButton } from "@/components/prices/UpdatePricesButton";
import { GenerateSignalsForm } from "@/components/prices/GenerateSignalsForm";
import { SignalTable } from "@/components/signals/SignalTable";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { ErrorBanner } from "@/components/common/ErrorBanner";
import { EmptyState } from "@/components/common/EmptyState";
import type { SignalType } from "@/lib/types/signals";

export default function MainPage() {
  // フォームとテーブルで共有する signal_type。
  // 変更すると useLatestSignals が自動で再フェッチする。
  const [signalType, setSignalType] = useState<SignalType>("paper_v1");

  const priceStatus = usePriceStatus();
  const signals = useLatestSignals(signalType);

  // 価格更新 or シグナル生成が完了したら両方再取得
  function handleDataUpdate() {
    priceStatus.refetch();
    signals.refetch();
  }

  // シグナルの US 基準日:
  // 全行の us_signal_date を重複除去し、1種類のみなら採用。
  // 複数種類 (通常は起きない) や未取得の場合は null。
  const usSignalDate = (() => {
    const dates = signals.data?.signals.map((s) => s.us_signal_date) ?? [];
    const unique = Array.from(new Set(dates));
    return unique.length === 1 ? unique[0] : null;
  })();

  return (
    <div className="flex flex-col gap-6">
      {/* データ状況バー */}
      <div className="rounded-lg border border-gray-200 bg-white px-5 py-4 flex flex-wrap items-center gap-4 justify-between">
        <PriceStatusBar
          status={priceStatus.data}
          loading={priceStatus.loading}
        />
        <div className="flex items-center gap-3 flex-wrap">
          <UpdatePricesButton onSuccess={handleDataUpdate} />
          <GenerateSignalsForm
            signalType={signalType}
            onSignalTypeChange={setSignalType}
            onSuccess={handleDataUpdate}
          />
        </div>
      </div>

      {/* 価格状況取得エラー */}
      {!priceStatus.loading && priceStatus.error && (
        <ErrorBanner
          message={`価格状況の取得に失敗しました: ${priceStatus.error}`}
          onRetry={priceStatus.refetch}
        />
      )}

      {/* シグナル一覧 */}
      <div>
        {signals.loading && <LoadingSpinner label="シグナルを取得中..." />}

        {!signals.loading && signals.error && (
          <ErrorBanner
            message={`シグナル取得エラー: ${signals.error}`}
            onRetry={signals.refetch}
          />
        )}

        {!signals.loading && !signals.error && signals.data && (
          signals.data.signals.length === 0 ? (
            <EmptyState
              message="シグナルデータがありません"
              hint="上の「シグナル生成」で生成してください"
            />
          ) : (
            <SignalTable
              signals={signals.data.signals}
              jpExecutionDate={signals.data.jp_execution_date}
              usSignalDate={usSignalDate}
              signalType={signals.data.signal_type}
            />
          )
        )}
      </div>
    </div>
  );
}
