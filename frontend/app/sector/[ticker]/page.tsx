"use client";

import Link from "next/link";
import { useLatestSignals } from "@/hooks/useLatestSignals";
import { SECTOR_BY_TICKER } from "@/lib/sectorMapping";
import { SectorSignalCard } from "@/components/sector/SectorSignalCard";
import { SectorMetaCard } from "@/components/sector/SectorMetaCard";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { ErrorBanner } from "@/components/common/ErrorBanner";
import { EmptyState } from "@/components/common/EmptyState";

// Next.js 14 では params はプレーンオブジェクト。use() は不要。
interface PageProps {
  params: { ticker: string };
}

export default function SectorDetailPage({ params }: PageProps) {
  const decodedTicker = decodeURIComponent(params.ticker);

  const { data, loading, error, refetch } = useLatestSignals();
  const sector = SECTOR_BY_TICKER[decodedTicker];
  const signal = data?.signals.find((s) => s.target_ticker === decodedTicker);

  return (
    <div className="flex flex-col gap-5">
      {/* パンくず */}
      <div className="flex items-center gap-2 text-sm">
        <Link href="/" className="text-blue-600 hover:underline">
          メイン
        </Link>
        <span className="text-gray-400">/</span>
        <span className="text-gray-600">
          {sector?.jpSectorName ?? decodedTicker}
        </span>
      </div>

      {/* ページタイトル */}
      <div>
        <h1 className="text-xl font-bold text-gray-800">
          {sector?.jpSectorName ?? decodedTicker}
        </h1>
        <p className="text-sm text-gray-500 mt-0.5">{decodedTicker}</p>
      </div>

      {/* 状態表示 */}
      {loading && <LoadingSpinner label="シグナルを取得中..." />}

      {!loading && error && (
        <ErrorBanner
          message={`取得エラー: ${error}`}
          onRetry={refetch}
        />
      )}

      {!loading && !error && !sector && (
        <EmptyState
          message={`Ticker "${decodedTicker}" はセクターマスタに存在しません`}
        />
      )}

      {!loading && !error && sector && !signal && (
        <EmptyState
          message="このティッカーの最新シグナルがありません"
          hint="メイン画面からシグナルを生成してください"
        />
      )}

      {/* シグナル詳細 */}
      {!loading && !error && sector && signal && (
        <>
          <SectorSignalCard signal={signal} />
          <SectorMetaCard sector={sector} />
        </>
      )}

      {/* セクターマスタはあるがシグナルがない場合でもメタ表示 */}
      {!loading && !error && sector && !signal && (
        <SectorMetaCard sector={sector} />
      )}
    </div>
  );
}
