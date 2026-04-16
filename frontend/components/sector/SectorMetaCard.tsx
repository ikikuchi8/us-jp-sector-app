import type { SectorInfo } from "@/lib/sectorMapping";

interface SectorMetaCardProps {
  sector: SectorInfo;
}

export function SectorMetaCard({ sector }: SectorMetaCardProps) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-4">
        業種対応情報
      </h2>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        <div>
          <p className="text-xs text-gray-500">JP業種名</p>
          <p className="text-base font-semibold text-gray-800 mt-0.5">
            {sector.jpSectorName}
          </p>
        </div>
        <div>
          <p className="text-xs text-gray-500">JP Ticker</p>
          <p className="text-base font-semibold font-mono text-gray-800 mt-0.5">
            {sector.jpTicker}
          </p>
        </div>
        <div>
          <p className="text-xs text-gray-500">対応 US ETF</p>
          <p className="text-base font-semibold font-mono text-gray-800 mt-0.5">
            {sector.usTickers.join(", ")}
          </p>
        </div>
      </div>
    </div>
  );
}
