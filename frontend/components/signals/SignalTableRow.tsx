import Link from "next/link";
import type { SignalRow } from "@/lib/types/signals";
import { SignalBadge } from "./SignalBadge";
import { SECTOR_BY_TICKER } from "@/lib/sectorMapping";

interface SignalTableRowProps {
  row: SignalRow;
  index: number;
}

export function SignalTableRow({ row, index }: SignalTableRowProps) {
  const sector = SECTOR_BY_TICKER[row.target_ticker];
  const score = row.signal_score !== null ? parseFloat(row.signal_score) : null;
  const scoreStr =
    score !== null ? (score >= 0 ? `+${score.toFixed(4)}` : score.toFixed(4)) : "—";
  const scoreColor =
    score === null ? "text-gray-400" : score > 0 ? "text-green-700" : "text-red-700";

  return (
    <tr
      className={`hover:bg-blue-50 cursor-pointer transition-colors ${
        index % 2 === 0 ? "bg-white" : "bg-gray-50"
      }`}
    >
      <td className="px-4 py-2 text-center tabular-nums text-gray-500 text-sm w-12">
        {row.signal_rank ?? "—"}
      </td>
      <td className="px-4 py-2">
        <Link
          href={`/sector/${encodeURIComponent(row.target_ticker)}`}
          className="block"
        >
          <span className="font-medium text-gray-800 text-sm">
            {sector?.jpSectorName ?? row.target_ticker}
          </span>
          <span className="ml-2 text-xs text-gray-400">{row.target_ticker}</span>
        </Link>
      </td>
      <td className="px-4 py-2 text-sm text-gray-500">
        {sector?.usTickers.join(", ") ?? "—"}
      </td>
      <td className={`px-4 py-2 tabular-nums text-sm font-mono text-right ${scoreColor}`}>
        {scoreStr}
      </td>
      <td className="px-4 py-2 text-center">
        <SignalBadge side={row.suggested_side} />
      </td>
    </tr>
  );
}
