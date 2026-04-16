/**
 * JP 17業種 → US ETF 対応テーブル (frontend 参照用)。
 * backend の sector_mapping.py と内容を一致させること。
 */

export interface SectorInfo {
  jpTicker: string;
  jpSectorName: string;
  usTickers: string[];
}

export const SECTOR_MAPPINGS: SectorInfo[] = [
  { jpTicker: "1617.T", jpSectorName: "食品",           usTickers: ["XLP"] },
  { jpTicker: "1618.T", jpSectorName: "エネルギー資源", usTickers: ["XLE"] },
  { jpTicker: "1619.T", jpSectorName: "建設・資材",     usTickers: ["XLB", "XLI"] },
  { jpTicker: "1620.T", jpSectorName: "素材・化学",     usTickers: ["XLB"] },
  { jpTicker: "1621.T", jpSectorName: "医薬品",         usTickers: ["XLV"] },
  { jpTicker: "1622.T", jpSectorName: "自動車・輸送機", usTickers: ["XLY", "XLI"] },
  { jpTicker: "1623.T", jpSectorName: "鉄鋼・非鉄",    usTickers: ["XLB"] },
  { jpTicker: "1624.T", jpSectorName: "機械",           usTickers: ["XLI"] },
  { jpTicker: "1625.T", jpSectorName: "電機・精密",     usTickers: ["XLK"] },
  { jpTicker: "1626.T", jpSectorName: "情報通信・サービスその他", usTickers: ["XLK", "XLC"] },
  { jpTicker: "1627.T", jpSectorName: "電力・ガス",     usTickers: ["XLU"] },
  { jpTicker: "1628.T", jpSectorName: "運輸・物流",     usTickers: ["XLI"] },
  { jpTicker: "1629.T", jpSectorName: "商社・卸売",     usTickers: ["XLB", "XLY"] },
  { jpTicker: "1630.T", jpSectorName: "小売",           usTickers: ["XLY"] },
  { jpTicker: "1631.T", jpSectorName: "銀行",           usTickers: ["XLF"] },
  { jpTicker: "1632.T", jpSectorName: "金融（除く銀行）", usTickers: ["XLF"] },
  { jpTicker: "1633.T", jpSectorName: "不動産",         usTickers: ["XLRE"] },
];

export const SECTOR_BY_TICKER: Record<string, SectorInfo> = Object.fromEntries(
  SECTOR_MAPPINGS.map((s) => [s.jpTicker, s])
);
