"""
instrument_master 初期投入データ定義。

US 11業種: SPDR Select Sector ETF シリーズ
JP 17業種: NEXT FUNDS TOPIX-17 シリーズ (野村アセットマネジメント、コード 1617〜1633)

yfinance での取得時:
  - US は ticker そのまま (例: "XLB")
  - JP は ".T" サフィックス付き (例: "1617.T")
"""

from typing import Final, TypedDict


class InstrumentRow(TypedDict):
    """instrument_master の 1 レコード分のデータ型。"""

    ticker: str          # yfinance 取得用識別子
    market: str          # "US" | "JP"
    instrument_name: str # 銘柄正式名称
    sector_name: str     # 業種名称


# ---------------------------------------------------------------------------
# US: SPDR Select Sector ETF (S&P500 の 11 GICS セクター)
# ---------------------------------------------------------------------------
US_INSTRUMENTS: Final[list[InstrumentRow]] = [
    {
        "ticker": "XLB",
        "market": "US",
        "instrument_name": "Materials Select Sector SPDR Fund",
        "sector_name": "素材",
    },
    {
        "ticker": "XLC",
        "market": "US",
        "instrument_name": "Communication Services Select Sector SPDR Fund",
        "sector_name": "コミュニケーション・サービス",
    },
    {
        "ticker": "XLE",
        "market": "US",
        "instrument_name": "Energy Select Sector SPDR Fund",
        "sector_name": "エネルギー",
    },
    {
        "ticker": "XLF",
        "market": "US",
        "instrument_name": "Financial Select Sector SPDR Fund",
        "sector_name": "金融",
    },
    {
        "ticker": "XLI",
        "market": "US",
        "instrument_name": "Industrial Select Sector SPDR Fund",
        "sector_name": "資本財・サービス",
    },
    {
        "ticker": "XLK",
        "market": "US",
        "instrument_name": "Technology Select Sector SPDR Fund",
        "sector_name": "情報技術",
    },
    {
        "ticker": "XLP",
        "market": "US",
        "instrument_name": "Consumer Staples Select Sector SPDR Fund",
        "sector_name": "生活必需品",
    },
    {
        "ticker": "XLRE",
        "market": "US",
        "instrument_name": "Real Estate Select Sector SPDR Fund",
        "sector_name": "不動産",
    },
    {
        "ticker": "XLU",
        "market": "US",
        "instrument_name": "Utilities Select Sector SPDR Fund",
        "sector_name": "公共事業",
    },
    {
        "ticker": "XLV",
        "market": "US",
        "instrument_name": "Health Care Select Sector SPDR Fund",
        "sector_name": "ヘルスケア",
    },
    {
        "ticker": "XLY",
        "market": "US",
        "instrument_name": "Consumer Discretionary Select Sector SPDR Fund",
        "sector_name": "一般消費財・サービス",
    },
]

# ---------------------------------------------------------------------------
# JP: NEXT FUNDS TOPIX-17 シリーズ (東証上場、野村アセットマネジメント)
#
# コード体系: 1617〜1633 が TOPIX-17 の 17 業種に 1:1 対応
# 採用理由  : TOPIX-17 業種を完全カバーする唯一の日本 ETF シリーズ
#             yfinance では "<コード>.T" で取得可能
# ---------------------------------------------------------------------------
JP_INSTRUMENTS: Final[list[InstrumentRow]] = [
    {
        "ticker": "1617.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 食品（TOPIX-17）ETF",
        "sector_name": "食品",
    },
    {
        "ticker": "1618.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS エネルギー資源（TOPIX-17）ETF",
        "sector_name": "エネルギー資源",
    },
    {
        "ticker": "1619.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 建設・資材（TOPIX-17）ETF",
        "sector_name": "建設・資材",
    },
    {
        "ticker": "1620.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 素材・化学（TOPIX-17）ETF",
        "sector_name": "素材・化学",
    },
    {
        "ticker": "1621.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 医薬品（TOPIX-17）ETF",
        "sector_name": "医薬品",
    },
    {
        "ticker": "1622.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 自動車・輸送機（TOPIX-17）ETF",
        "sector_name": "自動車・輸送機",
    },
    {
        "ticker": "1623.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 鉄鋼・非鉄（TOPIX-17）ETF",
        "sector_name": "鉄鋼・非鉄",
    },
    {
        "ticker": "1624.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 機械（TOPIX-17）ETF",
        "sector_name": "機械",
    },
    {
        "ticker": "1625.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 電機・精密（TOPIX-17）ETF",
        "sector_name": "電機・精密",
    },
    {
        "ticker": "1626.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 情報通信・サービスその他（TOPIX-17）ETF",
        "sector_name": "情報通信・サービスその他",
    },
    {
        "ticker": "1627.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 電力・ガス（TOPIX-17）ETF",
        "sector_name": "電力・ガス",
    },
    {
        "ticker": "1628.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 運輸・物流（TOPIX-17）ETF",
        "sector_name": "運輸・物流",
    },
    {
        "ticker": "1629.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 商社・卸売（TOPIX-17）ETF",
        "sector_name": "商社・卸売",
    },
    {
        "ticker": "1630.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 小売（TOPIX-17）ETF",
        "sector_name": "小売",
    },
    {
        "ticker": "1631.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 銀行（TOPIX-17）ETF",
        "sector_name": "銀行",
    },
    {
        "ticker": "1632.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 金融（除く銀行）（TOPIX-17）ETF",
        "sector_name": "金融（除く銀行）",
    },
    {
        "ticker": "1633.T",
        "market": "JP",
        "instrument_name": "NEXT FUNDS 不動産（TOPIX-17）ETF",
        "sector_name": "不動産",
    },
]

# ---------------------------------------------------------------------------
# 全件まとめ (seed スクリプトから参照する)
# ---------------------------------------------------------------------------
ALL_INSTRUMENTS: Final[list[InstrumentRow]] = US_INSTRUMENTS + JP_INSTRUMENTS
