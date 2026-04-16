"""
US→JP 業種対応テーブル (simple_v1 専用)。

# 用途
  signal_service.py の simple_v1 シグナルが使用する。
  JP 17業種 (NEXT FUNDS TOPIX-17) の各 ticker に対して、
  スコア計算に使う US ETF (SPDR Select Sector) を定義する。

# スコア計算方式
  signal_score(jp_ticker) = 等ウェイト平均(対応 US ETF の 1 日リターン)
  ウェイトの推定は行わない (v0_01 方針)。

# 主観的要素
  - 対応 US ETF の選択はドメイン知識に基づく等価対応であり、実証的な最適化ではない。
  - 最も主観が入りやすい業種は 1622.T (自動車・輸送機) と 1629.T (商社・卸売)。
    詳細は下記コメントを参照。

# v0_02 以降の拡張ポイント
  等ウェイトを「過去ローリング相関係数」ウェイトに差し替えることで精度向上を見込める。
  その場合、このファイルの構造は変えず signal_service 側で重み付けロジックを追加する。
"""

from typing import Final, TypedDict


class JpSectorMapping(TypedDict):
    """JP 1 業種分の US→JP 対応レコード。"""

    jp_ticker: str             # JP ETF ティッカー (例: "1617.T")
    jp_sector_name: str        # JP 業種名称 (instruments.py の sector_name と一致)
    us_tickers: list[str]      # 対応 US ETF リスト (等ウェイト平均のソース)
    mapping_note: str          # 対応根拠の短い説明


# ---------------------------------------------------------------------------
# JP 17業種 → 対応 US ETF 定義
# 並び順: JP ticker 昇順 (1617.T〜1633.T)
# ---------------------------------------------------------------------------

JP_SECTOR_MAPPINGS: Final[list[JpSectorMapping]] = [
    {
        "jp_ticker": "1617.T",
        "jp_sector_name": "食品",
        "us_tickers": ["XLP"],
        "mapping_note": "食品・飲料は GICS で Consumer Staples に分類。XLP の主要構成銘柄も食品・飲料が中心。",
    },
    {
        "jp_ticker": "1618.T",
        "jp_sector_name": "エネルギー資源",
        "us_tickers": ["XLE"],
        "mapping_note": "エネルギー資源 = Energy。石油・天然ガス・資源開発で直接対応。",
    },
    {
        "jp_ticker": "1619.T",
        "jp_sector_name": "建設・資材",
        "us_tickers": ["XLB", "XLI"],
        "mapping_note": (
            "建材・セメント・ガラス → XLB (Materials)。"
            "建設エンジニアリング・プラント → XLI (Industrials)。"
            "2 ETF の等ウェイト平均で近似。"
        ),
    },
    {
        "jp_ticker": "1620.T",
        "jp_sector_name": "素材・化学",
        "us_tickers": ["XLB"],
        "mapping_note": "素材・化学 = Materials。化学品・基礎素材は XLB が直接対応。",
    },
    {
        "jp_ticker": "1621.T",
        "jp_sector_name": "医薬品",
        "us_tickers": ["XLV"],
        "mapping_note": "医薬品 = Health Care。XLV の主要構成銘柄は製薬会社が中心。",
    },
    {
        "jp_ticker": "1622.T",
        "jp_sector_name": "自動車・輸送機",
        "us_tickers": ["XLY", "XLI"],
        "mapping_note": (
            "自動車メーカー (トヨタ等) は GICS で Consumer Discretionary → XLY。"
            "航空機・重機等の輸送機器は Industrials → XLI。"
            "業種名が「自動車」と「輸送機」を合算しているため 2 ETF で近似。"
            "※ v0_01 で最も主観が入りやすい対応のひとつ。"
        ),
    },
    {
        "jp_ticker": "1623.T",
        "jp_sector_name": "鉄鋼・非鉄",
        "us_tickers": ["XLB"],
        "mapping_note": "鉄鋼・非鉄金属 = Materials。金属・鉱業セクターとして XLB が直接対応。",
    },
    {
        "jp_ticker": "1624.T",
        "jp_sector_name": "機械",
        "us_tickers": ["XLI"],
        "mapping_note": "産業機械 = Industrials。XLI の主要構成銘柄は工作機械・産業機器メーカー。",
    },
    {
        "jp_ticker": "1625.T",
        "jp_sector_name": "電機・精密",
        "us_tickers": ["XLK"],
        "mapping_note": (
            "電機・精密機器は GICS で Technology に近い。"
            "ハードウェア・電子部品は XLK (Technology) が最近似。"
        ),
    },
    {
        "jp_ticker": "1626.T",
        "jp_sector_name": "情報通信・サービスその他",
        "us_tickers": ["XLK", "XLC"],
        "mapping_note": (
            "情報・ITサービス → XLK (Technology)。"
            "通信・メディア・インターネットサービス → XLC (Communication Services)。"
            "「その他サービス」を含む広義の業種定義のため 2 ETF で近似。"
        ),
    },
    {
        "jp_ticker": "1627.T",
        "jp_sector_name": "電力・ガス",
        "us_tickers": ["XLU"],
        "mapping_note": "電力・ガス = Utilities。XLU が直接対応。",
    },
    {
        "jp_ticker": "1628.T",
        "jp_sector_name": "運輸・物流",
        "us_tickers": ["XLI"],
        "mapping_note": "陸運・海運・航空・物流は GICS で Industrials (Transportation) に分類。XLI が対応。",
    },
    {
        "jp_ticker": "1629.T",
        "jp_sector_name": "商社・卸売",
        "us_tickers": ["XLB", "XLY"],
        "mapping_note": (
            "日本の総合商社 (三菱商事・三井物産等) は米国に直接対応する業態がない。"
            "素材・資源・エネルギー取引の側面 → XLB (Materials)。"
            "消費財・機械等の流通・卸売の側面 → XLY (Consumer Discretionary)。"
            "2 ETF の等ウェイト平均で近似するが、主観的要素が最も大きい対応。"
        ),
    },
    {
        "jp_ticker": "1630.T",
        "jp_sector_name": "小売",
        "us_tickers": ["XLY"],
        "mapping_note": "小売 = Consumer Discretionary。XLY の主要構成銘柄は大手小売・EC。",
    },
    {
        "jp_ticker": "1631.T",
        "jp_sector_name": "銀行",
        "us_tickers": ["XLF"],
        "mapping_note": "銀行 = Financials。XLF の主要構成銘柄は大手銀行。",
    },
    {
        "jp_ticker": "1632.T",
        "jp_sector_name": "金融（除く銀行）",
        "us_tickers": ["XLF"],
        "mapping_note": (
            "証券・保険・資産運用は GICS で Financials に分類。"
            "XLF は銀行だけでなく保険・証券も包含するため 1631.T と同じ XLF を使用。"
        ),
    },
    {
        "jp_ticker": "1633.T",
        "jp_sector_name": "不動産",
        "us_tickers": ["XLRE"],
        "mapping_note": "不動産 = Real Estate。XLRE が直接対応。",
    },
]


# ---------------------------------------------------------------------------
# signal_service からの参照用派生定数
# ---------------------------------------------------------------------------

JP_TICKER_TO_US_TICKERS: Final[dict[str, list[str]]] = {
    m["jp_ticker"]: m["us_tickers"] for m in JP_SECTOR_MAPPINGS
}
"""JP ticker → 対応 US ETF リストの辞書。signal_score 計算のメインルックアップ。

Usage:
    from app.seed_data.sector_mapping import JP_TICKER_TO_US_TICKERS

    us_tickers = JP_TICKER_TO_US_TICKERS["1617.T"]  # ["XLP"]
"""

JP_TICKER_TO_SECTOR_NAME: Final[dict[str, str]] = {
    m["jp_ticker"]: m["jp_sector_name"] for m in JP_SECTOR_MAPPINGS
}
"""JP ticker → 業種名称の辞書。ログ・メタデータ用。

Usage:
    from app.seed_data.sector_mapping import JP_TICKER_TO_SECTOR_NAME

    name = JP_TICKER_TO_SECTOR_NAME["1617.T"]  # "食品"
"""

ALL_JP_TICKERS: Final[tuple[str, ...]] = tuple(
    m["jp_ticker"] for m in JP_SECTOR_MAPPINGS
)
"""JP 17業種 ticker のタプル (1617.T〜1633.T 昇順)。

JP_SECTOR_MAPPINGS の並び順と一致。
signal_service でのループ順序の正本として使用する。
"""
