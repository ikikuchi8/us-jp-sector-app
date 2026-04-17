"""paper_v2-lite の cyclical / defensive セクタータグ定義。

論文 §4.1 の分類に準拠。v2.2-lite では固定。
"""
from __future__ import annotations

from typing import Final

CYCLICAL_US: Final[frozenset[str]] = frozenset({"XLB", "XLE", "XLF", "XLRE"})
DEFENSIVE_US: Final[frozenset[str]] = frozenset({"XLK", "XLP", "XLU", "XLV"})
CYCLICAL_JP: Final[frozenset[str]] = frozenset({"1618.T", "1625.T", "1629.T", "1631.T"})
DEFENSIVE_JP: Final[frozenset[str]] = frozenset({"1617.T", "1621.T", "1627.T", "1630.T"})
