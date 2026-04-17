"""paper_v2-lite の skip 理由定数 (v2.2-lite addendum-1 §5-B)."""
from __future__ import annotations
from typing import Final


class SkipReason:
    BEFORE_OOS_START: Final[str]      = "before_oos_start"
    INSUFFICIENT_WINDOW: Final[str]   = "insufficient_window"
    MISSING_US_PRICES: Final[str]     = "missing_us_prices"
    MISSING_JP_PRICES: Final[str]     = "missing_jp_prices"
    ARTIFACT_CHECK_FAILED: Final[str] = "artifact_check_failed"


ALL_SKIP_REASONS: Final[frozenset[str]] = frozenset({
    SkipReason.BEFORE_OOS_START,
    SkipReason.INSUFFICIENT_WINDOW,
    SkipReason.MISSING_US_PRICES,
    SkipReason.MISSING_JP_PRICES,
    SkipReason.ARTIFACT_CHECK_FAILED,
})
