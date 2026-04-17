"""
tests/paper_v2/test_skip_reasons.py — SkipReason / ALL_SKIP_REASONS の単体テスト。
"""
from __future__ import annotations

import pytest

from app.services.paper_v2.skip_reasons import ALL_SKIP_REASONS, SkipReason


class TestSkipReasons:
    def test_all_reasons_are_strings(self) -> None:
        """ALL_SKIP_REASONS の全要素が str であること。"""
        for reason in ALL_SKIP_REASONS:
            assert isinstance(reason, str), f"{reason!r} は str ではありません"

    def test_all_reasons_unique(self) -> None:
        """ALL_SKIP_REASONS に重複がないこと (frozenset なので定義上保証されるが、
        SkipReason クラスの定数値自体の重複がないことを確認)。"""
        all_values = [
            SkipReason.BEFORE_OOS_START,
            SkipReason.INSUFFICIENT_WINDOW,
            SkipReason.MISSING_US_PRICES,
            SkipReason.MISSING_JP_PRICES,
            SkipReason.ARTIFACT_CHECK_FAILED,
        ]
        assert len(all_values) == len(set(all_values)), "SkipReason クラスに重複した値があります"

    def test_expected_count(self) -> None:
        """ALL_SKIP_REASONS の要素数が 5 であること。"""
        assert len(ALL_SKIP_REASONS) == 5

    def test_all_class_constants_in_frozenset(self) -> None:
        """SkipReason クラスの全定数が ALL_SKIP_REASONS に含まれること。"""
        assert SkipReason.BEFORE_OOS_START in ALL_SKIP_REASONS
        assert SkipReason.INSUFFICIENT_WINDOW in ALL_SKIP_REASONS
        assert SkipReason.MISSING_US_PRICES in ALL_SKIP_REASONS
        assert SkipReason.MISSING_JP_PRICES in ALL_SKIP_REASONS
        assert SkipReason.ARTIFACT_CHECK_FAILED in ALL_SKIP_REASONS

    def test_constants_are_non_empty_strings(self) -> None:
        """各定数が空でない文字列であること。"""
        for attr in (
            "BEFORE_OOS_START",
            "INSUFFICIENT_WINDOW",
            "MISSING_US_PRICES",
            "MISSING_JP_PRICES",
            "ARTIFACT_CHECK_FAILED",
        ):
            val = getattr(SkipReason, attr)
            assert isinstance(val, str) and len(val) > 0, f"{attr} が空文字列です"
