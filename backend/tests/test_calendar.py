"""
CalendarService のユニットテスト。

# テスト方針
  - CalendarService は初期化コストが大きいため、モジュールスコープで 1 回だけ生成する
  - テスト日付はすべて実在する NYSE / JPX の営業日・休場日を使用する
  - 祝日の正確性は pandas_market_calendars に委ねる (ライブラリを信頼する)

# 使用する主要日付の根拠
  昭和の日    : 4/29 固定 → 2025-04-29 (火) = JP 休場, NYSE 開場
  成人の日    : 1月第2月曜 → 2025-01-13 (月) = JP 休場, NYSE 開場
  GW 2025    : 子供の日 5/5 (月) + 振替休日 5/6 (火) = JP 連休, NYSE 開場
  年末年始    : JPX は 12/31〜1/3 休場, NYSE は 1/1 のみ休場 (2024-2025 年末)
  MLK Day    : 1月第3月曜 → 2025-01-20 = NYSE 休場
"""

from datetime import date

import pytest

from app.services.calendar_service import COL_JP_EXECUTION_DATE, COL_US_SIGNAL_DATE, CalendarService


# ---------------------------------------------------------------------------
# フィクスチャ: モジュール全体で 1 インスタンスを共有
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def svc() -> CalendarService:
    """CalendarService をモジュールスコープで初期化する。

    約 30 年分のカレンダーキャッシュ構築 (数秒程度) を 1 回だけ実行する。
    """
    return CalendarService()


# ---------------------------------------------------------------------------
# 1. NYSE 営業日判定
# ---------------------------------------------------------------------------
class TestIsUsBusinessDay:
    def test_normal_open_day(self, svc: CalendarService) -> None:
        """通常の NYSE 営業日"""
        assert svc.is_us_business_day(date(2025, 1, 2)) is True  # 木曜

    def test_saturday(self, svc: CalendarService) -> None:
        """土曜日は NYSE 休場"""
        assert svc.is_us_business_day(date(2025, 1, 4)) is False

    def test_sunday(self, svc: CalendarService) -> None:
        """日曜日は NYSE 休場"""
        assert svc.is_us_business_day(date(2025, 1, 5)) is False

    def test_new_years_day(self, svc: CalendarService) -> None:
        """元日 (1/1) は NYSE 休場"""
        assert svc.is_us_business_day(date(2025, 1, 1)) is False

    def test_mlk_day_2025(self, svc: CalendarService) -> None:
        """MLK Day 2025: 1月第3月曜 = 1/20 は NYSE 休場"""
        assert svc.is_us_business_day(date(2025, 1, 20)) is False

    def test_showa_day_is_nyse_open(self, svc: CalendarService) -> None:
        """昭和の日 (4/29) は日本の祝日だが NYSE は通常開場"""
        assert svc.is_us_business_day(date(2025, 4, 29)) is True

    def test_coming_of_age_day_is_nyse_open(self, svc: CalendarService) -> None:
        """成人の日 (2025-01-13) は日本の祝日だが NYSE は通常開場"""
        assert svc.is_us_business_day(date(2025, 1, 13)) is True

    def test_gw_children_day_is_nyse_open(self, svc: CalendarService) -> None:
        """子供の日 (2025-05-05) は日本の祝日だが NYSE は通常開場"""
        assert svc.is_us_business_day(date(2025, 5, 5)) is True

    def test_gw_substitute_is_nyse_open(self, svc: CalendarService) -> None:
        """GW 振替休日 (2025-05-06) は日本の祝日だが NYSE は通常開場"""
        assert svc.is_us_business_day(date(2025, 5, 6)) is True


# ---------------------------------------------------------------------------
# 2. JPX 営業日判定
# ---------------------------------------------------------------------------
class TestIsJpBusinessDay:
    def test_normal_open_day(self, svc: CalendarService) -> None:
        """通常の JPX 営業日"""
        assert svc.is_jp_business_day(date(2025, 1, 6)) is True  # 月曜

    def test_saturday(self, svc: CalendarService) -> None:
        """土曜日は JPX 休場"""
        assert svc.is_jp_business_day(date(2025, 1, 4)) is False

    def test_new_years_day(self, svc: CalendarService) -> None:
        """元日 (1/1) は JPX 休場"""
        assert svc.is_jp_business_day(date(2025, 1, 1)) is False

    def test_jp_year_start_closed(self, svc: CalendarService) -> None:
        """年始 1/2・1/3 は JPX 休場"""
        assert svc.is_jp_business_day(date(2025, 1, 2)) is False
        assert svc.is_jp_business_day(date(2025, 1, 3)) is False

    def test_year_end_closed(self, svc: CalendarService) -> None:
        """年末 12/31 は JPX 休場"""
        assert svc.is_jp_business_day(date(2024, 12, 31)) is False

    def test_showa_day_is_jpx_closed(self, svc: CalendarService) -> None:
        """昭和の日 (2025-04-29 火) は JPX 休場"""
        assert svc.is_jp_business_day(date(2025, 4, 29)) is False

    def test_coming_of_age_day_2025(self, svc: CalendarService) -> None:
        """成人の日 2025: 1月第2月曜 = 1/13 は JPX 休場"""
        assert svc.is_jp_business_day(date(2025, 1, 13)) is False

    def test_gw_children_day_2025(self, svc: CalendarService) -> None:
        """子供の日 (2025-05-05 月) は JPX 休場"""
        assert svc.is_jp_business_day(date(2025, 5, 5)) is False

    def test_gw_substitute_holiday_2025(self, svc: CalendarService) -> None:
        """GW 振替休日 (2025-05-06 火) は JPX 休場"""
        assert svc.is_jp_business_day(date(2025, 5, 6)) is False

    def test_gw_first_open_day_2025(self, svc: CalendarService) -> None:
        """GW 明け最初の JPX 営業日は 2025-05-07 (水)"""
        assert svc.is_jp_business_day(date(2025, 5, 7)) is True


# ---------------------------------------------------------------------------
# 3. previous_us_business_day
# ---------------------------------------------------------------------------
class TestPreviousUsBusinessDay:
    def test_on_business_day(self, svc: CalendarService) -> None:
        """当日が NYSE 営業日 → 当日を返す"""
        d = date(2025, 1, 2)  # 木曜, NYSE 開場
        assert svc.previous_us_business_day(d) == d

    def test_on_sunday(self, svc: CalendarService) -> None:
        """日曜日 → 前金曜を返す"""
        sunday = date(2025, 1, 5)   # 日
        friday = date(2025, 1, 3)   # 金, NYSE 開場
        assert svc.previous_us_business_day(sunday) == friday

    def test_on_mlk_day_2025(self, svc: CalendarService) -> None:
        """MLK Day (2025-01-20) → 前金曜 (2025-01-17) を返す"""
        assert svc.previous_us_business_day(date(2025, 1, 20)) == date(2025, 1, 17)

    def test_before_cache_raises(self, svc: CalendarService) -> None:
        """キャッシュ範囲より前の日付は ValueError"""
        with pytest.raises(ValueError, match="キャッシュ範囲"):
            svc.previous_us_business_day(date(2009, 12, 31))


# ---------------------------------------------------------------------------
# 4. build_date_alignment: 通常ケース
# ---------------------------------------------------------------------------
class TestBuildDateAlignmentNormal:
    def test_normal_tuesday(self, svc: CalendarService) -> None:
        """通常営業日 (火): JP=2025-07-08 → US=2025-07-07 (月)

        7/8 (火) の前日以前の最新 NYSE 営業日 = 7/7 (月)
        """
        df = svc.build_date_alignment(date(2025, 7, 8), date(2025, 7, 8))
        assert len(df) == 1
        assert df.iloc[0][COL_JP_EXECUTION_DATE] == date(2025, 7, 8)
        assert df.iloc[0][COL_US_SIGNAL_DATE] == date(2025, 7, 7)

    def test_monday_jp_to_friday_us(self, svc: CalendarService) -> None:
        """JP 月曜 → US 前週金曜 (週末スキップ)

        JP=2025-07-14 (月): 前日 7/13 (日) → 前週金曜 7/11 (金)
        """
        df = svc.build_date_alignment(date(2025, 7, 14), date(2025, 7, 14))
        assert len(df) == 1
        assert df.iloc[0][COL_JP_EXECUTION_DATE] == date(2025, 7, 14)
        assert df.iloc[0][COL_US_SIGNAL_DATE] == date(2025, 7, 11)

    def test_empty_when_start_after_end(self, svc: CalendarService) -> None:
        """start > end の場合は空 DataFrame を返す"""
        df = svc.build_date_alignment(date(2025, 7, 14), date(2025, 7, 13))
        assert df.empty
        assert list(df.columns) == [COL_US_SIGNAL_DATE, COL_JP_EXECUTION_DATE]

    def test_jp_holiday_absent_from_alignment(self, svc: CalendarService) -> None:
        """JPX 休場日は jp_execution_date として alignment に出現しない

        2025-01-13 (成人の日) は JPX 休場 → alignment は空
        """
        df = svc.build_date_alignment(date(2025, 1, 13), date(2025, 1, 13))
        assert df.empty


# ---------------------------------------------------------------------------
# 5. build_date_alignment: JP 祝日スキップ
# ---------------------------------------------------------------------------
class TestJpHolidaySkip:
    def test_showa_day_2025(self, svc: CalendarService) -> None:
        """昭和の日 (2025-04-29 火) をスキップ

        JP=2025-04-30 (水): 前日 4/29 は NYSE 開場 → US=2025-04-29
        昭和の日は JPX 休場だが NYSE は通常開場するため、最新の US 情報を使える。
        """
        df = svc.build_date_alignment(date(2025, 4, 30), date(2025, 4, 30))
        assert len(df) == 1
        row = df.iloc[0]
        assert row[COL_JP_EXECUTION_DATE] == date(2025, 4, 30)
        assert row[COL_US_SIGNAL_DATE] == date(2025, 4, 29)

    def test_coming_of_age_day_2025(self, svc: CalendarService) -> None:
        """成人の日 (2025-01-13 月) をスキップ

        JP=2025-01-14 (火): 前日 1/13 は NYSE 開場 → US=2025-01-13
        1月第2月曜 = 成人の日 = JPX 休場, NYSE は通常開場。
        """
        df = svc.build_date_alignment(date(2025, 1, 14), date(2025, 1, 14))
        assert len(df) == 1
        row = df.iloc[0]
        assert row[COL_JP_EXECUTION_DATE] == date(2025, 1, 14)
        assert row[COL_US_SIGNAL_DATE] == date(2025, 1, 13)


# ---------------------------------------------------------------------------
# 6. build_date_alignment: 長期休場 (GW / 年末年始)
# ---------------------------------------------------------------------------
class TestLongHolidayPeriod:
    def test_gw_2025_first_open_day(self, svc: CalendarService) -> None:
        """GW 明け最初の JPX 営業日 (2025-05-07) → US=2025-05-06

        JP 連休: 5/3(土・憲法記念日), 5/4(日・みどりの日),
                 5/5(月・こどもの日), 5/6(火・振替休日)
        GW 中も NYSE は通常開場。前日 5/6 が NYSE 開場のため US=5/6。
        """
        df = svc.build_date_alignment(date(2025, 5, 7), date(2025, 5, 7))
        assert len(df) == 1
        row = df.iloc[0]
        assert row[COL_JP_EXECUTION_DATE] == date(2025, 5, 7)
        assert row[COL_US_SIGNAL_DATE] == date(2025, 5, 6)

    def test_gw_2025_holiday_range_all_absent(self, svc: CalendarService) -> None:
        """JP GW 休場期間 (5/3〜5/6) は alignment に出現しない

        5/3, 5/4: 週末 + 祝日。5/5, 5/6: 祝日・振替休日。
        全日が JPX 休場のため jp_execution_date として出現しない。
        """
        df = svc.build_date_alignment(date(2025, 5, 3), date(2025, 5, 6))
        assert df.empty

    def test_new_year_2024_2025(self, svc: CalendarService) -> None:
        """年末年始 2024-2025: JP 最初の営業日 (2025-01-06) → US=2025-01-03

        JPX 休場: 12/31〜1/3 (年始)
        NYSE 休場: 1/1 のみ
        JP=1/6 (月): 前日 1/5 (日) → 遡ると 1/3 (金, NYSE 開場) → US=1/3
        """
        df = svc.build_date_alignment(date(2025, 1, 6), date(2025, 1, 6))
        assert len(df) == 1
        row = df.iloc[0]
        assert row[COL_JP_EXECUTION_DATE] == date(2025, 1, 6)
        assert row[COL_US_SIGNAL_DATE] == date(2025, 1, 3)

    def test_new_year_holiday_range_all_absent(self, svc: CalendarService) -> None:
        """年末年始休場期間 (12/31〜1/3) は alignment に出現しない"""
        df = svc.build_date_alignment(date(2024, 12, 31), date(2025, 1, 3))
        assert df.empty


# ---------------------------------------------------------------------------
# 7. build_date_alignment: 構造的保証
# ---------------------------------------------------------------------------
class TestAlignmentStructure:
    # テスト用の期間: GW を含む 2025 年上半期 (十分な件数 + 多様なケースを含む)
    _START = date(2025, 1, 1)
    _END = date(2025, 6, 30)

    def test_lookahead_free(self, svc: CalendarService) -> None:
        """先読み防止: 全行で jp_execution_date > us_signal_date が成立する"""
        df = svc.build_date_alignment(self._START, self._END)
        assert not df.empty
        violations = df[df[COL_JP_EXECUTION_DATE] <= df[COL_US_SIGNAL_DATE]]
        assert violations.empty, (
            f"先読みバイアスを検出: {violations}"
        )

    def test_no_duplicate_jp_execution_dates(self, svc: CalendarService) -> None:
        """jp_execution_date に重複なし (1:1 マッピング保証)"""
        df = svc.build_date_alignment(self._START, self._END)
        assert df[COL_JP_EXECUTION_DATE].is_unique

    def test_sorted_by_jp_execution_date(self, svc: CalendarService) -> None:
        """jp_execution_date が昇順"""
        df = svc.build_date_alignment(self._START, self._END)
        assert df[COL_JP_EXECUTION_DATE].is_monotonic_increasing

    def test_row_count_equals_jpx_business_days(self, svc: CalendarService) -> None:
        """行数 = [start, end] 内の JPX 営業日数"""
        df = svc.build_date_alignment(self._START, self._END)
        jp_days = svc.get_jp_business_days(self._START, self._END)
        assert len(df) == len(jp_days)

    def test_all_us_dates_are_nyse_open(self, svc: CalendarService) -> None:
        """alignment の us_signal_date はすべて NYSE 営業日"""
        df = svc.build_date_alignment(self._START, self._END)
        non_nyse = [
            d for d in df[COL_US_SIGNAL_DATE]
            if not svc.is_us_business_day(d)
        ]
        assert non_nyse == [], f"NYSE 営業日でない us_signal_date が存在: {non_nyse}"

    def test_all_jp_dates_are_jpx_open(self, svc: CalendarService) -> None:
        """alignment の jp_execution_date はすべて JPX 営業日"""
        df = svc.build_date_alignment(self._START, self._END)
        non_jpx = [
            d for d in df[COL_JP_EXECUTION_DATE]
            if not svc.is_jp_business_day(d)
        ]
        assert non_jpx == [], f"JPX 営業日でない jp_execution_date が存在: {non_jpx}"

    def test_columns(self, svc: CalendarService) -> None:
        """返り値のカラム名が仕様どおり"""
        df = svc.build_date_alignment(self._START, self._END)
        assert list(df.columns) == [COL_US_SIGNAL_DATE, COL_JP_EXECUTION_DATE]
