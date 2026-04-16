"""
CalendarService: 日米営業日カレンダー管理。

# 設計方針
  - build_date_alignment() が v0_01 の正本ロジック (JP-first アプローチ)
  - 全行で jp_execution_date > us_signal_date を構造的に保証 → 先読みバイアス防止
  - 内部キャッシュ: frozenset による O(1) 判定 / bisect による O(log n) ナビゲーション

# 先読み防止の担保
  build_date_alignment() のアルゴリズム:
    for each jp_day in JPX_trading_days(start, end):
        us_day = previous_us_business_day(jp_day - 1 calendar day)

  us_day は定義上 jp_day - 1 以前であり、jp_day > us_day が常に成立する。
  さらに build_date_alignment() 内でアサーションによる実行時チェックを行う。

# `get_jp_execution_date()` について
  US-first の単純な「us → jp」変換は実装しない (v0_01 方針)。
  長期 JP 休場時に多対一マッピングが生じ、どの us 情報を使うかが曖昧になるため。
  デバッグ目的の補助として _next_jp_business_day_after() をプライベートで保持する。
"""

import bisect
import logging
from datetime import date, timedelta
from functools import lru_cache
from typing import Final

import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 設定定数
# ---------------------------------------------------------------------------
_CACHE_START: Final[str] = "2010-01-01"
_CACHE_END: Final[str] = "2040-12-31"

_DEFAULT_US_CALENDAR: Final[str] = "NYSE"
_DEFAULT_JP_CALENDAR: Final[str] = "JPX"

# build_date_alignment() の返り値カラム名
COL_US_SIGNAL_DATE: Final[str] = "us_signal_date"
COL_JP_EXECUTION_DATE: Final[str] = "jp_execution_date"


class CalendarService:
    """日米営業日カレンダー管理サービス。

    Attributes:
        _sorted_us_days: NYSE 営業日の昇順リスト (bisect 用)
        _sorted_jp_days: JPX 営業日の昇順リスト (bisect 用)
        _us_trading_days: NYSE 営業日の frozenset (O(1) 判定用)
        _jp_trading_days: JPX 営業日の frozenset (O(1) 判定用)

    使用方法:
        シグナル生成 / バックテスト層では build_date_alignment() をループ軸として使う。
        日付変換は必ずこのサービス経由で行い、呼び出し側でカレンダーロジックを持たない。
    """

    def __init__(
        self,
        us_calendar_name: str = _DEFAULT_US_CALENDAR,
        jp_calendar_name: str = _DEFAULT_JP_CALENDAR,
        cache_start: str = _CACHE_START,
        cache_end: str = _CACHE_END,
    ) -> None:
        """CalendarService を初期化する。

        Args:
            us_calendar_name: pandas_market_calendars の US カレンダー名
            jp_calendar_name: pandas_market_calendars の JP カレンダー名
            cache_start: キャッシュ開始日 (ISO 8601 形式)
            cache_end:   キャッシュ終了日 (ISO 8601 形式)

        Note:
            初期化時に [cache_start, cache_end] の全営業日を先読みしてキャッシュする。
            デフォルトは 2010〜2040 年の約 30 年分。
        """
        logger.info(
            "CalendarService: キャッシュ構築中 (%s〜%s, US=%s, JP=%s)",
            cache_start,
            cache_end,
            us_calendar_name,
            jp_calendar_name,
        )

        us_cal = mcal.get_calendar(us_calendar_name)
        jp_cal = mcal.get_calendar(jp_calendar_name)

        self._sorted_us_days: list[date] = _load_sorted_days(us_cal, cache_start, cache_end)
        self._sorted_jp_days: list[date] = _load_sorted_days(jp_cal, cache_start, cache_end)

        self._us_trading_days: frozenset[date] = frozenset(self._sorted_us_days)
        self._jp_trading_days: frozenset[date] = frozenset(self._sorted_jp_days)

        self._cache_start: date = date.fromisoformat(cache_start)
        self._cache_end: date = date.fromisoformat(cache_end)

        logger.info(
            "CalendarService: 完了 (NYSE %d 日, JPX %d 日)",
            len(self._sorted_us_days),
            len(self._sorted_jp_days),
        )

    # -----------------------------------------------------------------------
    # 公開 API: 営業日判定 (O(1))
    # -----------------------------------------------------------------------

    def is_us_business_day(self, d: date) -> bool:
        """d が NYSE 営業日であれば True を返す。"""
        return d in self._us_trading_days

    def is_jp_business_day(self, d: date) -> bool:
        """d が JPX 営業日であれば True を返す。"""
        return d in self._jp_trading_days

    # -----------------------------------------------------------------------
    # 公開 API: 営業日リスト取得 (O(log n + k))
    # -----------------------------------------------------------------------

    def get_us_business_days(self, start: date, end: date) -> list[date]:
        """[start, end] 内の NYSE 営業日リストを昇順で返す。"""
        lo = bisect.bisect_left(self._sorted_us_days, start)
        hi = bisect.bisect_right(self._sorted_us_days, end)
        return self._sorted_us_days[lo:hi]

    def get_jp_business_days(self, start: date, end: date) -> list[date]:
        """[start, end] 内の JPX 営業日リストを昇順で返す。"""
        lo = bisect.bisect_left(self._sorted_jp_days, start)
        hi = bisect.bisect_right(self._sorted_jp_days, end)
        return self._sorted_jp_days[lo:hi]

    # -----------------------------------------------------------------------
    # 公開 API: 営業日ナビゲーション
    # -----------------------------------------------------------------------

    def previous_us_business_day(self, d: date) -> date:
        """d 以前 (d を含む) で最新の NYSE 営業日を返す。

        Args:
            d: 基準日。d 自身が NYSE 営業日であれば d を返す。

        Raises:
            ValueError: d がキャッシュ範囲より前、または該当日なし。
        """
        if d < self._cache_start:
            raise ValueError(
                f"日付 {d} がキャッシュ範囲 ({self._cache_start}) より前です。"
            )
        idx = bisect.bisect_right(self._sorted_us_days, d) - 1
        if idx < 0:
            raise ValueError(f"{d} 以前に NYSE 営業日が見つかりません。")
        return self._sorted_us_days[idx]

    # -----------------------------------------------------------------------
    # 公開 API: 正本ロジック
    # -----------------------------------------------------------------------

    def build_date_alignment(self, start: date, end: date) -> pd.DataFrame:
        """[正本] JP-first で us_signal_date ↔ jp_execution_date の 1:1 マッピングを生成する。

        # アルゴリズム (JP-first)
          for each jp_execution_date in JPX_business_days(start, end):
              us_signal_date = previous_us_business_day(jp_execution_date - 1 calendar day)

        # 先読み防止保証
          us_signal_date は「jp_execution_date の前日以前の最新 NYSE 営業日」であり、
          構造上 jp_execution_date > us_signal_date が常に成立する。
          実行時アサーションで二重に確認する。

        # 多対一の解消
          JP 連休時に複数の NYSE 日が同一 jp_execution_date へ対応しうるが、
          JP-first アプローチにより jp_execution_date の重複が生じない。
          連休明けの jp_execution_date には「連休中の最終 NYSE 営業日」が us_signal_date
          として割り当てられ、常に最新の US 情報を使う。

        Args:
            start: jp_execution_date の開始日 (inclusive)
            end:   jp_execution_date の終了日 (inclusive)

        Returns:
            columns:
                us_signal_date   (datetime.date) : 米国シグナル生成日 (NYSE 営業日)
                jp_execution_date (datetime.date): 日本執行日 (JPX 営業日)
            ソート順: jp_execution_date 昇順
            重複:     jp_execution_date に重複なし (1:1 マッピング)
            行数:     [start, end] 内の JPX 営業日数
            空:       start > end または範囲内に JPX 営業日なしの場合
        """
        if start > end:
            return pd.DataFrame(columns=[COL_US_SIGNAL_DATE, COL_JP_EXECUTION_DATE])

        jp_days = self.get_jp_business_days(start, end)
        if not jp_days:
            return pd.DataFrame(columns=[COL_US_SIGNAL_DATE, COL_JP_EXECUTION_DATE])

        rows: list[dict[str, date]] = []
        for jp_day in jp_days:
            # 先読み防止: jp_day の前日以前の最新 NYSE 営業日のみ参照する
            # jp_day - 1 を上限にすることで jp_day 自身や未来を絶対に参照しない
            us_day = self.previous_us_business_day(jp_day - timedelta(days=1))
            rows.append(
                {
                    COL_US_SIGNAL_DATE: us_day,
                    COL_JP_EXECUTION_DATE: jp_day,
                }
            )

        df = pd.DataFrame(rows, columns=[COL_US_SIGNAL_DATE, COL_JP_EXECUTION_DATE])

        # --- 実行時の先読み防止アサーション ---
        # jp_days を昇順で処理しているため df はすでに jp_execution_date 昇順
        violated = df[df[COL_JP_EXECUTION_DATE] <= df[COL_US_SIGNAL_DATE]]
        if not violated.empty:
            raise RuntimeError(
                f"先読みバイアス検出: jp_execution_date <= us_signal_date の行があります\n"
                f"{violated}"
            )

        return df

    # -----------------------------------------------------------------------
    # 非公開: 内部補助メソッド
    # -----------------------------------------------------------------------

    def _next_jp_business_day_after(self, d: date) -> date:
        """d より後 (d を含まない) で最初の JPX 営業日を返す。O(log n)。

        [補助用途限定 / デバッグ専用]
        外部からは build_date_alignment() を使うこと。
        US-first で「us_signal_date → jp_execution_date」を求めると
        JP 長期休場時に多対一マッピングが生じ、どの US 情報を使うかが曖昧になる。

        Raises:
            ValueError: d より後に JPX 営業日が見つからない場合。
        """
        idx = bisect.bisect_right(self._sorted_jp_days, d)
        if idx >= len(self._sorted_jp_days):
            raise ValueError(
                f"{d} より後に JPX 営業日が見つかりません (キャッシュ終端: {self._cache_end})。"
            )
        return self._sorted_jp_days[idx]


# ---------------------------------------------------------------------------
# モジュールプライベート関数
# ---------------------------------------------------------------------------

def _load_sorted_days(
    calendar: mcal.MarketCalendar,
    start: str,
    end: str,
) -> list[date]:
    """カレンダーから営業日リストを取得し、ソート済みリストで返す。"""
    schedule = calendar.schedule(start_date=start, end_date=end)
    return sorted(ts.date() for ts in schedule.index)


# ---------------------------------------------------------------------------
# FastAPI Dependency
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_calendar_service() -> CalendarService:
    """CalendarService のキャッシュ済みシングルトンを返す。

    初期化コスト (約 30 年分のカレンダー構築) は初回呼び出し時に 1 度だけ発生する。

    Usage in FastAPI route::

        from typing import Annotated
        from fastapi import Depends

        def some_endpoint(
            cal: Annotated[CalendarService, Depends(get_calendar_service)]
        ):
            alignment = cal.build_date_alignment(start, end)
    """
    return CalendarService()
