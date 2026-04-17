"""
audit_paper_v2_coverage.py — Data coverage audit for paper_v2 design.

Read-only script. No DB writes. No modifications to production code.

Usage:
    cd backend && .venv/bin/python scripts/audit_paper_v2_coverage.py
    cd backend && .venv/bin/python scripts/audit_paper_v2_coverage.py --as-of 2025-12-31
    cd backend && .venv/bin/python scripts/audit_paper_v2_coverage.py --output /tmp/audit.md
"""

from __future__ import annotations

import argparse
import bisect
import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure backend/ is on sys.path so `app.*` imports work when called as a script
# ---------------------------------------------------------------------------
_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

# Change working directory to backend/ so .env is found by pydantic_settings
os.chdir(_BACKEND_DIR)

# ---------------------------------------------------------------------------
# Silence noisy loggers BEFORE importing app modules (engine is created at import)
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.WARNING)
for _noisy in (
    "sqlalchemy",
    "sqlalchemy.engine",
    "sqlalchemy.engine.Engine",
    "app.services.calendar_service",
    "app.database",
):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

# Force app_debug=false so create_engine(echo=False) regardless of .env
os.environ["APP_DEBUG"] = "false"

logger = logging.getLogger(__name__)

from sqlalchemy import text

from app.database import SessionLocal
from app.repositories.price_repository import PriceRepository
from app.seed_data.sector_mapping import ALL_JP_TICKERS, JP_TICKER_TO_US_TICKERS
from app.services.calendar_service import (
    COL_JP_EXECUTION_DATE,
    COL_US_SIGNAL_DATE,
    CalendarService,
)

# ---------------------------------------------------------------------------
# Universe constants
# ---------------------------------------------------------------------------
_ALL_US_TICKERS: tuple[str, ...] = tuple(
    sorted(
        frozenset(
            ticker
            for us_list in JP_TICKER_TO_US_TICKERS.values()
            for ticker in us_list
        )
    )
)
_ALL_JP_TICKERS: tuple[str, ...] = tuple(ALL_JP_TICKERS)

# U=27: no XLC
_US_NO_XLC: tuple[str, ...] = tuple(t for t in _ALL_US_TICKERS if t != "XLC")
# U=26: no XLC, no XLRE
_US_NO_XLC_XLRE: tuple[str, ...] = tuple(t for t in _ALL_US_TICKERS if t not in {"XLC", "XLRE"})

WINDOW_SIZE = 60

# C_full candidate windows
C_FULL_WINDOWS = [
    (date(2010, 1, 1), date(2014, 12, 31)),
    (date(2015, 1, 1), date(2019, 6, 30)),
    (date(2019, 7, 1), date(2024, 12, 31)),
]

SCAN_START = date(2005, 1, 1)
BACKTEST_START = date(2015, 1, 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(_BACKEND_DIR.parent), text=True
        ).strip()
    except Exception:
        return "unknown"


def _fetch_ticker_summary(
    session, ticker: str, scan_start: date, scan_end: date
) -> dict:
    """Return coverage summary for one ticker."""
    rows = session.execute(
        text(
            """
            SELECT
                MIN(business_date)  AS first_date,
                MAX(business_date)  AS last_date,
                COUNT(*)            AS row_count,
                COUNT(open_price)   AS has_open,
                COUNT(close_price)  AS has_close,
                COUNT(adjusted_close_price) AS has_adj_close
            FROM price_daily
            WHERE ticker = :ticker
              AND business_date >= :start
              AND business_date <= :end
            """
        ),
        {"ticker": ticker, "start": scan_start, "end": scan_end},
    ).fetchone()
    return {
        "ticker": ticker,
        "first_date": str(rows.first_date) if rows.first_date else None,
        "last_date": str(rows.last_date) if rows.last_date else None,
        "row_count": rows.row_count or 0,
        "has_open": rows.has_open or 0,
        "has_close": rows.has_close or 0,
        "has_adjusted_close": rows.has_adj_close or 0,
    }


def _fetch_all_prices(session, tickers: list[str], scan_start: date, scan_end: date):
    """
    Bulk-fetch all price rows for the given tickers and date range.
    Returns:
        us_adj_map: {ticker: {business_date: float | None}}
        us_sorted_dates: {ticker: sorted list of dates}
        jp_price_map: {ticker: {business_date: (open | None, close | None)}}
    """
    us_tickers = [t for t in tickers if not t.endswith(".T")]
    jp_tickers = [t for t in tickers if t.endswith(".T")]

    us_adj_map: dict[str, dict[date, float | None]] = {}
    us_sorted_dates: dict[str, list[date]] = {}
    jp_price_map: dict[str, dict[date, tuple[float | None, float | None]]] = {}

    if us_tickers:
        rows = session.execute(
            text(
                """
                SELECT ticker, business_date, adjusted_close_price
                FROM price_daily
                WHERE ticker = ANY(:tickers)
                  AND business_date >= :start
                  AND business_date <= :end
                ORDER BY ticker, business_date
                """
            ),
            {"tickers": us_tickers, "start": scan_start, "end": scan_end},
        ).fetchall()
        for row in rows:
            t = row.ticker
            if t not in us_adj_map:
                us_adj_map[t] = {}
            val = float(row.adjusted_close_price) if row.adjusted_close_price is not None else None
            us_adj_map[t][row.business_date] = val
        for t in us_tickers:
            us_sorted_dates[t] = sorted(us_adj_map.get(t, {}).keys())

    if jp_tickers:
        rows = session.execute(
            text(
                """
                SELECT ticker, business_date, open_price, close_price
                FROM price_daily
                WHERE ticker = ANY(:tickers)
                  AND business_date >= :start
                  AND business_date <= :end
                ORDER BY ticker, business_date
                """
            ),
            {"tickers": jp_tickers, "start": scan_start, "end": scan_end},
        ).fetchall()
        for row in rows:
            t = row.ticker
            if t not in jp_price_map:
                jp_price_map[t] = {}
            o = float(row.open_price) if row.open_price is not None else None
            c = float(row.close_price) if row.close_price is not None else None
            jp_price_map[t][row.business_date] = (o, c)

    return us_adj_map, us_sorted_dates, jp_price_map


def _is_valid_row(
    us_date: date,
    jp_date: date,
    us_tickers: tuple[str, ...],
    us_adj_map: dict,
    us_sorted_dates: dict,
    jp_price_map: dict,
) -> bool:
    """
    Complete-case check for one alignment row.
    US: adjusted_close_price non-null on us_date AND previous trading day.
    JP: open_price and close_price non-null on jp_date.
    """
    for us_ticker in us_tickers:
        dates = us_sorted_dates.get(us_ticker, [])
        adj_map = us_adj_map.get(us_ticker, {})

        idx = bisect.bisect_right(dates, us_date) - 1
        if idx < 1 or dates[idx] != us_date:
            return False
        curr_adj = adj_map.get(us_date)
        prev_adj = adj_map.get(dates[idx - 1])
        if curr_adj is None or prev_adj is None:
            return False

    for jp_ticker in _ALL_JP_TICKERS:
        oc = jp_price_map.get(jp_ticker, {}).get(jp_date)
        if oc is None:
            return False
        o, c = oc
        if o is None or c is None:
            return False

    return True


def _compute_valid_flags(
    alignment_df,
    us_tickers: tuple[str, ...],
    us_adj_map: dict,
    us_sorted_dates: dict,
    jp_price_map: dict,
) -> list[bool]:
    """Return list of valid booleans for each alignment row."""
    flags = []
    for _, row in alignment_df.iterrows():
        us_date: date = row[COL_US_SIGNAL_DATE]
        jp_date: date = row[COL_JP_EXECUTION_DATE]
        flags.append(
            _is_valid_row(
                us_date, jp_date, us_tickers, us_adj_map, us_sorted_dates, jp_price_map
            )
        )
    return flags


def _count_cfull_complete_cases(
    alignment_df,
    c_start: date,
    c_end: date,
    us_tickers: tuple[str, ...],
    valid_flags_28: list[bool],
    valid_flags_27: list[bool],
    valid_flags_26: list[bool],
    alignment_all_df,
) -> tuple[int, int, int, int]:
    """
    Return (alignment_days, cc28, cc27, cc26) for C_full window.
    alignment_df is the full scan range; we filter to [c_start, c_end].
    """
    # Build index map: position in alignment_all_df -> valid flag per universe
    # We need to match the rows from alignment_df that fall in [c_start, c_end]
    mask = (alignment_all_df[COL_JP_EXECUTION_DATE] >= c_start) & (
        alignment_all_df[COL_JP_EXECUTION_DATE] <= c_end
    )
    indices = alignment_all_df.index[mask].tolist()
    # Remap to positional indices in the original list
    all_positions = list(range(len(alignment_all_df)))
    idx_map = {original_idx: pos for pos, original_idx in enumerate(alignment_all_df.index)}

    positions = [idx_map[i] for i in indices]
    alignment_days = len(positions)
    cc28 = sum(valid_flags_28[p] for p in positions)
    cc27 = sum(valid_flags_27[p] for p in positions)
    cc26 = sum(valid_flags_26[p] for p in positions)
    return alignment_days, cc28, cc27, cc26


def _rolling_window_valid(
    jp_date: date,
    alignment_all_df,
    valid_flags: list[bool],
    idx_map: dict,
    us_sorted_dates: dict,
    us_adj_map: dict,
) -> bool:
    """
    For a given jp_execution_date, check if the L=60 preceding complete-case rows exist.
    We look at all alignment rows with jp_execution_date < jp_date,
    and count how many are valid; if >= 60 we return True.
    Actually we need the 60 valid rows to be among the most recent preceding rows.
    Per paper_v1 logic: count all valid rows in the preceding buffer. If >= 60, valid.
    """
    pos = idx_map.get(jp_date)
    if pos is None:
        return False
    # All rows before this position
    preceding_valid = sum(valid_flags[:pos])
    return preceding_valid >= WINDOW_SIZE


def _compute_rolling_by_year(
    alignment_all_df,
    valid_flags_28: list[bool],
    valid_flags_27: list[bool],
    valid_flags_26: list[bool],
    backtest_start: date,
    as_of: date,
) -> dict[int, dict]:
    """
    For each jp_execution_date >= backtest_start, check if 60 complete-case rows exist
    in preceding alignment. Aggregate by year.

    Returns dict: year -> {alignment_days, exec28, exec27, exec26}
    """
    # Build positional index map: jp_date -> position
    jp_dates = list(alignment_all_df[COL_JP_EXECUTION_DATE])
    jp_date_to_pos = {d: i for i, d in enumerate(jp_dates)}

    result: dict[int, dict] = {}

    backtest_mask = alignment_all_df[COL_JP_EXECUTION_DATE] >= backtest_start
    backtest_df = alignment_all_df[backtest_mask]

    for _, row in backtest_df.iterrows():
        jp_date: date = row[COL_JP_EXECUTION_DATE]
        if jp_date > as_of:
            continue
        yr = jp_date.year
        if yr not in result:
            result[yr] = {"alignment_days": 0, "exec28": 0, "exec27": 0, "exec26": 0}

        pos = jp_date_to_pos[jp_date]
        result[yr]["alignment_days"] += 1

        # Count valid rows strictly before this position
        v28 = sum(valid_flags_28[:pos])
        v27 = sum(valid_flags_27[:pos])
        v26 = sum(valid_flags_26[:pos])

        if v28 >= WINDOW_SIZE:
            result[yr]["exec28"] += 1
        if v27 >= WINDOW_SIZE:
            result[yr]["exec27"] += 1
        if v26 >= WINDOW_SIZE:
            result[yr]["exec26"] += 1

    return result


def _earliest_viable_start(
    alignment_all_df,
    valid_flags: list[bool],
    backtest_start: date,
    as_of: date,
    min_streak: int = 250,
) -> tuple[date | None, int]:
    """
    Find the earliest jp_execution_date >= backtest_start such that
    the next >= min_streak consecutive jp_execution_dates all have valid 60-day windows.

    Returns (earliest_date, longest_streak_if_none_found).
    """
    jp_dates = list(alignment_all_df[COL_JP_EXECUTION_DATE])
    jp_date_to_pos = {d: i for i, d in enumerate(jp_dates)}

    # For each jp_date >= backtest_start, compute whether it has a valid window
    backtest_jp = [d for d in jp_dates if backtest_start <= d <= as_of]

    window_valid: list[bool] = []
    for jp_date in backtest_jp:
        pos = jp_date_to_pos[jp_date]
        preceding_valid = sum(valid_flags[:pos])
        window_valid.append(preceding_valid >= WINDOW_SIZE)

    # Find earliest position with >= min_streak consecutive True
    n = len(backtest_jp)
    longest_streak = 0
    current_streak = 0
    earliest_date = None

    for i in range(n):
        if window_valid[i]:
            current_streak += 1
            if current_streak > longest_streak:
                longest_streak = current_streak
        else:
            current_streak = 0

        if current_streak >= min_streak and earliest_date is None:
            # streak starts at i - min_streak + 1
            start_idx = i - min_streak + 1
            earliest_date = backtest_jp[start_idx]

    return earliest_date, longest_streak


# ---------------------------------------------------------------------------
# Markdown generation
# ---------------------------------------------------------------------------

def _pct(num: int, denom: int) -> str:
    if denom == 0:
        return "N/A"
    return f"{100.0 * num / denom:.1f}%"


def generate_markdown(
    run_ts: str,
    git_head: str,
    db_info: dict,
    scan_start: date,
    scan_end: date,
    total_rows_queried: int,
    ticker_summaries: list[dict],
    cfull_table: list[dict],
    rolling_by_year: dict[int, dict],
    viable: dict[str, tuple[date | None, int]],
    options_matrix: list[dict],
) -> str:
    lines: list[str] = []
    a = lines.append

    a("# paper_v2 Data Coverage Audit")
    a("")

    # Section 1
    a("## Section 1: Run Metadata")
    a("")
    a(f"- **Run timestamp (UTC):** {run_ts}")
    a(f"- **git HEAD:** `{git_head}`")
    a(f"- **DB:** {db_info['db_label']} (connected: {db_info['connected']})")
    a(f"- **DB row count (price_daily):** {db_info['price_daily_count']:,}")
    if db_info.get("sparse_warning"):
        a(f"- **WARNING:** DB appears sparse (< 10,000 rows). All downstream counts reflect this. Re-run after DB backfill.")
    a(f"- **Scan range:** {scan_start} .. {scan_end}")
    a(f"- **Total rows queried from price_daily:** {total_rows_queried:,}")
    a("")

    # Section 2
    a("## Section 2: Per-Ticker Coverage Table")
    a("")
    a("| ticker | first_date | last_date | row_count | has_open | has_close | has_adjusted_close |")
    a("|--------|-----------|----------|-----------|----------|-----------|-------------------|")
    for s in ticker_summaries:
        a(
            f"| {s['ticker']} | {s['first_date'] or 'NULL'} | {s['last_date'] or 'NULL'} "
            f"| {s['row_count']:,} | {s['has_open']:,} | {s['has_close']:,} | {s['has_adjusted_close']:,} |"
        )
    a("")

    # Section 3
    a("## Section 3: C_full Complete-Case Row Count")
    a("")
    a("| C_full window | Universe | Alignment days | Complete-case rows | % |")
    a("|---------------|----------|---------------|-------------------|---|")
    for row in cfull_table:
        a(
            f"| {row['c_start']} .. {row['c_end']} | {row['universe_label']} "
            f"| {row['alignment_days']:,} | {row['complete_case']:,} | {_pct(row['complete_case'], row['alignment_days'])} |"
        )
    a("")

    # Section 4
    a("## Section 4: Rolling L=60 Valid-Window Count by Year (2015+)")
    a("")
    a("| Year | Alignment days | Executed (U=28) | Executed (U=27) | Executed (U=26) |")
    a("|------|--------------|----------------|----------------|----------------|")
    for yr in sorted(rolling_by_year.keys()):
        d = rolling_by_year[yr]
        a(
            f"| {yr} | {d['alignment_days']:,} | {d['exec28']:,} | {d['exec27']:,} | {d['exec26']:,} |"
        )
    a("")

    # Section 5
    a("## Section 5: Skip Rate Projection")
    a("")
    a("| Year | Alignment days | Skip (U=28) | Skip% (U=28) | Skip (U=27) | Skip% (U=27) | Skip (U=26) | Skip% (U=26) |")
    a("|------|--------------|------------|------------|------------|------------|------------|------------|")
    for yr in sorted(rolling_by_year.keys()):
        d = rolling_by_year[yr]
        al = d["alignment_days"]
        s28 = al - d["exec28"]
        s27 = al - d["exec27"]
        s26 = al - d["exec26"]
        a(
            f"| {yr} | {al:,} | {s28:,} | {_pct(s28, al)} | {s27:,} | {_pct(s27, al)} | {s26:,} | {_pct(s26, al)} |"
        )
    a("")

    # Section 6
    a("## Section 6: Earliest Viable Start Date per Universe")
    a("")
    for label, (earliest, longest) in viable.items():
        if earliest is not None:
            a(f"- **{label}:** earliest viable start = `{earliest}` (first date in a ≥250-consecutive-valid-window streak)")
        else:
            a(f"- **{label}:** no ≥250-consecutive streak found. Longest streak = {longest} days.")
    a("")

    # Section 7
    a("## Section 7: Options & Impact Matrix (DECISION INPUT — no winner)")
    a("")
    a("| C_full period | Universe | C_full complete-case rows | Viable paper_v2 start | Exec days 2015-2025 | Exec days 2019-07+ |")
    a("|--------------|----------|--------------------------|----------------------|--------------------|--------------------|")
    for row in options_matrix:
        a(
            f"| {row['c_full']} | {row['universe']} | {row['cfull_cc']} "
            f"| {row['viable_start'] or 'N/A'} | {row['exec_2015_2025']} | {row['exec_2019h2']} |"
        )
    a("")
    a("This table is decision input. The C_full period, universe, and paper_v2 recommended start date are human decisions made outside this document.")
    a("")

    # Section 8
    a("## Section 8: Raw Data Dump")
    a("")
    a("<details>")
    a("<summary>Per-ticker coverage JSON</summary>")
    a("")
    a("```json")
    a(json.dumps(ticker_summaries, indent=2, default=str))
    a("```")
    a("")
    a("</details>")
    a("")

    # Section 9
    a("## Section 9: Limitations (objective, no recommendations)")
    a("")
    limitations = []
    for s in ticker_summaries:
        if s["first_date"] is not None:
            if s["ticker"] == "XLC" and s["first_date"] > "2015-01-01":
                limitations.append(f"XLC first_date is {s['first_date']} — starts after backtest candidate start (2015-01-01).")
            if s["ticker"] == "XLRE" and s["first_date"] > "2010-01-01":
                limitations.append(f"XLRE first_date is {s['first_date']} — not available for full C_full 2010-2014 window.")
        else:
            limitations.append(f"{s['ticker']}: no rows found in price_daily for scan range {scan_start} .. {scan_end}.")

    if db_info.get("sparse_warning"):
        limitations.append(
            "DB is sparse (< 10,000 rows total). All complete-case and window counts are 0 or near-0 and will change after backfill."
        )

    # C_full 2010-2014 + U=28 specific note
    for row in cfull_table:
        if row["c_start"] == str(date(2010, 1, 1)) and row["universe_label"].startswith("28") and row["complete_case"] == 0:
            limitations.append(
                "C_full 2010-01-01 .. 2014-12-31 U=28 has 0 complete-case rows because XLC and/or XLRE are absent in that period."
            )
            break

    if not limitations:
        limitations.append("No data issues observed beyond expected late-start dates for XLC and XLRE.")

    for lim in limitations:
        a(f"- {lim}")
    a("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# lite-2021 mode: analysis functions
# ---------------------------------------------------------------------------

# C_full candidate windows for lite-2021 mode
LITE_CFULL_WINDOWS = [
    (date(2021, 1, 12), date(2021, 12, 31)),
    (date(2021, 1, 12), date(2022, 12, 31)),
    (date(2022, 1, 1),  date(2022, 12, 31)),
    (date(2022, 1, 1),  date(2023, 12, 31)),
    (date(2023, 1, 1),  date(2023, 12, 31)),
    (date(2021, 1, 12), date(2023, 12, 31)),
]

LITE_SCAN_START = date(2021, 1, 1)
LITE_WINDOW_SIZES = [60, 120, 250]


def _compute_monthly_coverage(
    alignment_df,
    valid_flags_28: list[bool],
    us_adj_map: dict,
    jp_price_map: dict,
    scan_start: date,
    as_of: date,
) -> list[dict]:
    """
    Compute per-month coverage: alignment days, US-11-complete, JP-17-complete, full complete-case.
    """
    jp_dates = list(alignment_df[COL_JP_EXECUTION_DATE])
    us_dates = list(alignment_df[COL_US_SIGNAL_DATE])

    monthly: dict[str, dict] = {}

    for i, (jp_date, us_date) in enumerate(zip(jp_dates, us_dates)):
        if jp_date < scan_start or jp_date > as_of:
            continue
        ym = jp_date.strftime("%Y-%m")
        if ym not in monthly:
            monthly[ym] = {
                "ym": ym,
                "alignment_days": 0,
                "us11_complete": 0,
                "jp17_complete": 0,
                "complete_case_28": 0,
            }
        monthly[ym]["alignment_days"] += 1

        # US-11 complete: all 11 US tickers have adjusted_close on us_date (and prev day)
        us_ok = True
        for us_ticker in _ALL_US_TICKERS:
            dates_sorted = sorted(us_adj_map.get(us_ticker, {}).keys())
            adj_map = us_adj_map.get(us_ticker, {})
            idx = bisect.bisect_right(dates_sorted, us_date) - 1
            if idx < 1 or dates_sorted[idx] != us_date:
                us_ok = False
                break
            if adj_map.get(us_date) is None or adj_map.get(dates_sorted[idx - 1]) is None:
                us_ok = False
                break
        if us_ok:
            monthly[ym]["us11_complete"] += 1

        # JP-17 complete: all 17 JP tickers have open and close on jp_date
        jp_ok = True
        for jp_ticker in _ALL_JP_TICKERS:
            oc = jp_price_map.get(jp_ticker, {}).get(jp_date)
            if oc is None or oc[0] is None or oc[1] is None:
                jp_ok = False
                break
        if jp_ok:
            monthly[ym]["jp17_complete"] += 1

        if valid_flags_28[i]:
            monthly[ym]["complete_case_28"] += 1

    return [v for k, v in sorted(monthly.items())]


def _compute_data_absence_vs_gap(
    alignment_df,
    us_adj_map: dict,
    jp_price_map: dict,
    scan_start: date,
    as_of: date,
) -> dict:
    """
    Classify each alignment date as:
    - full_absent: both US and JP have 0 records (DB data not present)
    - partial_missing: some tickers missing (data gap)
    - complete: all tickers present (complete-case)
    """
    full_absent = 0
    partial_missing = 0
    complete_count = 0

    jp_dates = list(alignment_df[COL_JP_EXECUTION_DATE])
    us_dates = list(alignment_df[COL_US_SIGNAL_DATE])

    for jp_date, us_date in zip(jp_dates, us_dates):
        if jp_date < scan_start or jp_date > as_of:
            continue

        # Count US tickers with data on us_date
        us_count = sum(
            1 for t in _ALL_US_TICKERS
            if us_adj_map.get(t, {}).get(us_date) is not None
        )
        # Count JP tickers with data on jp_date
        jp_count = sum(
            1 for t in _ALL_JP_TICKERS
            if jp_price_map.get(t, {}).get(jp_date) is not None
        )

        total_us = len(_ALL_US_TICKERS)
        total_jp = len(_ALL_JP_TICKERS)

        if us_count == 0 and jp_count == 0:
            full_absent += 1
        elif us_count == total_us and jp_count == total_jp:
            complete_count += 1
        else:
            partial_missing += 1

    return {
        "full_absent": full_absent,
        "partial_missing": partial_missing,
        "complete": complete_count,
    }


def _compute_consecutive_window_starts(
    alignment_df,
    valid_flags: list[bool],
    jp_dates_all: list[date],
    jp_date_to_pos: dict[date, int],
    scan_start: date,
    as_of: date,
    window_size: int,
) -> date | None:
    """
    Find the first jp_execution_date >= scan_start such that there are >= window_size
    valid complete-case rows in all preceding alignment rows.

    This finds the first day where the rolling window of `window_size` days is satisfied.
    """
    for jp_date in jp_dates_all:
        if jp_date < scan_start or jp_date > as_of:
            continue
        pos = jp_date_to_pos[jp_date]
        preceding_valid = sum(valid_flags[:pos])
        if preceding_valid >= window_size:
            return jp_date
    return None


def _compute_oos_start_after_cfull(
    cfull_end: date,
    alignment_df,
    valid_flags: list[bool],
    jp_dates_all: list[date],
    jp_date_to_pos: dict[date, int],
    window_size: int = 60,
) -> date | None:
    """
    Find the first jp_execution_date strictly after cfull_end where the 60-day window is valid.
    """
    for jp_date in jp_dates_all:
        if jp_date <= cfull_end:
            continue
        pos = jp_date_to_pos[jp_date]
        preceding_valid = sum(valid_flags[:pos])
        if preceding_valid >= window_size:
            return jp_date
    return None


def _count_oos_days(
    oos_start: date | None,
    alignment_df,
    valid_flags: list[bool],
    jp_date_to_pos: dict[date, int],
    jp_dates_all: list[date],
    as_of: date,
) -> tuple[int, int]:
    """
    Return (executed_days, skip_days) for OOS period [oos_start, as_of].
    executed_days = alignment days where window was valid (had >= 60 preceding complete rows).
    skip_days = alignment days where window was NOT valid.
    """
    if oos_start is None:
        return 0, 0
    executed = 0
    skipped = 0
    for jp_date in jp_dates_all:
        if jp_date < oos_start or jp_date > as_of:
            continue
        pos = jp_date_to_pos[jp_date]
        preceding_valid = sum(valid_flags[:pos])
        if preceding_valid >= 60:
            executed += 1
        else:
            skipped += 1
    return executed, skipped


def generate_lite_markdown(
    run_ts: str,
    git_head: str,
    db_info: dict,
    as_of: date,
    ticker_summaries: list[dict],
    monthly_coverage: list[dict],
    absence_stats: dict,
    window_starts: dict,  # {window_size: {universe_label: date|None}}
    cfull_table: list[dict],
    period_cases: list[dict],
) -> str:
    """Generate the lite-2021 period design markdown report."""
    lines: list[str] = []
    a = lines.append

    a("# paper_v2-lite 期間設計監査レポート（2021+ DB 前提）")
    a("")
    a(f"生成日時 (UTC): {run_ts}")
    a(f"git HEAD: `{git_head}`")
    a(f"DB: {db_info['db_label']} / price_daily 総行数: {db_info['price_daily_count']:,}")
    a(f"監査基準日 (as_of): {as_of}")
    a("")

    # Section 1
    a("## 1. 監査の目的")
    a("")
    a("現 DB (2021-01-11/12 以降のデータのみ) を前提として、paper_v2-lite を成立させるための")
    a("期間構造（C_full 期間 / OOS 開始日）の候補を数値で整理し、PdM の判断材料とする。")
    a("推奨案は提示しない。候補の長所・短所・想定リスクのみ記載する。")
    a("")

    # Section 2
    a("## 2. 監査対象")
    a("")
    a("- テーブル: `price_daily`")
    a("- ファイル: `backend/app/seed_data/sector_mapping.py` (universe 正本: US 11 + JP 17 = 28)")
    a("- 使用ツール: `CalendarService.build_date_alignment`, `PriceRepository` (read-only)")
    a(f"- DB 接続: {db_info['db_label']}")
    a("")

    # Section 3
    a("## 3. 監査方法の要約")
    a("")
    a("1. DB の `price_daily` から 2021-01-01 以降の全 28 ticker の価格データを一括取得する。")
    a("2. `CalendarService.build_date_alignment` で JP/US 営業日の alignment を構築する。")
    a("3. 各 alignment 行について complete-case フラグを算出する（US: adjusted_close 連日非 null / JP: open+close 非 null）。")
    a("4. monthly / 連続窓 / C_full 候補ごとに complete-case 行数を集計する。")
    a("5. C_full 候補ごとに OOS 開始可能日を探索し、OOS 期間の有効日数を集計する。")
    a("")

    # Section 4
    a("## 4. 2021 年以降 coverage サマリー")
    a("")

    # 4.1 Ticker coverage
    a("### 4.1 ticker 別 coverage（28 ticker）")
    a("")
    a("| ticker | first_date | last_date | row_count | non-null adjusted_close | non-null open | non-null close |")
    a("|--------|-----------|----------|-----------|------------------------|--------------|----------------|")
    for s in ticker_summaries:
        a(
            f"| {s['ticker']} | {s['first_date'] or 'NULL'} | {s['last_date'] or 'NULL'} "
            f"| {s['row_count']:,} | {s['has_adjusted_close']:,} | {s['has_open']:,} | {s['has_close']:,} |"
        )
    a("")

    # 4.2 Monthly coverage
    a("### 4.2 月次 complete-case カバレッジ")
    a("")
    a("| 年月 | alignment 日数 | US 11 揃い日数 | JP 17 揃い日数 | complete-case 日数 (U=28) |")
    a("|------|--------------|--------------|--------------|--------------------------|")
    for row in monthly_coverage:
        a(
            f"| {row['ym']} | {row['alignment_days']} | {row['us11_complete']} "
            f"| {row['jp17_complete']} | {row['complete_case_28']} |"
        )
    a("")

    # 4.3 Data absence vs gap
    a("### 4.3 「データ欠損 vs ticker 不在」の区別（2021-01-01 以降）")
    a("")
    total_classified = absence_stats["full_absent"] + absence_stats["partial_missing"] + absence_stats["complete"]
    a(f"- alignment 総日数（2021-01-01 以降）: {total_classified:,}")
    a(f"- 全欠損日数（US も JP も 0 件 = DB データ不在）: {absence_stats['full_absent']:,}")
    a(f"- 部分欠損日数（一部 ticker のみ欠損 = データ欠損）: {absence_stats['partial_missing']:,}")
    a(f"- 完全揃い日数 (complete-case U=28): {absence_stats['complete']:,}")
    a("")

    # Section 5
    a("## 5. 連続窓の成立状況")
    a("")
    a("各成立開始日 = その日時点で N 日分の complete-case 行が preceding rows に存在する最初の jp_execution_date")
    a("")
    a("| 窓長 | U=28 成立開始日 | U=27 (no XLC) 成立開始日 | U=26 (no XLC, no XLRE) 成立開始日 |")
    a("|------|----------------|------------------------|----------------------------------|")
    for ws in LITE_WINDOW_SIZES:
        d28 = window_starts[ws].get("28", None)
        d27 = window_starts[ws].get("27", None)
        d26 = window_starts[ws].get("26", None)
        a(
            f"| {ws} 日 | {d28 or 'N/A'} | {d27 or 'N/A'} | {d26 or 'N/A'} |"
        )
    a("")

    # Section 6
    a("## 6. C_full 候補期間ごとの成立状況")
    a("")
    a("| C_full 期間 | U=28 complete rows | U=27 complete rows | U=26 complete rows | OOS 開始可能日 (U=28, L=60) |")
    a("|------------|-------------------|-------------------|-------------------|--------------------------|")
    for row in cfull_table:
        a(
            f"| {row['c_start']} .. {row['c_end']} | {row['cc28']:,} | {row['cc27']:,} | {row['cc26']:,} "
            f"| {row['oos_start_28'] or 'N/A'} |"
        )
    a("")

    # Section 7
    a("## 7. Period design 案（U=28 固定）")
    a("")
    for case in period_cases:
        a(f"---")
        a("")
        a(f"#### {case['label']}")
        a("")
        a(f"- C_full 期間: {case['cfull_start']} 〜 {case['cfull_end']}")
        a(f"- C_full complete-case 行数 (U=28): {case['cfull_cc']:,}")
        a(f"- OOS 開始日: {case['oos_start'] or 'N/A'}")
        a(f"- OOS 終了日: {as_of} (最新)")
        a(f"- OOS 期間 executed 日数: {case['oos_executed']}")
        a(f"- OOS 期間 skip 日数: {case['oos_skipped']}")
        a("")
        a(f"**長所**:")
        for pro in case["pros"]:
            a(f"- {pro}")
        a("")
        a(f"**短所**:")
        for con in case["cons"]:
            a(f"- {con}")
        a("")
        a(f"**想定リスク**:")
        for risk in case["risks"]:
            a(f"- {risk}")
        a("")

    # Section 8
    a("---")
    a("")
    a("## 8. 候補比較テーブル（decision input）")
    a("")
    a("| 案 | C_full 期間 | C_full 行数 | OOS 開始 | OOS 日数 | 長所 1 行 | 短所 1 行 |")
    a("|---|------------|-----------|---------|---------|---------|---------|")
    for case in period_cases:
        a(
            f"| {case['label']} | {case['cfull_start']} .. {case['cfull_end']} "
            f"| {case['cfull_cc']:,} | {case['oos_start'] or 'N/A'} "
            f"| {case['oos_executed']} | {case['pros'][0]} | {case['cons'][0]} |"
        )
    a("")

    # Section 9
    a("## 9. PdM 判断ポイント（推奨ではない、確認項目）")
    a("")
    a("- C_full 期間の長さとサンプル代表性のトレードオフ: 長いほど多様な相場環境を含むが、OOS 期間が短くなる。短い C_full は特定相場（例: コロナ禍後上昇局面）に偏るリスクがある。")
    a("- OOS 期間の長さと統計的有意性のトレードオフ: OOS が短すぎると（例: 1 年未満）シグナル有効性の評価が不安定になる。実務上最低でも 200 営業日程度の OOS が望ましいとされる場合がある。")
    a("- XLC / XLRE の実 first_date 未解決点: DB の first_date が 2021-01-11 であることが「その日以前の実在データが DB に無い」ことを示すのか、「その ETF 自体が 2021-01-11 以前に存在しなかった」ことを示すのかは未確認。XLC の実際の上場日は 2018-06-18 / XLRE は 2015-10-07 であるため、DB にバックフィルされていない可能性が高い。")
    a("- バックフィル（2018 以降）の再検討要否: XLC が 2018-06-18 以降存在するとすれば、2018-06-18 〜 2020-12-31 のバックフィルにより C_full 期間の選択肢が広がる。バックフィルするかどうかは PdM の判断。")
    a("")

    # Section 10
    a("## 10. 限界事項")
    a("")
    a("- 現 DB の price_daily は 2021-01-11 (US) / 2021-01-12 (JP) 以前のデータが 0 行。バックフィルが行われていない場合、このレポートの C_full 候補はすべて 2021+ に限定されたまま変わらない。")
    a("- 全 ticker の first_date が 2021-01-11/12 に揃っていることは、同日に一括インポートされた可能性を示唆する。各 ticker の実際の取引開始日ではない可能性が高い。")
    a("- OOS 開始可能日の算出は「preceding alignment rows に >= 60 complete-case 行がある最初の日」として定義しているが、paper_v2 本体の実装で窓定義が変わる場合は再計算が必要。")
    a("- U=27 / U=26 の数値は参考値として掲載しているが、本設計案の主軸は U=28 固定。")
    a("")

    return "\n".join(lines)


def run_lite_2021(session, as_of: date, output_path: Path, db_info: dict, run_ts: str, git_head: str) -> None:
    """Execute the lite-2021 analysis mode and write the markdown report."""

    print("[lite-2021] Fetching ticker summaries (2021+)...")
    all_tickers = list(_ALL_US_TICKERS) + list(_ALL_JP_TICKERS)
    ticker_summaries: list[dict] = []
    for ticker in all_tickers:
        s = _fetch_ticker_summary(session, ticker, LITE_SCAN_START, as_of)
        ticker_summaries.append(s)
        print(f"  {ticker}: {s['row_count']:,} rows  {s['first_date']} .. {s['last_date']}")

    print("[lite-2021] Building CalendarService...")
    calendar = CalendarService(cache_start="2020-01-01")
    alignment_all = calendar.build_date_alignment(LITE_SCAN_START, as_of)
    print(f"[lite-2021] Alignment rows ({LITE_SCAN_START} .. {as_of}): {len(alignment_all):,}")

    print("[lite-2021] Bulk-fetching all prices (2020-12-01 .. as_of) for complete-case...")
    from datetime import timedelta
    fetch_start = LITE_SCAN_START - timedelta(days=10)
    us_adj_map_28, us_sorted_28, jp_price_map = _fetch_all_prices(
        session, all_tickers, fetch_start, as_of
    )

    print("[lite-2021] Computing valid flags U=28/27/26...")
    valid_28 = _compute_valid_flags(
        alignment_all, _ALL_US_TICKERS, us_adj_map_28, us_sorted_28, jp_price_map
    )
    us_adj_27 = {t: v for t, v in us_adj_map_28.items() if t != "XLC"}
    us_sorted_27 = {t: v for t, v in us_sorted_28.items() if t != "XLC"}
    valid_27 = _compute_valid_flags(
        alignment_all, _US_NO_XLC, us_adj_27, us_sorted_27, jp_price_map
    )
    us_adj_26 = {t: v for t, v in us_adj_map_28.items() if t not in {"XLC", "XLRE"}}
    us_sorted_26 = {t: v for t, v in us_sorted_28.items() if t not in {"XLC", "XLRE"}}
    valid_26 = _compute_valid_flags(
        alignment_all, _US_NO_XLC_XLRE, us_adj_26, us_sorted_26, jp_price_map
    )

    jp_dates_all = list(alignment_all[COL_JP_EXECUTION_DATE])
    jp_date_to_pos = {d: i for i, d in enumerate(jp_dates_all)}

    print("[lite-2021] Computing monthly coverage...")
    monthly_coverage = _compute_monthly_coverage(
        alignment_all, valid_28, us_adj_map_28, jp_price_map, LITE_SCAN_START, as_of
    )

    print("[lite-2021] Computing data absence vs gap...")
    absence_stats = _compute_data_absence_vs_gap(
        alignment_all, us_adj_map_28, jp_price_map, LITE_SCAN_START, as_of
    )

    print("[lite-2021] Computing consecutive window starts...")
    window_starts: dict[int, dict[str, date | None]] = {}
    for ws in LITE_WINDOW_SIZES:
        window_starts[ws] = {
            "28": _compute_consecutive_window_starts(
                alignment_all, valid_28, jp_dates_all, jp_date_to_pos, LITE_SCAN_START, as_of, ws
            ),
            "27": _compute_consecutive_window_starts(
                alignment_all, valid_27, jp_dates_all, jp_date_to_pos, LITE_SCAN_START, as_of, ws
            ),
            "26": _compute_consecutive_window_starts(
                alignment_all, valid_26, jp_dates_all, jp_date_to_pos, LITE_SCAN_START, as_of, ws
            ),
        }

    print("[lite-2021] Computing C_full candidate table...")
    cfull_table: list[dict] = []
    for (c_start, c_end) in LITE_CFULL_WINDOWS:
        positions_28 = [
            i for i, d in enumerate(jp_dates_all)
            if c_start <= d <= c_end
        ]
        cc28 = sum(valid_28[p] for p in positions_28)
        cc27 = sum(valid_27[p] for p in positions_28)
        cc26 = sum(valid_26[p] for p in positions_28)

        oos_start_28 = _compute_oos_start_after_cfull(
            c_end, alignment_all, valid_28, jp_dates_all, jp_date_to_pos, window_size=60
        )

        cfull_table.append({
            "c_start": str(c_start),
            "c_end": str(c_end),
            "cc28": cc28,
            "cc27": cc27,
            "cc26": cc26,
            "oos_start_28": str(oos_start_28) if oos_start_28 else None,
        })

    print("[lite-2021] Building period design cases...")
    # Case A: Maximize C_full (2021-01-12 .. 2023-12-31), OOS 2024+
    # Case B: Balanced (2022-01-01 .. 2023-12-31), OOS 2024+
    # Case C: Maximize OOS (2021-01-12 .. 2021-12-31), OOS 2022+

    def _find_cfull_row(c_start_str: str, c_end_str: str) -> dict:
        for row in cfull_table:
            if row["c_start"] == c_start_str and row["c_end"] == c_end_str:
                return row
        return {"cc28": 0, "cc27": 0, "cc26": 0, "oos_start_28": None}

    period_cases = []

    # Case A
    row_a = _find_cfull_row("2021-01-12", "2023-12-31")
    oos_start_a = date.fromisoformat(row_a["oos_start_28"]) if row_a["oos_start_28"] else None
    exec_a, skip_a = _count_oos_days(oos_start_a, alignment_all, valid_28, jp_date_to_pos, jp_dates_all, as_of)
    period_cases.append({
        "label": "案 A（C_full 最大化: 3 年）",
        "cfull_start": "2021-01-12",
        "cfull_end": "2023-12-31",
        "cfull_cc": row_a["cc28"],
        "oos_start": row_a["oos_start_28"],
        "oos_executed": exec_a,
        "oos_skipped": skip_a,
        "pros": [
            "C_full 3 年でリーマン後回復・コロナ後相場など多様な局面を含む",
            f"complete-case 行数 {row_a['cc28']:,} 行で PCA+Ridge 訓練の安定性が高い",
        ],
        "cons": [
            f"OOS 開始が {row_a['oos_start_28'] or 'N/A'} からのため OOS 期間が短い（{exec_a} 日）",
            "2021-2023 はコロナ後特殊相場が多く代表性に偏りがある可能性",
        ],
        "risks": [
            "OOS が約 1〜1.5 年に限定されるため、シグナルの out-of-sample 有効性評価が統計的に不安定になる可能性",
            "C_full 期間内の市場構造変化（利上げサイクル開始 2022 等）が PCA 主成分の不安定性をもたらす可能性",
        ],
    })

    # Case B: Balanced C_full 2022-01-01 .. 2023-12-31
    row_b = _find_cfull_row("2022-01-01", "2023-12-31")
    oos_start_b = date.fromisoformat(row_b["oos_start_28"]) if row_b["oos_start_28"] else None
    exec_b, skip_b = _count_oos_days(oos_start_b, alignment_all, valid_28, jp_date_to_pos, jp_dates_all, as_of)
    period_cases.append({
        "label": "案 B（バランス: C_full 2 年、OOS 2024 以降）",
        "cfull_start": "2022-01-01",
        "cfull_end": "2023-12-31",
        "cfull_cc": row_b["cc28"],
        "oos_start": row_b["oos_start_28"],
        "oos_executed": exec_b,
        "oos_skipped": skip_b,
        "pros": [
            f"C_full 2 年（{row_b['cc28']:,} 行）で OOS 期間 {exec_b} 日を確保するバランス型",
            "2022-2023 は利上げ→一服局面で US-JP 相関が比較的安定",
        ],
        "cons": [
            "C_full が 2 年に短縮されるため、C_full 内のレジーム多様性は案 A より低い",
            "2021 の complete-case データを C_full に使わないため情報を捨てる",
        ],
        "risks": [
            "2022 年の急激な利上げ相場を C_full に含むため、Ridge 係数が利上げ局面に過剰適合するリスク",
            "OOS 開始が 2024+ になるため、2024-2025 の tariff ショック局面への一般化が未検証",
        ],
    })

    # Case C: Maximize OOS — C_full 2021-01-12 .. 2021-12-31
    row_c = _find_cfull_row("2021-01-12", "2021-12-31")
    oos_start_c = date.fromisoformat(row_c["oos_start_28"]) if row_c["oos_start_28"] else None
    exec_c, skip_c = _count_oos_days(oos_start_c, alignment_all, valid_28, jp_date_to_pos, jp_dates_all, as_of)
    period_cases.append({
        "label": "案 C（OOS 最大化: C_full 1 年）",
        "cfull_start": "2021-01-12",
        "cfull_end": "2021-12-31",
        "cfull_cc": row_c["cc28"],
        "oos_start": row_c["oos_start_28"],
        "oos_executed": exec_c,
        "oos_skipped": skip_c,
        "pros": [
            f"OOS 期間を {exec_c} 日と最大化でき、OOS 評価の統計的安定性が最も高い",
            "C_full を短く抑えることで訓練サンプルの時代偏りを最小化",
        ],
        "cons": [
            f"C_full complete-case {row_c['cc28']:,} 行は最小（L=60 窓の稼働開始以降の 1 年分のみ）",
            "2021 はコロナ後の回復局面のみで相場多様性が最低",
        ],
        "risks": [
            "C_full サンプルが少ないため PCA 主成分の推定誤差が大きく、モデルが不安定になるリスクが最も高い",
            "1 年分の C_full では Ridge 係数の汎化性能の事前検証が困難",
        ],
    })

    print("[lite-2021] Generating markdown...")
    md = generate_lite_markdown(
        run_ts=run_ts,
        git_head=git_head,
        db_info=db_info,
        as_of=as_of,
        ticker_summaries=ticker_summaries,
        monthly_coverage=monthly_coverage,
        absence_stats=absence_stats,
        window_starts=window_starts,
        cfull_table=cfull_table,
        period_cases=period_cases,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(md, encoding="utf-8")
    print(f"[lite-2021] Written: {output_path}")


# ---------------------------------------------------------------------------
# Main (original mode preserved; lite-2021 mode added)
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="paper_v2 data coverage audit (read-only)")
    parser.add_argument(
        "--output",
        default=None,
        help="Output path for markdown doc. Default: docs/paper_v2/coverage_audit_<YYYYMMDD>.md",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="Upper bound date for scan (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--mode",
        default="default",
        choices=["default", "lite-2021"],
        help="Audit mode. 'lite-2021' runs the 2021+ period design analysis.",
    )
    args = parser.parse_args()

    today = date.today()
    as_of = date.fromisoformat(args.as_of) if args.as_of else today

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    git_head = _git_head()

    # ----------------------------------------------------------------
    # DB connectivity check (shared)
    # ----------------------------------------------------------------
    session = SessionLocal()
    db_info: dict = {}
    try:
        row = session.execute(text("SELECT COUNT(*) FROM price_daily")).scalar()
        price_daily_count = row or 0
        db_info = {
            "connected": True,
            "db_label": "connected (host/db masked)",
            "price_daily_count": price_daily_count,
            "sparse_warning": price_daily_count < 10_000,
        }
        print(f"[audit] price_daily rows: {price_daily_count:,}")
    except Exception as e:
        db_info = {
            "connected": False,
            "db_label": f"connection error: {type(e).__name__}",
            "price_daily_count": 0,
            "sparse_warning": True,
        }
        print(f"[audit] DB error: {e}")

    if args.mode == "lite-2021":
        if args.output:
            output_path = Path(args.output)
        else:
            worktree_root = _BACKEND_DIR.parent
            output_path = worktree_root / "docs" / "paper_v2" / f"period_design_lite_{today.strftime('%Y%m%d')}.md"

        print(f"[audit] mode=lite-2021  run_ts={run_ts}  git={git_head[:12]}  as_of={as_of}")
        print(f"[audit] output -> {output_path}")

        if not db_info["connected"]:
            print("[audit] ERROR: DB not connected. Cannot run lite-2021 mode.")
            session.close()
            return

        run_lite_2021(session, as_of, output_path, db_info, run_ts, git_head)
        session.close()
        return

    # ----------------------------------------------------------------
    # Default mode (original logic preserved below)
    # ----------------------------------------------------------------

    # Determine output path (relative to worktree root = backend/../)
    if args.output:
        output_path = Path(args.output)
    else:
        worktree_root = _BACKEND_DIR.parent
        output_path = worktree_root / "docs" / "paper_v2" / f"coverage_audit_{today.strftime('%Y%m%d')}.md"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[audit] run_ts={run_ts}  git={git_head[:12]}  as_of={as_of}")
    print(f"[audit] output -> {output_path}")

    # ----------------------------------------------------------------
    # Per-ticker coverage (Section 2)
    # ----------------------------------------------------------------
    all_tickers = list(_ALL_US_TICKERS) + list(_ALL_JP_TICKERS)
    ticker_summaries: list[dict] = []
    total_rows_queried = 0

    if db_info["connected"]:
        for ticker in all_tickers:
            s = _fetch_ticker_summary(session, ticker, SCAN_START, as_of)
            ticker_summaries.append(s)
            total_rows_queried += s["row_count"]
            print(f"  {ticker}: {s['row_count']:,} rows  {s['first_date']} .. {s['last_date']}")
    else:
        ticker_summaries = [
            {
                "ticker": t,
                "first_date": None,
                "last_date": None,
                "row_count": 0,
                "has_open": 0,
                "has_close": 0,
                "has_adjusted_close": 0,
            }
            for t in all_tickers
        ]

    # ----------------------------------------------------------------
    # Build CalendarService and full alignment
    # ----------------------------------------------------------------
    print("[audit] Building CalendarService...")
    # Use cache_start earlier than SCAN_START to allow previous_us_business_day to work
    calendar = CalendarService(cache_start="2004-01-01")
    alignment_all = calendar.build_date_alignment(SCAN_START, as_of)
    print(f"[audit] Full alignment rows ({SCAN_START} .. {as_of}): {len(alignment_all):,}")

    # ----------------------------------------------------------------
    # Bulk-fetch all prices for valid-row computation
    # ----------------------------------------------------------------
    all_tickers_set = list(_ALL_US_TICKERS) + list(_ALL_JP_TICKERS)
    print("[audit] Bulk-fetching all prices...")
    if db_info["connected"]:
        # Extra buffer for CC return (need previous trading day for us_date)
        from datetime import timedelta
        fetch_start = SCAN_START - timedelta(days=10)
        us_adj_map_28, us_sorted_28, jp_price_map = _fetch_all_prices(
            session, all_tickers_set, fetch_start, as_of
        )
    else:
        us_adj_map_28 = {}
        us_sorted_28 = {}
        jp_price_map = {}

    # ----------------------------------------------------------------
    # Compute valid flags for all 3 universes
    # ----------------------------------------------------------------
    print("[audit] Computing valid flags for U=28, U=27, U=26...")
    # U=28 (all US)
    valid_28 = _compute_valid_flags(
        alignment_all, _ALL_US_TICKERS, us_adj_map_28, us_sorted_28, jp_price_map
    )
    # U=27 (no XLC) — same jp_price_map
    us_adj_27 = {t: v for t, v in us_adj_map_28.items() if t != "XLC"}
    us_sorted_27 = {t: v for t, v in us_sorted_28.items() if t != "XLC"}
    valid_27 = _compute_valid_flags(
        alignment_all, _US_NO_XLC, us_adj_27, us_sorted_27, jp_price_map
    )
    # U=26 (no XLC, no XLRE)
    us_adj_26 = {t: v for t, v in us_adj_map_28.items() if t not in {"XLC", "XLRE"}}
    us_sorted_26 = {t: v for t, v in us_sorted_28.items() if t not in {"XLC", "XLRE"}}
    valid_26 = _compute_valid_flags(
        alignment_all, _US_NO_XLC_XLRE, us_adj_26, us_sorted_26, jp_price_map
    )

    # Build index map jp_date -> position
    jp_dates_all = list(alignment_all[COL_JP_EXECUTION_DATE])
    idx_map_all = {d: i for i, d in enumerate(jp_dates_all)}

    # ----------------------------------------------------------------
    # Section 3: C_full complete-case table
    # ----------------------------------------------------------------
    cfull_table: list[dict] = []
    for (c_start, c_end) in C_FULL_WINDOWS:
        mask = (alignment_all[COL_JP_EXECUTION_DATE] >= c_start) & (
            alignment_all[COL_JP_EXECUTION_DATE] <= c_end
        )
        positions = [i for i, m in enumerate(mask) if m]
        al = len(positions)
        cc28 = sum(valid_28[p] for p in positions)
        cc27 = sum(valid_27[p] for p in positions)
        cc26 = sum(valid_26[p] for p in positions)

        window_label = f"{c_start} .. {c_end}"
        if c_start == date(2019, 7, 1):
            # Only U=28 for the 2019-2024 window per spec
            cfull_table.append({
                "c_start": str(c_start), "c_end": str(c_end),
                "universe_label": "28 (all)",
                "alignment_days": al, "complete_case": cc28,
            })
        else:
            cfull_table.append({
                "c_start": str(c_start), "c_end": str(c_end),
                "universe_label": "28 (all)",
                "alignment_days": al, "complete_case": cc28,
            })
            cfull_table.append({
                "c_start": str(c_start), "c_end": str(c_end),
                "universe_label": "27 (no XLC)",
                "alignment_days": al, "complete_case": cc27,
            })
            if c_start == date(2010, 1, 1):
                cfull_table.append({
                    "c_start": str(c_start), "c_end": str(c_end),
                    "universe_label": "26 (no XLC, no XLRE)",
                    "alignment_days": al, "complete_case": cc26,
                })

    # ----------------------------------------------------------------
    # Section 4 & 5: Rolling by year
    # ----------------------------------------------------------------
    print("[audit] Computing rolling valid windows by year...")
    rolling_by_year = _compute_rolling_by_year(
        alignment_all, valid_28, valid_27, valid_26, BACKTEST_START, as_of
    )

    # ----------------------------------------------------------------
    # Section 6: Earliest viable start
    # ----------------------------------------------------------------
    print("[audit] Computing earliest viable start dates...")
    viable_28_date, viable_28_streak = _earliest_viable_start(
        alignment_all, valid_28, BACKTEST_START, as_of
    )
    viable_27_date, viable_27_streak = _earliest_viable_start(
        alignment_all, valid_27, BACKTEST_START, as_of
    )
    viable_26_date, viable_26_streak = _earliest_viable_start(
        alignment_all, valid_26, BACKTEST_START, as_of
    )
    viable = {
        "Universe=28 (all)": (viable_28_date, viable_28_streak),
        "Universe=27 (no XLC)": (viable_27_date, viable_27_streak),
        "Universe=26 (no XLC, no XLRE)": (viable_26_date, viable_26_streak),
    }

    # ----------------------------------------------------------------
    # Section 7: Options matrix
    # ----------------------------------------------------------------
    # Helper: count exec days in a year range per universe
    def exec_days_range(start_yr: int, end_yr: int, flag_key: str) -> int:
        flags_map = {"28": valid_28, "27": valid_27, "26": valid_26}
        flags = flags_map[flag_key]
        total = 0
        for yr in range(start_yr, end_yr + 1):
            d = rolling_by_year.get(yr, {})
            total += d.get(f"exec{flag_key}", 0)
        return total

    def get_cfull_cc(c_start_str: str, universe_label: str) -> int:
        for row in cfull_table:
            if row["c_start"] == c_start_str and row["universe_label"].startswith(universe_label[:2]):
                return row["complete_case"]
        return 0

    def exec_2019h2_plus(flag_key: str) -> int:
        flags_map = {"28": valid_28, "27": valid_27, "26": valid_26}
        flags = flags_map[flag_key]
        h2_start = date(2019, 7, 1)
        total = 0
        for i, row in alignment_all.iterrows():
            jp_date: date = row[COL_JP_EXECUTION_DATE]
            if jp_date < h2_start or jp_date > as_of:
                continue
            pos = idx_map_all.get(jp_date, -1)
            if pos < 0:
                continue
            preceding = sum(flags[:pos])
            if preceding >= WINDOW_SIZE:
                total += 1
        return total

    e2019h2_28 = exec_2019h2_plus("28")
    e2019h2_27 = exec_2019h2_plus("27")
    e2019h2_26 = exec_2019h2_plus("26")

    e2015_2025_28 = exec_days_range(2015, 2025, "28")
    e2015_2025_27 = exec_days_range(2015, 2025, "27")
    e2015_2025_26 = exec_days_range(2015, 2025, "26")

    options_matrix = [
        {
            "c_full": "2010-01-01 .. 2014-12-31", "universe": "28 (all)",
            "cfull_cc": get_cfull_cc("2010-01-01", "28"),
            "viable_start": str(viable_28_date) if viable_28_date else "N/A",
            "exec_2015_2025": e2015_2025_28, "exec_2019h2": e2019h2_28,
        },
        {
            "c_full": "2010-01-01 .. 2014-12-31", "universe": "27 (no XLC)",
            "cfull_cc": get_cfull_cc("2010-01-01", "27"),
            "viable_start": str(viable_27_date) if viable_27_date else "N/A",
            "exec_2015_2025": e2015_2025_27, "exec_2019h2": e2019h2_27,
        },
        {
            "c_full": "2010-01-01 .. 2014-12-31", "universe": "26 (no XLC, no XLRE)",
            "cfull_cc": get_cfull_cc("2010-01-01", "26"),
            "viable_start": str(viable_26_date) if viable_26_date else "N/A",
            "exec_2015_2025": e2015_2025_26, "exec_2019h2": e2019h2_26,
        },
        {
            "c_full": "2015-01-01 .. 2019-06-30", "universe": "28 (all)",
            "cfull_cc": get_cfull_cc("2015-01-01", "28"),
            "viable_start": str(viable_28_date) if viable_28_date else "N/A",
            "exec_2015_2025": e2015_2025_28, "exec_2019h2": e2019h2_28,
        },
        {
            "c_full": "2015-01-01 .. 2019-06-30", "universe": "27 (no XLC)",
            "cfull_cc": get_cfull_cc("2015-01-01", "27"),
            "viable_start": str(viable_27_date) if viable_27_date else "N/A",
            "exec_2015_2025": e2015_2025_27, "exec_2019h2": e2019h2_27,
        },
        {
            "c_full": "2019-07-01 .. 2024-12-31", "universe": "28 (all)",
            "cfull_cc": get_cfull_cc("2019-07-01", "28"),
            "viable_start": str(viable_28_date) if viable_28_date else "N/A",
            "exec_2015_2025": e2015_2025_28, "exec_2019h2": e2019h2_28,
        },
    ]

    # ----------------------------------------------------------------
    # Generate markdown
    # ----------------------------------------------------------------
    print("[audit] Generating markdown...")
    md = generate_markdown(
        run_ts=run_ts,
        git_head=git_head,
        db_info=db_info,
        scan_start=SCAN_START,
        scan_end=as_of,
        total_rows_queried=total_rows_queried,
        ticker_summaries=ticker_summaries,
        cfull_table=cfull_table,
        rolling_by_year=rolling_by_year,
        viable=viable,
        options_matrix=options_matrix,
    )

    output_path.write_text(md, encoding="utf-8")
    print(f"[audit] Written: {output_path}")

    session.close()


if __name__ == "__main__":
    main()
