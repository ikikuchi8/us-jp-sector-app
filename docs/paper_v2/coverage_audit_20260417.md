# paper_v2 Data Coverage Audit

## Section 1: Run Metadata

- **Run timestamp (UTC):** 2026-04-17T06:09:32Z
- **git HEAD:** `4b4c16d6a4fdd39d262a99928d9b1fdb9477c5bb`
- **DB:** connected (host/db masked) (connected: True)
- **DB row count (price_daily):** 36,421
- **Scan range:** 2005-01-01 .. 2026-04-17
- **Total rows queried from price_daily:** 36,421

## Section 2: Per-Ticker Coverage Table

| ticker | first_date | last_date | row_count | has_open | has_close | has_adjusted_close |
|--------|-----------|----------|-----------|----------|-----------|-------------------|
| XLB | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLC | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLE | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLF | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLI | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLK | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLP | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLRE | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLU | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLV | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| XLY | 2021-01-11 | 2026-04-16 | 1,322 | 1,322 | 1,322 | 1,322 |
| 1617.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1618.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1619.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1620.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1621.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1622.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1623.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1624.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1625.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1626.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1627.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1628.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1629.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1630.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1631.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1632.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |
| 1633.T | 2021-01-12 | 2026-04-16 | 1,287 | 1,287 | 1,287 | 1,287 |

## Section 3: C_full Complete-Case Row Count

| C_full window | Universe | Alignment days | Complete-case rows | % |
|---------------|----------|---------------|-------------------|---|
| 2010-01-01 .. 2014-12-31 | 28 (all) | 1,227 | 0 | 0.0% |
| 2010-01-01 .. 2014-12-31 | 27 (no XLC) | 1,227 | 0 | 0.0% |
| 2010-01-01 .. 2014-12-31 | 26 (no XLC, no XLRE) | 1,227 | 0 | 0.0% |
| 2015-01-01 .. 2019-06-30 | 28 (all) | 1,098 | 0 | 0.0% |
| 2015-01-01 .. 2019-06-30 | 27 (no XLC) | 1,098 | 0 | 0.0% |
| 2019-07-01 .. 2024-12-31 | 28 (all) | 1,346 | 974 | 72.4% |

## Section 4: Rolling L=60 Valid-Window Count by Year (2015+)

| Year | Alignment days | Executed (U=28) | Executed (U=27) | Executed (U=26) |
|------|--------------|----------------|----------------|----------------|
| 2015 | 244 | 0 | 0 | 0 |
| 2016 | 245 | 0 | 0 | 0 |
| 2017 | 247 | 0 | 0 | 0 |
| 2018 | 245 | 0 | 0 | 0 |
| 2019 | 241 | 0 | 0 | 0 |
| 2020 | 242 | 0 | 0 | 0 |
| 2021 | 245 | 179 | 179 | 179 |
| 2022 | 244 | 244 | 244 | 244 |
| 2023 | 246 | 246 | 246 | 246 |
| 2024 | 245 | 245 | 245 | 245 |
| 2025 | 243 | 243 | 243 | 243 |
| 2026 | 71 | 71 | 71 | 71 |

## Section 5: Skip Rate Projection

| Year | Alignment days | Skip (U=28) | Skip% (U=28) | Skip (U=27) | Skip% (U=27) | Skip (U=26) | Skip% (U=26) |
|------|--------------|------------|------------|------------|------------|------------|------------|
| 2015 | 244 | 244 | 100.0% | 244 | 100.0% | 244 | 100.0% |
| 2016 | 245 | 245 | 100.0% | 245 | 100.0% | 245 | 100.0% |
| 2017 | 247 | 247 | 100.0% | 247 | 100.0% | 247 | 100.0% |
| 2018 | 245 | 245 | 100.0% | 245 | 100.0% | 245 | 100.0% |
| 2019 | 241 | 241 | 100.0% | 241 | 100.0% | 241 | 100.0% |
| 2020 | 242 | 242 | 100.0% | 242 | 100.0% | 242 | 100.0% |
| 2021 | 245 | 66 | 26.9% | 66 | 26.9% | 66 | 26.9% |
| 2022 | 244 | 0 | 0.0% | 0 | 0.0% | 0 | 0.0% |
| 2023 | 246 | 0 | 0.0% | 0 | 0.0% | 0 | 0.0% |
| 2024 | 245 | 0 | 0.0% | 0 | 0.0% | 0 | 0.0% |
| 2025 | 243 | 0 | 0.0% | 0 | 0.0% | 0 | 0.0% |
| 2026 | 71 | 0 | 0.0% | 0 | 0.0% | 0 | 0.0% |

## Section 6: Earliest Viable Start Date per Universe

- **Universe=28 (all):** earliest viable start = `2021-04-09` (first date in a ≥250-consecutive-valid-window streak)
- **Universe=27 (no XLC):** earliest viable start = `2021-04-09` (first date in a ≥250-consecutive-valid-window streak)
- **Universe=26 (no XLC, no XLRE):** earliest viable start = `2021-04-09` (first date in a ≥250-consecutive-valid-window streak)

## Section 7: Options & Impact Matrix (DECISION INPUT — no winner)

| C_full period | Universe | C_full complete-case rows | Viable paper_v2 start | Exec days 2015-2025 | Exec days 2019-07+ |
|--------------|----------|--------------------------|----------------------|--------------------|--------------------|
| 2010-01-01 .. 2014-12-31 | 28 (all) | 0 | 2021-04-09 | 1157 | 1228 |
| 2010-01-01 .. 2014-12-31 | 27 (no XLC) | 0 | 2021-04-09 | 1157 | 1228 |
| 2010-01-01 .. 2014-12-31 | 26 (no XLC, no XLRE) | 0 | 2021-04-09 | 1157 | 1228 |
| 2015-01-01 .. 2019-06-30 | 28 (all) | 0 | 2021-04-09 | 1157 | 1228 |
| 2015-01-01 .. 2019-06-30 | 27 (no XLC) | 0 | 2021-04-09 | 1157 | 1228 |
| 2019-07-01 .. 2024-12-31 | 28 (all) | 974 | 2021-04-09 | 1157 | 1228 |

This table is decision input. The C_full period, universe, and paper_v2 recommended start date are human decisions made outside this document.

## Section 8: Raw Data Dump

<details>
<summary>Per-ticker coverage JSON</summary>

```json
[
  {
    "ticker": "XLB",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLC",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLE",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLF",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLI",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLK",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLP",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLRE",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLU",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLV",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "XLY",
    "first_date": "2021-01-11",
    "last_date": "2026-04-16",
    "row_count": 1322,
    "has_open": 1322,
    "has_close": 1322,
    "has_adjusted_close": 1322
  },
  {
    "ticker": "1617.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1618.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1619.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1620.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1621.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1622.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1623.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1624.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1625.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1626.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1627.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1628.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1629.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1630.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1631.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1632.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  },
  {
    "ticker": "1633.T",
    "first_date": "2021-01-12",
    "last_date": "2026-04-16",
    "row_count": 1287,
    "has_open": 1287,
    "has_close": 1287,
    "has_adjusted_close": 1287
  }
]
```

</details>

## Section 9: Limitations (objective, no recommendations)

- XLC first_date is 2021-01-11 — starts after backtest candidate start (2015-01-01).
- XLRE first_date is 2021-01-11 — not available for full C_full 2010-2014 window.
- C_full 2010-01-01 .. 2014-12-31 U=28 has 0 complete-case rows because XLC and/or XLRE are absent in that period.
