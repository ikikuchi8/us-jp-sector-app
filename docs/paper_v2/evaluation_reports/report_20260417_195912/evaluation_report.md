# paper_v2-lite Evaluation Report (2026-04-17)

## 1. 実行メタデータ

- run_at: 2026-04-17T13:03:25Z
- git_sha: 39ef413be605bf7328a5b6b0e3372b77585edd0f-dirty
- backtest_run_ids: simple_v1=54, paper_v1=55, paper_v2=56
- DB snapshot: price_daily rows 36438, last_date 2026-04-17
- c0_version: v1
- 評価期間: 2024-01-04 〜 2026-04-15
- コスト: commission=0.0, slippage=0.0

## 2. View A: 全期間サマリー

| signal_type | AR (%) | RISK (%) | R/R | MDD (%) | alignment_days | executed_days | skip_rate (%) |
|---|---:|---:|---:|---:|---:|---:|---:|
| simple_v1 | 31.65 | 6.90 | 4.584 | -4.23 | 557 | 556 | 0.18 |
| paper_v1 | 10.95 | 5.55 | 1.973 | -3.35 | 557 | 556 | 0.18 |
| paper_v2 | 3.01 | 6.29 | 0.479 | -9.72 | 557 | 556 | 0.18 |

## 3. View B: 共通実行可能日サブセット

### 3.1 3-way intersection (556 days)

| signal_type | AR (%) | RISK (%) | R/R | MDD (%) |
|---|---:|---:|---:|---:|
| simple_v1 | 31.71 | 6.91 | 4.589 | -4.23 |
| paper_v1 | 10.97 | 5.55 | 1.975 | -3.35 |
| paper_v2 | 3.02 | 6.29 | 0.479 | -9.72 |

### 3.2 Pairwise

#### simple_v1 ∩ paper_v1 (556 days)

| signal_type | AR (%) | RISK (%) | R/R | MDD (%) |
|---|---:|---:|---:|---:|
| simple_v1 | 31.71 | 6.91 | 4.589 | -4.23 |
| paper_v1 | 10.97 | 5.55 | 1.975 | -3.35 |

#### simple_v1 ∩ paper_v2 (556 days)

| signal_type | AR (%) | RISK (%) | R/R | MDD (%) |
|---|---:|---:|---:|---:|
| simple_v1 | 31.71 | 6.91 | 4.589 | -4.23 |
| paper_v2 | 3.02 | 6.29 | 0.479 | -9.72 |

#### paper_v1 ∩ paper_v2 (556 days)

| signal_type | AR (%) | RISK (%) | R/R | MDD (%) |
|---|---:|---:|---:|---:|
| paper_v1 | 10.97 | 5.55 | 1.975 | -3.35 |
| paper_v2 | 3.02 | 6.29 | 0.479 | -9.72 |

## 4. スキップ構造

| signal_type | alignment_days | executed_days | skipped_days | skip_rate (%) | skip_reasons_summary |
|---|---:|---:|---:|---:|---|
| simple_v1 | 557 | 556 | 1 | 0.18 | {} |
| paper_v1 | 557 | 556 | 1 | 0.18 | {} |
| paper_v2 | 557 | 556 | 1 | 0.18 | {} |

## 5. 累積リターン時系列

詳細 CSV: `cumulative_returns.csv`

最終累積リターン (W_last - 1):
- simple_v1: 100.14%
- paper_v1: 26.93%
- paper_v2: 6.41%

## 6. 所見 (factual only)

- View A で R/R の最大値は `simple_v1` の `4.584`
- View B 3-way intersection で R/R の最大値は `simple_v1` の `4.589`
- `paper_v2` の skip 理由は `{}` (skip 無し)

## 7. 限界事項

- 評価期間: 557 alignment days (年率換算で約 2.2 年のサンプル)
- regime: 2024-01-04 以降（lite 版の制約、2010 以降ではない）
- コスト: 0 固定 (実運用前提の簡略化)
- ユニバース: U=28 fixed
