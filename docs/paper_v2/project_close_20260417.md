# paper_v2-lite Project Close (2026-04-17)

本ドキュメントは paper_v2-lite フェーズの終了宣言と、次フェーズ着手時の入口を 1 枚にまとめる。

## 1. 完了範囲

### 1.1 実装タスク（Task 7-0 〜 7-6 + Step 1 〜 Step C）

| フェーズ | スコープ | commit |
|---|---|---|
| Task 7-0 | Data coverage audit (full + 2021+ lite) | `61f4580` |
| Task 7-1 | V_0 subspace utility | `fb81eb0` |
| Task 7-2 | C_0 builder pure function | `2a6a394` |
| Task 7-C0 | artifact build script + c0_v1 artifact | `415c4d1`, `6f5518b` |
| Task 7-3 | Subspace-regularized PCA core | `0e6fa69` |
| Task 7-4 | PaperV2SignalService + Codex P1/P2 remediation | `81b72d0`, `6d97770` |
| Task 7-5 | API/router wiring + backtest smoke test | `e2e78c5`, `1234157` |
| Task 7-6 | Frontend select + toast UX | `a5c1a09`, `f22bd61` |
| Step 1 | evaluation_spec.md freeze | `e02ad3d` |
| Step 2/B | run_evaluation_phase.py + first run JSON | `39ef413`, `be19d2c` |
| Step C | build_evaluation_report.py + first report | `8e980d7` |
| Close | project_close_20260417.md | (このコミット) |

全 commit は `main` ブランチに積載。

### 1.2 テスト・品質保証

- **backend pytest: 450 tests pass**
- **frontend TypeScript: `tsc --noEmit` 0 errors**
- 外部 Codex review: 2 ラウンド（Task 7-1/7-2/7-C0 / Task 7-3/7-4）完了、全指摘反映済み

### 1.3 主要成果物

- サービス: `backend/app/services/paper_v2/` (7 モジュール)
- artifact: `backend/app/services/paper_v2/data/c0_v1.{npz,meta.json}`
- スクリプト: `audit_paper_v2_coverage.py`, `build_paper_v2_prior.py`, `run_evaluation_phase.py`, `build_evaluation_report.py`
- ドキュメント: `docs/paper_v2/` 配下 (監査・期間設計・評価仕様・評価レポート)

## 2. 評価結果（観測事実のみ）

評価期間: 2024-01-04 〜 2026-04-15、U=28 fixed、cost=0、L=60、K=3、λ=0.9

### 2.1 View A（全期間、557 alignment / 556 executed）

| signal_type | AR (%) | RISK (%) | R/R | MDD (%) |
|---|---:|---:|---:|---:|
| simple_v1 | 31.65 | 6.90 | 4.584 | -4.23 |
| paper_v1 | 10.95 | 5.55 | 1.973 | -3.35 |
| paper_v2 | 3.01 | 6.29 | 0.479 | -9.72 |

### 2.2 View B 3-way intersection (556 days)

View A と数値はほぼ同一（skip 1 日のみ、3 系列で整合）。

### 2.3 参照

- backtest_run_ids: `simple_v1=54`, `paper_v1=55`, `paper_v2=56`
- 詳細レポート: [`docs/paper_v2/evaluation_reports/report_20260417_195912/`](evaluation_reports/report_20260417_195912/)
- 実行メタデータ: [`docs/paper_v2/evaluation_runs/run_20260417_130325.json`](evaluation_runs/run_20260417_130325.json)

## 3. 未解決課題

### 3.1 成績の説明がついていない（**次フェーズ主題**）

- **`paper_v2` が `paper_v1` より劣る**: 設計上は論文準拠度が高いはずが、View A/B 双方で下回る
- **`simple_v1` が論文報告値を大幅に超える**: 論文 Table 2 の MOM は R/R=0.53、本評価の simple_v1 は 4.58
- View A/B 差がほぼ無いため、skip 起因ではない

### 3.2 データソース・定義の opacity（**次フェーズ入口**）

- `price_daily` の出所（データ提供者、API、取得ジョブ）の明示的ドキュメントなし
- `adjusted_close_price` の定義（splits-only か、dividends 込みか）未確認
- US / JP のタイムゾーン・終値確定時刻の境界処理が論文 §3.1「same day」定義と整合するか未検証
- 特殊日（半日取引、臨時休場）のカレンダー整合性未検証
- `sector_mapping.py` の主観的対応（例: 1622.T 自動車・輸送機、1629.T 商社・卸売）が結果に効いているかの定量検証なし

### 3.3 パラメータ・期間の制約

- `C_full = 2021-01-12 .. 2023-12-31` はコロナ後〜利上げサイクル偏重
- OOS = 2024-01-04〜 で約 2.2 年、標本として thin
- K=3, λ=0.9 は論文固定値のまま採用、現 DB / regime に対する sensitivity 未検証
- cost=0 は理想条件

### 3.4 評価拡張の未実施

- Sensitivity check (K × λ グリッド)
- Fama-French 3 / Carhart 4 factor regression
- Cost > 0 サブレポート
- paper_v2-full (2010 起点バックフィル、`C_full` 2010-2014 再構築)

## 4. 次フェーズの入口

次フェーズは **原因分析 / データ深掘り** から再開する（PdM 指示）。

### 4.1 最優先の調査候補

- **価格データの実態**
  - `price_daily` の取得ロジック (`backend/app/services/price_service.py`, `price_fetcher.py`) の再読
  - データ提供者・API の明示（環境変数、migration、seed script）
  - `adjusted_close_price` 算出ルールがアプリ側か提供者側か
  - 欠損日の背景（休場 vs データ取得失敗）分類
- **タイムゾーンと境界処理**
  - US 4/16 が 2026-04-17 に取り込まれるタイミング等、DB 書き込み時刻と market close 時刻の関係
  - 論文 §3.1 の「same day US close to close」が現実装でどの日付にマップされているか
- **universe / mapping**
  - US 11 ETF 選定根拠
  - JP 17 ETF の GICS マッピング、特に主観判断業種の代替案比較

### 4.2 推奨着手順序（PdM 判断マター）

1. `price_daily` の取得経路とソース特定
2. 特定日を選び raw 価格 → CC return → signal_daily まで手計算で検算
3. 必要なら seed / fetch ロジック再設計を別フェーズで

### 4.3 再開時のブランチ戦略

- 原因分析フェーズは別ブランチ（例: `investigate/data-source-audit`）を切る
- `main` は paper_v2-lite の「動作する最終版」として保持
- 改修は新しい `signal_type` や version として並列追加する形を想定（既存系列を壊さない）

## 5. 参照ドキュメント

- [`docs/paper_v2/coverage_audit_20260417.md`](coverage_audit_20260417.md) — Task 7-0 監査
- [`docs/paper_v2/period_design_lite_20260417.md`](period_design_lite_20260417.md) — 期間設計（Option A 採用根拠）
- [`docs/paper_v2/evaluation_spec.md`](evaluation_spec.md) — 評価仕様（凍結版 v1.0）
- [`docs/paper_v2/evaluation_runs/run_20260417_130325.json`](evaluation_runs/run_20260417_130325.json) — 評価実行メタ
- [`docs/paper_v2/evaluation_reports/report_20260417_195912/`](evaluation_reports/report_20260417_195912/) — 評価レポート（View A/B 表、CSV、metadata）
- [`backend/app/services/paper_v2/data/c0_v1.meta.json`](../../backend/app/services/paper_v2/data/c0_v1.meta.json) — prior artifact メタ
- 論文一次資料: `txt/2026_76.pdf`（改変禁止）

## 6. クローズ宣言

本クローズ時点で `paper_v2-lite` は「**動作する論文準拠 lite 実装 + 初版評価レポート付き**」として成立している。

次フェーズは PdM 主導の「原因分析 / データ深掘り」として **別スレッド・別ブランチで再開**する。

本ドキュメントは実装側の区切り宣言であり、**評価結果の解釈・優劣判断・次の推奨方針は含まない**。
