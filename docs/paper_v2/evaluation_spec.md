# paper_v2-lite Evaluation Specification

## 1. 目的

`simple_v1` / `paper_v1` / `paper_v2` の 3 シグナルを **公平な条件下で比較可能**にするための評価ルールを固定する。本文書はレポート作成前に凍結し、評価実行後に書き換えない（事後的な指標・期間の変更は結論を歪める）。

## 2. スコープ

### In scope
- 比較対象シグナルの列挙
- 評価期間・コスト・ユニバース等の実行条件
- 必須指標とその計算式
- skip 日の扱い
- ペアワイズ共通期間比較のルール
- レポート成果物のフォーマット
- 再現性担保のためのメタデータ要件

### Out of scope（別ドキュメント）
- 個別シグナルの実装仕様（`paper_v2-lite` 仕様書参照）
- `BacktestService` の内部アルゴリズム
- Sensitivity check（K×λ グリッド）— 本評価の結果を受けて別タスクとして判断
- バックフィル（2010 起点 paper_v2-full 版）の実行可否判断

## 3. 固定条件

| 項目 | 値 | 根拠 |
|---|---|---|
| 比較期間 | **2024-01-04 〜 評価実行時の前々営業日** | paper_v2 の OOS 開始日。比較の下限を揃えるため |
| 下限日 | 2024-01-04 (含む) | `PAPER_V2_OOS_START` |
| 上限日 | 実行時の JP 営業日 `today - 2 biz days` | 価格確定性確保（前日分の OHLC が確定している保証） |
| コスト | **0 (commission_rate=0, slippage_rate=0)** | 論文 Table 2 と整合、UX・実運用方針（日中取引で手数料実質ゼロ前提） |
| ユニバース | JP 17 業種固定（`1617.T` 〜 `1633.T`） | paper 系は内部で US 11 を参照 |
| ポジション | long 5 / short 5 等ウェイト、Σ\|w\|=2 | 全 signal_type 共通 |
| リバランス | 日次 | 全 signal_type 共通 |
| 執行価格 | JP open → close 相当の日次 OC return | `BacktestService` 既存契約 |
| 比較対象 | `simple_v1`, `paper_v1`, `paper_v2` | 3 系を等価条件で並べる |

## 4. 必須指標

各 signal_type 毎に以下を必ず出力する。

| 指標 | 記号 | 単位 |
|---|---|---|
| 年率リターン | AR | % |
| 年率ボラティリティ | RISK | % |
| リターン/リスク | R/R | — |
| 最大ドローダウン | MDD | %（負値） |
| 実行可能日数 | `executed_days` | 日 |
| alignment 日数 | `alignment_days` | 日 |
| スキップ日数 | `skipped_days` | 日 |
| スキップ率 | `skip_rate` | % |

## 5. 指標定義

日次リターン `R_t` は `BacktestService.DailyResult.daily_return` を用いる。スキップ日の扱いは §6 を参照。

年率換算は **252 営業日** を用いる:

```
AR   = mean(R_t) × 252
RISK = std(R_t, ddof=1) × sqrt(252)
R/R  = AR / RISK                # 年率換算は分子分母で相殺され daily 比と同値
```

累積リターン:

```
W_0  = 1
W_t  = prod_{τ=1..t} (1 + R_τ)    # skip 日は R_τ=0
```

最大ドローダウン:

```
MDD = min_t { W_t / max_{τ≤t}(W_τ) − 1 }     # 値は 0 以下、-0.10 = -10%
```

実行可能日数とスキップ率:

```
alignment_days = 評価期間内の JP 営業日総数
executed_days  = |{ t : signal_daily(signal_type, t) 存在かつ R_t 計算可能 }|
skipped_days   = alignment_days − executed_days
skip_rate      = skipped_days / alignment_days
```

## 6. スキップ日の扱い

**2 つのビューを併記する**（片方だけでは評価が歪む）。

### View A: 全期間（運用結果ビュー）

- 期間は **alignment_days（評価期間内の全 JP 営業日）**
- スキップ日は `daily_return = 0` として累積に carry-over
- 「この戦略を仮に運用していたら手元にいくら残るか」を反映
- スキップが多いと累積リターンが抑えられる → フェアなペナルティになる
- 全指標（AR / RISK / R/R / MDD）はこのビューで計算

### View B: 共通実行可能日サブセット（モデル性能ビュー）

- 期間は **3 系すべてで `executed_days` になっている日の共通集合**（intersection）
- この日集合だけで AR / RISK / R/R / MDD を再計算
- 「同じ日集合で比較したときのモデル性能差」を反映
- `paper_v2` の OOS 境界直後や `simple_v1` の履歴不足のような一方的制約を除外
- ペアワイズサブセットも同時に出す:
  - `simple_v1 ∩ paper_v1`
  - `simple_v1 ∩ paper_v2`
  - `paper_v1 ∩ paper_v2`
  - `simple_v1 ∩ paper_v1 ∩ paper_v2` （3-way intersection）

### ルール

**View A と View B を両方報告する**ことを必須とする。片方だけの報告は禁止。

## 7. 公平比較ルール

1. **期間の下限は 3 系統一で 2024-01-04**。各系の固有の最早開始日（paper_v1 は 2021-04-09 〜）ではなく、評価期間統一下限を守る。
2. **コストは 0 で揃える**。どれか 1 系だけコストを入れる比較は禁止。将来 cost>0 を見たくなった場合は**別レポート**として切る。
3. **ユニバースは JP 17 固定**。除外・追加は禁止。
4. **ランキング・側決定の仕様差は各 signal_type の設計に依存**し評価時には触らない（`simple_v1` は 17 業種を 3 long / 3 short、`paper_v1` / `paper_v2` は 5 long / 5 short など。現状はいずれも等ウェイト）。
5. **signal_score の絶対値は比較しない**。各系でスケールが異なる（`simple_v1` は生リターン、`paper_v2` は標準化空間）。
6. **評価期間は事前に固定**。結果を見てから「ここからここまで」を選び直す（P-hacking）ことは禁止。

## 8. レポート構造

出力: `docs/paper_v2/evaluation_report_<YYYYMMDD>.md`

### 必須セクション

1. **実行メタデータ**
   - 実行日時（UTC）
   - git commit SHA
   - backtest run IDs（3 本分）
   - DB スナップショット日付
   - `c0_version`（paper_v2 のみ）

2. **View A: 全期間サマリー表**

   | signal_type | AR | RISK | R/R | MDD | alignment_days | executed_days | skip_rate |
   |---|---:|---:|---:|---:|---:|---:|---:|
   | simple_v1 | ... | ... | ... | ... | N | ... | ... |
   | paper_v1  | ... | ... | ... | ... | N | ... | ... |
   | paper_v2  | ... | ... | ... | ... | N | ... | ... |

3. **View B: 共通実行可能日サブセット表**

   | subset | days | simple_v1 | paper_v1 | paper_v2 |
   |---|---:|---:|---:|---:|
   | 3-way ∩ | ... | AR/R/R/MDD | AR/R/R/MDD | AR/R/R/MDD |
   | simple_v1 ∩ paper_v2 | ... | — | — | — |
   | paper_v1 ∩ paper_v2 | ... | — | — | — |

4. **累積リターン時系列（CSV）**
   - `docs/paper_v2/evaluation_<YYYYMMDD>/cumulative_returns.csv` （3 列 + date）

5. **スキップ構造**
   - 年別 skip 日数テーブル（各 signal_type × 年）
   - skip 理由サマリー（`paper_v2` は `skip_reasons_summary` を活用）

6. **所見（短く）**
   - AR / R/R の大小関係
   - paper_v2 の期待される優位（論文 Table 2 の R/R=2.22）の実測値との乖離
   - 限界事項（N=559 日前後の標本サイズ、regime 偏重、コスト未考慮等）

### 任意セクション（余裕があれば）

- 累積リターン図（PNG or SVG）
- Fama-French / Carhart factor regression（論文 Table 3/4 再現）
- Newey-West 補正した t 値

**禁止**: レポートに推奨案を記述しない。数値と所見のみ。次アクション判断は PdM が別途行う。

## 9. 再現性要件

評価実行時、以下を `evaluation_<YYYYMMDD>/metadata.json` に保存:

```json
{
  "run_at": "ISO-8601 UTC",
  "git_sha": "...",
  "python_version": "...",
  "eval_period_start": "2024-01-04",
  "eval_period_end": "YYYY-MM-DD",
  "cost_params": {"commission_rate": 0.0, "slippage_rate": 0.0},
  "backtest_run_ids": {"simple_v1": N, "paper_v1": N, "paper_v2": N},
  "c0_version": "v1",
  "price_db_row_count": N,
  "price_db_last_date": "YYYY-MM-DD"
}
```

この metadata があれば、後日同じ DB 状態を再現したときに backtest が同値になることを検証できる（`BacktestService` が決定的前提）。

## 10. 評価フロー（参考）

実際の評価実行は本仕様に基づき別タスクで実施。想定フロー:

```
Step A: シグナル生成を 3 signal_type すべてで実行
        期間: 2024-01-04 〜 today - 2 biz days
        出力: signal_daily 行 (signal_type 別)

Step B: バックテストを 3 signal_type すべてで実行
        期間: 同上, cost=0
        出力: backtest_run / backtest_result_daily 行 (run_id 別)

Step C: 評価スクリプトで View A + View B の指標を抽出
        新規: backend/scripts/build_evaluation_report.py (別タスク)
        出力: evaluation_report_<YYYYMMDD>.md + CSV + metadata.json

Step D: レポートを PdM レビュー
        判断: sensitivity check に進むか、lite 版確定か、paper_v2-full へ進むか
```

Step A / B は既存 API / サービスで可能。Step C のみ新規スクリプトが必要。

## 11. 凍結宣言

本仕様は **評価レポート作成前に凍結** する。評価結果を見てから以下を**事後に変更することは禁止**:

- 比較期間の下限・上限
- コストの扱い
- 指標の選定
- skip 日の扱い方針
- View A / View B の両方併記ルール

変更が必要と判断された場合は、**別バージョン `evaluation_spec_v2.md` を起こし**、旧版レポートも保持する（過去の結論を書き換えない）。

## 12. Open points / 今後の判断事項

以下は本仕様では決めず、評価結果を見て別途判断する:

1. 評価期間を延長するか（現状は 2024-01-04 〜 today - 2）
2. cost>0 のサブレポートを追加するか
3. Sensitivity check (K ∈ {2,3,4,5} × λ ∈ {0.5..0.95}) に進むか
4. Fama-French / Carhart レグレッションを追加するか
5. paper_v2-full（2010 起点バックフィル + C_full 再構築）に進むか

---

**仕様バージョン**: 1.0
**凍結日**: 2026-04-17
**根拠ドキュメント**: v2.2-lite 仕様 §6、v2.2-lite addendum-1 §5-B、論文 2026_76.pdf §4.2–4.4
