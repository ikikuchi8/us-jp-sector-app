# US-JP セクターローテーション アプリ

米国セクター ETF の騰落率を先行指標として、翌日の日本セクター ETF に対する **買い/売りシグナルを生成・バックテスト** するための個人用分析ツール。

---

## 主な機能

- **価格データ取得**: yfinance 経由で US・JP セクター ETF の日次 OHLCV を取得・保存
- **シグナル生成**: 2 種類のアルゴリズムで JP セクターの Long/Short ランキングを算出
- **バックテスト**: 生成済みシグナルを使った日次 PnL・累積リターン・シャープレシオ等の計算
- **フロントエンド**: 価格状況の確認・シグナル一覧・バックテスト結果の可視化

---

## ディレクトリ構成

```
us-jp-sector-app/
├── backend/                # FastAPI アプリケーション
│   ├── app/
│   │   ├── api/            # ルーター (prices, signals, backtest)
│   │   ├── models/         # SQLAlchemy ORM モデル
│   │   ├── repositories/   # DB アクセス層
│   │   ├── services/       # ビジネスロジック (signal, backtest, calendar)
│   │   ├── schemas/        # Pydantic リクエスト/レスポンス型
│   │   ├── seed_data/      # 銘柄マスタ・セクターマッピング定義
│   │   └── scripts/        # seed スクリプト
│   ├── alembic/            # DB マイグレーション
│   ├── tests/              # pytest テスト (314 件)
│   ├── pyproject.toml
│   └── .env.example
├── frontend/               # Next.js アプリケーション
│   ├── app/                # App Router ページ
│   │   ├── page.tsx        # メイン画面 (シグナル一覧)
│   │   └── backtest/       # バックテスト画面
│   ├── components/         # UI コンポーネント
│   ├── hooks/              # カスタムフック
│   ├── lib/                # API クライアント・型定義
│   └── .env.local.example
└── docker/
    └── docker-compose.yml  # PostgreSQL 16 のみ
```

---

## 技術スタック

| レイヤー | 技術 |
|---|---|
| Backend | Python 3.11 / FastAPI / SQLAlchemy / Alembic |
| Database | PostgreSQL 16 |
| データ取得 | yfinance |
| ML | scikit-learn (PCA, Ridge) / NumPy |
| Frontend | Next.js 14 / React 18 / TypeScript 5 |
| UI | TailwindCSS 3 / Recharts 2 |
| インフラ | Docker (DB のみ) |

---

## 対象ユニバース

### 米国セクター ETF（11 銘柄）

| ティッカー | セクター |
|---|---|
| XLB | 素材 |
| XLC | 通信サービス |
| XLE | エネルギー |
| XLF | 金融 |
| XLI | 資本財 |
| XLK | 情報技術 |
| XLP | 生活必需品 |
| XLRE | 不動産 |
| XLU | 公益事業 |
| XLV | ヘルスケア |
| XLY | 一般消費財 |

### 日本セクター ETF（17 銘柄）

| ティッカー | 業種名 |
|---|---|
| 1617.T | 食品 |
| 1618.T | エネルギー資源 |
| 1619.T | 建設・資材 |
| 1620.T | 素材・化学 |
| 1621.T | 医薬品 |
| 1622.T | 自動車・輸送機 |
| 1623.T | 鉄鋼・非鉄 |
| 1624.T | 機械 |
| 1625.T | 電機・精密 |
| 1626.T | 情報通信・サービスその他 |
| 1627.T | 電力・ガス |
| 1628.T | 運輸・物流 |
| 1629.T | 商社・卸売 |
| 1630.T | 小売 |
| 1631.T | 銀行 |
| 1632.T | 金融（除く銀行） |
| 1633.T | 不動産 |

---

## セットアップ手順

### 前提

- Docker Desktop が起動していること
- Python 3.11+ がインストールされていること
- Node.js 18+ がインストールされていること

---

### 1. DB 起動

```bash
cd docker
docker compose up -d
```

PostgreSQL 16 が `localhost:5432` で起動します。

---

### 2. backend セットアップ

```bash
cd backend

# 仮想環境を作成・有効化
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 依存パッケージをインストール
pip install -e ".[dev]"

# 環境変数ファイルを作成
cp .env.example .env
# .env を必要に応じて編集（DB 接続情報など）
```

---

### 3. マイグレーション

```bash
cd backend
alembic upgrade head
```

`instrument_master`, `price_daily`, `signal_daily`, `backtest_run`, `backtest_result_daily` テーブルが作成されます。

---

### 4. シードデータ投入

```bash
cd backend
python -m app.scripts.seed_instruments
```

US 11 銘柄・JP 17 銘柄の銘柄マスタが登録されます（冪等・再実行安全）。

---

### 5. backend 起動

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

API ドキュメント: http://localhost:8000/docs

---

### 6. frontend セットアップ

```bash
cd frontend

# 依存パッケージをインストール
npm install

# 環境変数ファイルを作成
cp .env.local.example .env.local
# NEXT_PUBLIC_API_BASE_URL=http://localhost:8000 が設定されていることを確認
```

---

### 7. frontend 起動

```bash
cd frontend
npm run dev
```

http://localhost:3000 でアクセスできます。

---

## 環境変数

### backend （`backend/.env`）

| 変数名 | デフォルト | 説明 |
|---|---|---|
| `APP_ENV` | `development` | 実行環境 |
| `APP_DEBUG` | `true` | デバッグモード |
| `LOG_LEVEL` | `INFO` | ログレベル |
| `POSTGRES_USER` | `sector_user` | DB ユーザー |
| `POSTGRES_PASSWORD` | `sector_pass` | DB パスワード |
| `POSTGRES_DB` | `sector_db` | DB 名 |
| `POSTGRES_HOST` | `localhost` | DB ホスト |
| `POSTGRES_PORT` | `5432` | DB ポート |
| `CORS_ORIGINS` | `http://localhost:3000` | 許可オリジン（カンマ区切り） |
| `PRICE_DATA_SOURCE` | `yfinance` | 価格データソース |

### frontend （`frontend/.env.local`）

| 変数名 | デフォルト | 説明 |
|---|---|---|
| `NEXT_PUBLIC_API_BASE_URL` | `http://localhost:8000` | backend の URL |

---

## 主要 API エンドポイント

API ドキュメント（Swagger UI）: http://localhost:8000/docs

| メソッド | パス | 説明 |
|---|---|---|
| `GET` | `/health` | ヘルスチェック |
| `POST` | `/prices/update` | 価格データを yfinance から取得・保存 |
| `GET` | `/prices/status` | 最新価格取得日の状況確認 |
| `POST` | `/signals/generate` | 指定期間のシグナルを生成・保存 |
| `GET` | `/signals/latest` | 最新 JP 執行日のシグナル一覧を取得 |
| `POST` | `/backtest/run` | バックテストを実行し結果を保存 |
| `GET` | `/backtest/{run_id}/daily` | バックテスト日次結果を取得 |

### `POST /prices/update` リクエスト例

```json
{
  "scope": "all",
  "start_date": "2021-01-01",
  "end_date": "2026-04-16"
}
```

### `POST /signals/generate` リクエスト例

```json
{
  "start_date": "2021-04-09",
  "end_date": "2026-04-16",
  "signal_type": "paper_v1"
}
```

### `POST /backtest/run` リクエスト例

```json
{
  "start_date": "2021-04-09",
  "end_date": "2026-04-16",
  "signal_type": "paper_v1",
  "commission_rate": 0.001,
  "slippage_rate": 0.001
}
```

---

## シグナル種別

### simple_v1

- **アルゴリズム**: 前日 US セクター ETF の終値騰落率を JP セクターにマッピングし、スコアで Long/Short をランキング
- **先読み防止**: US シグナル日 = JP 執行日の前営業日（US 引け後に JP を執行）
- **必要データ量**: 当日の US 終値 2 日分のみ（数日分の価格があれば生成可能）
- **用途**: シンプルな動作確認・比較ベースライン

### paper_v1（デフォルト）

- **アルゴリズム**: PCA + Ridge 回帰による機械学習モデル
  1. 訓練ウィンドウ（直近 120 暦日）の US/JP 日次リターンを取得
  2. US リターン行列を PCA で次元削減（累積寄与率 ≥ 80%、最大 5 成分）
  3. JP セクターリターンを目的変数として Ridge 回帰（α=1.0）を訓練
  4. 当日の US リターンを入力としてスコアを予測
  5. 上位 5 業種を Long・下位 5 業種を Short に分類
- **先読み防止**:
  - 訓練末尾 = JP 執行日 − 1 日（当日データを訓練に含めない）
  - 当日 US リターン = `get_prices_up_to(as_of_date=us_signal_date)` で取得
- **必要データ量**: 有効な訓練行が 60 行以上（≒ 120 暦日の価格履歴）
- **最古生成可能日**: `2021-04-09`（価格データが 2021-01-11 から存在するため）
- **UI デフォルト**: メイン画面・バックテスト画面ともに `paper_v1` がデフォルト選択

> ⚠️ paper_v1 は訓練データが不足している日付をスキップします。シグナル生成時に「全日スキップ」トーストが表示された場合は、十分な期間の価格データを先に取得してください。

---

## 画面構成

### メイン画面 (`/`)

- **データ状況バー**: US・JP それぞれの価格最終取得日を表示
- **価格更新ボタン**: 直近 7 日分の価格を取得（より長い期間は API を直接叩く）
- **シグナル生成フォーム**: 開始日・終了日・signal_type（simple_v1 / paper_v1）を指定して実行
- **シグナル一覧テーブル**: 最新 JP 執行日のランク・業種・スコア・Side を表示

### バックテスト画面 (`/backtest`)

- **実行フォーム**: 開始日・終了日・signal_type・手数料率・スリッページ率を指定
- **サマリーカード**: 実行条件（期間・種別・コスト）と計算結果（取引日数・累積リターン・シャープレシオ等）を表示
- **累積リターンチャート**: 日次の累積リターン推移
- **日次結果テーブル**: 日付ごとの日次リターン・Long/Short 内訳

---

## 手動確認チェックリスト

初回セットアップ後、以下の順番で動作を確認してください。

```
[ ] 1. docker compose up -d でコンテナが起動する
[ ] 2. alembic upgrade head でエラーなくマイグレーションが完了する
[ ] 3. seed_instruments でエラーなく銘柄マスタが投入される
[ ] 4. GET /health が {"status": "ok"} を返す
[ ] 5. POST /prices/update (scope=all, 過去 5 年分) で価格が取得される
[ ] 6. POST /signals/generate (signal_type=paper_v1, 2021-04-09 〜 今日) でシグナルが生成される
       ※ API タイムアウト (15 秒) が出ても DB には保存され続けるので完了まで待つ
[ ] 7. GET /signals/latest?signal_type=paper_v1 で 17 業種分のシグナルが返る
[ ] 8. フロントエンドのメイン画面でシグナル一覧が表示される
[ ] 9. バックテスト画面で paper_v1 を実行し、サマリーと日次グラフが表示される
```

---

## 既知の制約

- **価格更新ボタンは直近 7 日のみ**: 長期間の価格取得は `POST /prices/update` を curl 等で直接実行する必要がある
- **シグナル生成の API タイムアウト**: フロントエンドの API クライアントは 15 秒でタイムアウトするが、バックエンド処理は継続する。タイムアウトが出た場合は DB を確認して完了を待つ
- **paper_v1 の最古生成可能日**: 価格データ開始日 (2021-01-11) から 60 有効行が揃う `2021-04-09` 以降のみ生成可能
- **バックテスト結果の状態保持**: 画面遷移するとバックテスト結果はリセットされる（フォーム入力値は sessionStorage で保持）
- **simple_v1 のシグナルデータ**: デフォルト生成対象外のため、必要な場合は明示的に生成する
- **本番環境非対応**: 認証・認可なし。ローカル開発用途のみを想定

---

## 今後の拡張候補

- 価格更新フォームへの任意期間指定 UI の追加
- シグナル生成の非同期ジョブ化（タイムアウト問題の根本解決）
- paper_v1 以外の新しいシグナルアルゴリズムの追加
- バックテスト結果の複数 run 比較機能
- ポジションサイジング（現在は等ウェイト固定）のパラメータ化
- 価格データソースの yfinance 以外への対応
