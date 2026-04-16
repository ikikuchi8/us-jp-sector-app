# US-JP Sector Lead-Lag Backend

FastAPI + PostgreSQL バックエンド。  
Python 3.11+ / FastAPI / SQLAlchemy / Alembic

---

## ディレクトリ構成

```
backend/
├── app/
│   ├── main.py           # FastAPI エントリポイント
│   ├── config.py         # 設定値 (pydantic-settings)
│   ├── database.py       # DB接続・セッション管理
│   └── models/           # SQLAlchemy ORM モデル
│       ├── instrument.py
│       ├── price.py
│       ├── signal.py
│       └── backtest.py
├── alembic/              # Alembic マイグレーション
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── 20260415_f3a8b2c19e05_initial_schema.py
├── tests/                # pytest (Task 5-1 以降で追加)
├── alembic.ini           # Alembic 設定
├── .env                  # ローカル環境変数 (.env.example をコピーして作成)
├── .env.example          # 環境変数テンプレート
└── pyproject.toml
```

---

## セットアップ手順

### 1. Python 仮想環境の作成

```bash
cd backend
python3.11 -m venv .venv
source .venv/bin/activate
```

### 2. 依存パッケージのインストール

```bash
pip install -e ".[dev]"
```

### 3. 環境変数の設定

```bash
cp .env.example .env
# 必要に応じて .env を編集する
```

デフォルト値のままでも、docker-compose で起動した DB に接続できます。

### 4. PostgreSQL の起動 (Docker)

プロジェクトルートの `docker/` ディレクトリから実行します。

```bash
cd ../docker
docker compose up -d db
```

DB が healthy になるまで少し待ちます。

```bash
docker compose ps      # State が healthy になっていることを確認
```

### 5. バックエンドの起動 (ローカル)

```bash
cd ../backend
source .venv/bin/activate
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 6. 動作確認

```bash
curl http://localhost:8000/health
```

DB に接続できている場合のレスポンス例:

```json
{
  "status": "ok",
  "db": "connected",
  "timestamp": "2026-04-15T10:00:00.000000+00:00",
  "version": "0.1.0",
  "env": "development"
}
```

DB に接続できていない場合:

```json
{
  "status": "degraded",
  "db": "unreachable",
  ...
}
```

Swagger UI: http://localhost:8000/docs

---

## 環境変数一覧

| 変数名 | デフォルト | 説明 |
|---|---|---|
| `APP_ENV` | `development` | 実行環境 |
| `APP_DEBUG` | `true` | SQLログ出力など |
| `LOG_LEVEL` | `INFO` | ログレベル |
| `POSTGRES_USER` | `sector_user` | DB ユーザー |
| `POSTGRES_PASSWORD` | `sector_pass` | DB パスワード |
| `POSTGRES_DB` | `sector_db` | DB 名 |
| `POSTGRES_HOST` | `localhost` | DB ホスト |
| `POSTGRES_PORT` | `5432` | DB ポート |
| `CORS_ORIGINS` | `http://localhost:3000` | 許可 CORS オリジン |

---

---

## DB マイグレーション (Alembic)

### 前提
- DB (docker-compose の `db` サービス) が起動していること
- 仮想環境が有効化されていること (`source .venv/bin/activate`)
- `backend/` ディレクトリで実行すること

### 初回セットアップ: テーブルを作成する

```bash
cd backend
alembic upgrade head
```

成功すると 5 テーブルと market_type ENUM 型が作成されます。

### 現在のマイグレーション状態を確認する

```bash
alembic current
```

### マイグレーション履歴を確認する

```bash
alembic history --verbose
```

### ロールバック: 1 つ前のリビジョンに戻す

```bash
alembic downgrade -1
```

### ロールバック: 初期状態 (テーブルなし) に戻す

```bash
alembic downgrade base
```

### モデル変更後に新しいマイグレーションを自動生成する

```bash
# DB が起動していること・モデルを変更済みであることを確認してから実行
alembic revision --autogenerate -m "変更内容の説明"
```

> **注意**: autogenerate は差分を検出するが、完全ではない。  
> 生成されたファイルを必ず手動で確認・修正してから `alembic upgrade head` を実行すること。

---

---

## 初期データ投入 (Seed)

### 対象
- `instrument_master` テーブルに US 11件 + JP 17件 を投入する
- 冪等に動作する (再実行しても既存レコードは変更されない)

### 前提
- `alembic upgrade head` が適用済みであること
- DB が起動していること
- 仮想環境が有効化されていること

### 実行コマンド

```bash
cd backend
python -m app.scripts.seed_instruments
```

### 実行結果例 (初回)

```
10:00:00 INFO  ==================================================
10:00:00 INFO  instrument_master seed を開始します
10:00:00 INFO  US 11 件 + JP 17 件 = 合計 28 件
10:00:00 INFO  ==================================================
10:00:00 INFO  対象: 合計 28 件 / 投入予定 28 件 / スキップ 0 件 (既存)
10:00:00 INFO  投入完了: 28 件を INSERT しました。 (DB rowcount: 28)
10:00:00 INFO  現在の instrument_master: US 11 件 / JP 17 件 / 合計 28 件
10:00:00 INFO  seed が正常に完了しました。
```

### 実行結果例 (2回目以降・冪等確認)

```
10:00:00 INFO  対象: 合計 28 件 / 投入予定 0 件 / スキップ 28 件 (既存)
10:00:00 INFO  全レコードが既に投入済みです。処理を終了します。
10:00:00 INFO  seed が正常に完了しました。
```

### データ定義の場所

| ファイル | 内容 |
|---|---|
| `app/seed_data/instruments.py` | US / JP 全 ticker の定義 (データのみ) |
| `app/scripts/seed_instruments.py` | 投入スクリプト本体 |

---

## 価格データ取得 (Task 2-3)

### 取得対象

| 市場 | 銘柄数 | 例 |
|---|---|---|
| US | 11 | XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY |
| JP | 17 | 1617.T〜1633.T (NEXT FUNDS TOPIX-17) |

### 価格更新の実行 (Python から直接呼ぶ場合)

```python
from datetime import date
from app.database import SessionLocal
from app.services.price_service import PriceService

with SessionLocal() as session:
    svc = PriceService(session)
    result = svc.update_all_prices(date(2024, 1, 1), date(2025, 1, 10))
    print(result)
```

### fetcher の差し替え (テスト用)

`PriceService(session, fetcher=MockFetcher(...))` で `YFinanceFetcher` を任意の実装に差し替えられる。  
`PriceFetcher` Protocol を満たす任意のクラスが使用可能。

### 注意事項

- market (US/JP) は `seed_data/instruments.py` の定義を正本とする。呼び出し側から自由入力できない。
- yfinance の end パラメータは exclusive のため内部で +1 日して渡す。
- ticker 単位で commit する (fail-soft)。一部取得失敗しても他 ticker は保存される。

---

## 今後の実装予定 (Task 2-4 以降)

- `app/api/` — 各種 Router (/prices/update, /prices/status 等)
- `app/services/signal_*.py` — シグナル生成ロジック
- `app/api/` — signal / backtest API
- `tests/` — 追加 pytest テスト
