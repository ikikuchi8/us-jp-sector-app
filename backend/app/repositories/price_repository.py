"""
PriceRepository: price_daily テーブルへの CRUD 操作。

# 責務
  - price_daily テーブルの読み書きに特化する
  - ビジネスロジック (計算・判断) は持たない
  - SQLAlchemy Session を DI で受け取る; commit は呼び出し側が行う

# 先読み防止の核心
  get_prices_up_to(ticker, as_of_date) は
  WHERE business_date <= as_of_date を SQL レベルで適用する。
  signal 層では as_of_date = us_signal_date として呼ぶことで、
  jp_execution_date 以降のデータへのアクセスが構造的に不可能になる。

# 返り値ポリシー
  - 単一行: PriceDaily | None  (未存在 → None、例外なし)
  - 複数行: list[PriceDaily]  (未存在 → [], 例外なし)
  - upsert_many: int           (入力件数を返す)

# ソート順
  - list 系メソッドはすべて business_date 昇順
  - get_prices_up_to で limit 指定時: 内部的に DESC で取得して最新 N 件を得た後、
    Python 側で逆順にして昇順で返す
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.price import PriceDaily

# upsert_many で INSERT する全カラム (PK の id は autoincrement のため除外)
_INSERT_FIELDS: tuple[str, ...] = (
    "ticker",
    "market",
    "business_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adjusted_close_price",
    "volume",
    "data_source",
    "fetched_at",
)

# upsert_many の DO UPDATE SET 対象フィールド
# 衝突キー (ticker, business_date) と PK (id) は除外する
_UPDATABLE_FIELDS: tuple[str, ...] = (
    "market",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adjusted_close_price",
    "volume",
    "data_source",
    "fetched_at",
)


class PriceRepository:
    """price_daily テーブルへのアクセスを担う Repository。

    Args:
        session: SQLAlchemy Session。commit / rollback は呼び出し側が行う。
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # 単一行取得
    # ------------------------------------------------------------------

    def get_by_ticker_and_date(
        self, ticker: str, business_date: date
    ) -> PriceDaily | None:
        """(ticker, business_date) で一意なレコードを返す。

        Returns:
            該当レコード。存在しない場合は None。
        """
        stmt = select(PriceDaily).where(
            PriceDaily.ticker == ticker,
            PriceDaily.business_date == business_date,
        )
        return self._session.scalars(stmt).one_or_none()

    # ------------------------------------------------------------------
    # 複数行取得
    # ------------------------------------------------------------------

    def list_by_ticker(
        self,
        ticker: str,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> list[PriceDaily]:
        """ticker の価格リストを business_date 昇順で返す。

        Args:
            ticker: ティッカーシンボル。
            start:  取得開始日 (inclusive)。None の場合は制限なし。
            end:    取得終了日 (inclusive)。None の場合は制限なし。

        Returns:
            business_date 昇順のリスト。該当なしは空リスト。
        """
        stmt = select(PriceDaily).where(PriceDaily.ticker == ticker)
        if start is not None:
            stmt = stmt.where(PriceDaily.business_date >= start)
        if end is not None:
            stmt = stmt.where(PriceDaily.business_date <= end)
        stmt = stmt.order_by(PriceDaily.business_date.asc())
        return list(self._session.scalars(stmt).all())

    def get_prices_up_to(
        self,
        ticker: str,
        as_of_date: date,
        limit: int | None = None,
    ) -> list[PriceDaily]:
        """as_of_date 以前 (inclusive) の価格を business_date 昇順で返す。

        # 先読み防止の核心
          WHERE business_date <= as_of_date を SQL レベルで強制する。
          as_of_date より後のデータはいかなる場合も返さない。

        # signal 層での使い方
          signal 生成時は as_of_date = us_signal_date として呼ぶ。
          これにより jp_execution_date (未来) のデータへのアクセスが
          構造的に不可能になる。

        Args:
            ticker:     ティッカーシンボル。
            as_of_date: この日付以前のデータのみ返す (inclusive)。
            limit:      取得上限件数。None の場合は全件。
                        指定時は「最新の limit 件」を昇順で返す。

        Returns:
            business_date 昇順のリスト。該当なしは空リスト。
        """
        stmt = (
            select(PriceDaily)
            .where(PriceDaily.ticker == ticker)
            .where(PriceDaily.business_date <= as_of_date)  # ← 先読み防止
            .order_by(PriceDaily.business_date.desc())       # 最新から取得して limit を適用
        )
        if limit is not None:
            stmt = stmt.limit(limit)

        rows = list(self._session.scalars(stmt).all())
        rows.reverse()  # 降順で取得したものを昇順に戻して返す
        return rows

    def get_oc_on_date(
        self,
        business_date: date,
        tickers: Iterable[str],
    ) -> dict[str, tuple[Decimal, Decimal] | None]:
        """指定日の (open_price, close_price) を ticker ごとに返す。

        # 先読み防止
          WHERE business_date = :date のみで取得する。
          指定日以外のデータは SQL レベルで除外される。
          BacktestService はこのメソッドに jp_execution_date を渡して
          当日の open/close だけを取得する。翌日以降を誤って渡さないのは
          呼び出し側 (BacktestService) の責務。

        # 欠損ルール
          - 対応行が存在しない ticker    → None
          - open_price が None の ticker → None
          - close_price が None の ticker → None
          open/close の両方が揃っている場合のみ (open, close) を返す。

        Args:
            business_date: 取得対象の営業日 (= jp_execution_date)。
            tickers:       取得対象のティッカーシンボルの iterable。

        Returns:
            {ticker: (open_price, close_price) | None}
            tickers に含まれる全 ticker をキーとする dict。
        """
        tickers_list = list(tickers)
        if not tickers_list:
            return {}

        stmt = (
            select(PriceDaily)
            .where(PriceDaily.business_date == business_date)  # ← 当日のみ (先読み防止)
            .where(PriceDaily.ticker.in_(tickers_list))
        )
        rows_by_ticker = {
            r.ticker: r for r in self._session.scalars(stmt).all()
        }

        result: dict[str, tuple[Decimal, Decimal] | None] = {}
        for ticker in tickers_list:
            row = rows_by_ticker.get(ticker)
            if (
                row is None
                or row.open_price is None
                or row.close_price is None
            ):
                result[ticker] = None
            else:
                result[ticker] = (row.open_price, row.close_price)
        return result

    def get_prices_between(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[PriceDaily]:
        """[start_date, end_date] 内の価格を business_date 昇順で返す。

        Args:
            ticker:     ティッカーシンボル。
            start_date: 取得開始日 (inclusive)。
            end_date:   取得終了日 (inclusive)。

        Returns:
            business_date 昇順のリスト。該当なしは空リスト。
        """
        stmt = (
            select(PriceDaily)
            .where(PriceDaily.ticker == ticker)
            .where(PriceDaily.business_date >= start_date)
            .where(PriceDaily.business_date <= end_date)
            .order_by(PriceDaily.business_date.asc())
        )
        return list(self._session.scalars(stmt).all())

    # ------------------------------------------------------------------
    # 書き込み
    # ------------------------------------------------------------------

    def upsert_many(self, rows: Sequence[PriceDaily]) -> int:
        """複数行を INSERT ... ON CONFLICT DO UPDATE で一括 upsert する。

        # upsert 戦略 (DB レベル)
          衝突キー: (ticker, business_date) = uq_price_daily_ticker_date
            - 既存レコードなし → INSERT
            - 既存レコードあり → _UPDATABLE_FIELDS のみ DO UPDATE SET
          1 SQL 文で完結するため、Python 側での SELECT / 比較は不要。

        # ダイアレクト対応
          PostgreSQL (本番): INSERT ... ON CONFLICT (ticker, business_date) DO UPDATE SET ...
          SQLite (テスト):   同等の ON CONFLICT 構文 (SQLite 3.24+)
          その他:            NotImplementedError

        # 返り値の定義
          入力 rows の件数を返す。
          insert / update の内訳は DB 内部で処理されるため外部には不透明。

        Args:
            rows: 保存対象の PriceDaily リスト。

        Returns:
            入力 rows の件数。空リスト入力時は 0。

        Note:
            commit は呼び出し側が行う。
            execute 後に session.expire_all() を呼ぶため、セッションに
            キャッシュされた PriceDaily インスタンスは次回アクセス時に
            DB から再ロードされる (同一トランザクション内で最新値を参照できる)。
        """
        if not rows:
            return 0

        rows_data = [_to_dict(r) for r in rows]
        stmt = _build_upsert_stmt(self._session, rows_data)
        self._session.execute(stmt)
        self._session.expire_all()
        return len(rows)


# ---------------------------------------------------------------------------
# モジュールプライベート
# ---------------------------------------------------------------------------

def _to_dict(row: PriceDaily) -> dict[str, Any]:
    """PriceDaily ORM オブジェクトを INSERT 用 dict に変換する。

    _INSERT_FIELDS に列挙されたカラムを含む。
    id は None の場合のみ省略する (PostgreSQL SERIAL / SQLite autoincrement に委ねる)。
    id が明示的に設定されている場合は dict に含める。

    Note:
        SQLite は BIGINT PRIMARY KEY を rowid alias として扱わないため、
        SQLite テストでは明示的な id を提供する必要がある。
        本番 (PostgreSQL) では id=None のまま呼ぶと SERIAL が自動採番する。
    """
    d: dict[str, Any] = {field: getattr(row, field) for field in _INSERT_FIELDS}
    if row.id is not None:
        d["id"] = row.id
    return d


def _build_upsert_stmt(session: Session, rows_data: list[dict[str, Any]]):
    """ダイアレクトに応じた INSERT ... ON CONFLICT DO UPDATE 文を生成する。

    Args:
        session:   現在の Session。dialect 判定に session.bind を使用する。
        rows_data: INSERT する行データのリスト (_to_dict() の出力)。

    Returns:
        実行可能な INSERT ... ON CONFLICT DO UPDATE ステートメント。

    Raises:
        NotImplementedError: postgresql / sqlite 以外のダイアレクトで呼ばれた場合。
    """
    dialect_name: str = session.bind.dialect.name  # type: ignore[union-attr]

    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    elif dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert
    else:
        raise NotImplementedError(
            f"upsert_many は postgresql / sqlite のみ対応です (dialect={dialect_name!r})"
        )

    stmt = insert(PriceDaily).values(rows_data)
    update_cols = {field: getattr(stmt.excluded, field) for field in _UPDATABLE_FIELDS}
    return stmt.on_conflict_do_update(
        index_elements=["ticker", "business_date"],
        set_=update_cols,
    )
