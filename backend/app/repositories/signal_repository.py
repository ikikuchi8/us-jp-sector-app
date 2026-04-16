"""
SignalRepository: signal_daily テーブルへの CRUD 操作。

# 責務
  - signal_daily テーブルの読み書きに特化する
  - commit は呼び出し側 (SignalService) が行う
  - upsert の衝突キー: (signal_type, target_ticker, jp_execution_date)
    ※ US 休場時に複数 JP 日が同一 us_signal_date を共有するケースに対応するため、
      jp_execution_date を一意キー軸としている

# ダイアレクト対応
  PostgreSQL (本番) / SQLite (テスト) の両方をサポートする。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.signal import SignalDaily

# upsert_many で INSERT する全カラム (PK の id・server_default の created_at は除外)
_INSERT_FIELDS: tuple[str, ...] = (
    "signal_type",
    "target_ticker",
    "us_signal_date",
    "jp_execution_date",
    "signal_score",
    "signal_rank",
    "suggested_side",
    "input_metadata_json",
)

# ON CONFLICT 時に UPDATE するフィールド
# 衝突キー (signal_type, target_ticker, jp_execution_date) は除外する
_UPDATABLE_FIELDS: tuple[str, ...] = (
    "us_signal_date",
    "signal_score",
    "signal_rank",
    "suggested_side",
    "input_metadata_json",
)


class SignalRepository:
    """signal_daily テーブルへのアクセスを担う Repository。

    Args:
        session: SQLAlchemy Session。commit / rollback は呼び出し側が行う。
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # 読み取り
    # ------------------------------------------------------------------

    def list_by_jp_execution_date(
        self,
        signal_type: str,
        jp_execution_date: date,
    ) -> list[SignalDaily]:
        """指定した jp_execution_date の全 JP 業種シグナルを signal_rank 昇順で返す。

        signal_rank が None の行は末尾に並ぶ (nullslast)。

        Args:
            signal_type:       シグナル種別 (例: "simple_v1")
            jp_execution_date: 取得対象の JP 執行日

        Returns:
            SignalDaily のリスト。該当なしは空リスト。
        """
        stmt = (
            select(SignalDaily)
            .where(SignalDaily.signal_type == signal_type)
            .where(SignalDaily.jp_execution_date == jp_execution_date)
            .order_by(SignalDaily.signal_rank.asc().nullslast())
        )
        return list(self._session.scalars(stmt).all())

    # ------------------------------------------------------------------
    # 書き込み
    # ------------------------------------------------------------------

    def upsert_many(self, rows: Sequence[SignalDaily]) -> int:
        """複数行を INSERT ... ON CONFLICT DO UPDATE で一括 upsert する。

        # 衝突キー
          (signal_type, target_ticker, jp_execution_date)
          = uq_signal_daily_type_ticker_jpdate

        # 冪等性
          同一 jp_execution_date で再実行すると既存行を上書きする。
          US 休場で複数の JP 日が同一 us_signal_date を共有する場合も、
          jp_execution_date が異なるため衝突せず両方保存される。

        Args:
            rows: 保存対象の SignalDaily リスト。

        Returns:
            入力 rows の件数。空リスト入力時は 0。

        Note:
            commit は呼び出し側が行う。
            execute 後に session.expire_all() を呼ぶため ORM キャッシュが無効化される。
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

def _to_dict(row: SignalDaily) -> dict[str, Any]:
    """SignalDaily → INSERT 用 dict に変換する。

    id は None の場合のみ省略する (PostgreSQL SERIAL に委ねる)。
    SQLite テストでは明示的な id が必要な場合のみ含める。
    """
    d: dict[str, Any] = {field: getattr(row, field) for field in _INSERT_FIELDS}
    if row.id is not None:
        d["id"] = row.id
    return d


def _build_upsert_stmt(session: Session, rows_data: list[dict[str, Any]]):
    """ダイアレクトに応じた INSERT ... ON CONFLICT DO UPDATE 文を生成する。

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
            f"upsert_many は postgresql / sqlite のみ対応 (dialect={dialect_name!r})"
        )

    stmt = insert(SignalDaily).values(rows_data)
    update_cols = {field: getattr(stmt.excluded, field) for field in _UPDATABLE_FIELDS}
    return stmt.on_conflict_do_update(
        index_elements=["signal_type", "target_ticker", "jp_execution_date"],
        set_=update_cols,
    )
