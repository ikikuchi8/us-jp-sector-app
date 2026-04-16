"""
instrument_master 初期データ投入スクリプト。

冪等性:
  - 実行前に既存 ticker を取得し、未投入分だけを INSERT する。
  - INSERT には ON CONFLICT DO NOTHING を使用し、競合時も安全に処理する。
  - 再実行しても既存レコードは変更されない。

実行方法:
  cd backend
  python -m app.scripts.seed_instruments

前提:
  - DB (docker-compose の db サービス) が起動していること。
  - .env が配置されていること (cp .env.example .env)。
"""

import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# sys.path 設定
# `python app/scripts/seed_instruments.py` で直接実行した場合の保険。
# `python -m app.scripts.seed_instruments` では不要だが明示的に保持する。
# ---------------------------------------------------------------------------
_backend_dir = Path(__file__).parent.parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import SessionLocal
from app.models.instrument import InstrumentMaster
from app.seed_data.instruments import ALL_INSTRUMENTS, JP_INSTRUMENTS, US_INSTRUMENTS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Seed 関数
# ---------------------------------------------------------------------------
def seed_instruments() -> None:
    """instrument_master に初期データを投入する。

    冪等に動作する:
      - 既存 ticker はスキップ (変更しない)
      - 未投入 ticker のみ INSERT

    Raises:
        Exception: DB 操作中にエラーが発生した場合。自動ロールバック後に再送出する。
    """
    with SessionLocal() as session:
        # ----------------------------------------------------------------
        # Step 1: 既存 ticker を取得して差分を計算
        # ----------------------------------------------------------------
        existing_tickers: set[str] = {
            row[0]
            for row in session.execute(select(InstrumentMaster.ticker)).all()
        }

        new_rows = [
            {
                "ticker": row["ticker"],
                "market": row["market"],
                "instrument_name": row["instrument_name"],
                "sector_name": row["sector_name"],
                "is_active": True,
            }
            for row in ALL_INSTRUMENTS
            if row["ticker"] not in existing_tickers
        ]

        total = len(ALL_INSTRUMENTS)
        skip_count = len(existing_tickers & {r["ticker"] for r in ALL_INSTRUMENTS})
        insert_count = len(new_rows)

        logger.info(
            "対象: 合計 %d 件 / 投入予定 %d 件 / スキップ %d 件 (既存)",
            total,
            insert_count,
            skip_count,
        )

        if not new_rows:
            logger.info("全レコードが既に投入済みです。処理を終了します。")
            return

        # ----------------------------------------------------------------
        # Step 2: INSERT ... ON CONFLICT DO NOTHING
        # Python 側フィルタ後の残差も DB レベルで安全に処理する。
        # ----------------------------------------------------------------
        stmt = (
            pg_insert(InstrumentMaster)
            .values(new_rows)
            .on_conflict_do_nothing(index_elements=["ticker"])
        )
        result = session.execute(stmt)
        session.commit()

        logger.info(
            "投入完了: %d 件を INSERT しました。 (DB rowcount: %d)",
            insert_count,
            result.rowcount,
        )

        # ----------------------------------------------------------------
        # Step 3: 投入結果のサマリーを出力
        # ----------------------------------------------------------------
        _print_summary(session)


def _print_summary(session) -> None:
    """投入後の件数を market 別にログ出力する。"""
    us_count = session.execute(
        select(InstrumentMaster).where(InstrumentMaster.market == "US")
    ).scalars().all()
    jp_count = session.execute(
        select(InstrumentMaster).where(InstrumentMaster.market == "JP")
    ).scalars().all()

    logger.info(
        "現在の instrument_master: US %d 件 / JP %d 件 / 合計 %d 件",
        len(us_count),
        len(jp_count),
        len(us_count) + len(jp_count),
    )


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("instrument_master seed を開始します")
    logger.info("US %d 件 + JP %d 件 = 合計 %d 件", len(US_INSTRUMENTS), len(JP_INSTRUMENTS), len(ALL_INSTRUMENTS))
    logger.info("=" * 50)

    try:
        seed_instruments()
        logger.info("seed が正常に完了しました。")
    except Exception as e:
        logger.error("seed 中にエラーが発生しました: %s", e)
        sys.exit(1)
