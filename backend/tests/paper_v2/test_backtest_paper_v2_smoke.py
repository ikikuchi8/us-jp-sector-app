"""
tests/paper_v2/test_backtest_paper_v2_smoke.py — paper_v2 バックテスト DB-backed smoke test。

# 目的
  signal_type="paper_v2" で BacktestService が動作し、
  signal_daily から行を読んで P&L を計算することを end-to-end で証明する。

# テスト方針
  - SQLite in-memory DB を使用 (test_backtest_repository.py / test_price_repository.py と同パターン)
  - 実 DB (SQLite in-memory) を使用し mock なし
  - InstrumentMaster → PriceDaily → SignalDaily を最小限投入
  - CalendarService は実インスタンスを使用 (2024-01-04 / 2024-01-05 は実 JPX 営業日)
  - long 1 件のみ (short 欠損) → daily_return = long_return / 2 で動作確認

# SQLite BigInteger PK の回避
  BacktestResultDaily.id は BigInteger PK。
  SQLite は BigInteger (BIGINT) PRIMARY KEY を rowid alias として扱わないため、
  BacktestRepository.save_daily_results が id=None で add_all すると NOT NULL 違反になる。
  テストコードで BacktestRepository.save_daily_results を薄くラップして
  flush 前に明示的な id を付与する (本番コード・ORM モデルは変更しない)。
  この回避策はテストコード専用であり app/ 配下のコードは一切変更しない。
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.backtest import BacktestResultDaily, BacktestRun
from app.models.instrument import InstrumentMaster
from app.models.price import PriceDaily
from app.models.signal import SignalDaily, SuggestedSide
from app.repositories.backtest_repository import BacktestRepository
from app.services.backtest_service import BacktestService, CostParams
from app.services.calendar_service import CalendarService
from app.services.paper_v2.constants import SIGNAL_TYPE_PAPER_V2

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_JP_TICKER = "1617.T"

# 2024-01-04 (木) と 2024-01-05 (金) はいずれも JPX 営業日
_JP_EXEC_DATE = date(2024, 1, 5)   # OC リターン計算対象日
_US_SIGNAL_DATE = date(2024, 1, 4)  # US シグナル生成日 (前営業日)

_FETCHED_AT = datetime(2024, 1, 6, 0, 0, 0, tzinfo=timezone.utc)

# SQLite + BigInteger PK 回避用 id カウンター
_price_id_gen = itertools.count(1)
_signal_id_gen = itertools.count(1)
_result_id_gen = itertools.count(1)


# ---------------------------------------------------------------------------
# SQLite BigInteger PK 回避ヘルパー: BacktestRepository をラップ
# ---------------------------------------------------------------------------


class _SQLiteBacktestRepository(BacktestRepository):
    """SQLite テスト専用: save_daily_results で明示的な id を付与する。

    BacktestResultDaily.id は BigInteger PK。SQLite は BIGINT PRIMARY KEY を
    rowid alias として扱わないため id=None では NOT NULL 違反になる。
    本番 (PostgreSQL) では SERIAL が自動採番するため問題ない。
    この subclass はテストコードのみで使用し、app/ 配下のコードは変更しない。
    """

    def save_daily_results(self, rows: Sequence[BacktestResultDaily]) -> int:
        for row in rows:
            if row.id is None:
                row.id = next(_result_id_gen)
        return super().save_daily_results(rows)


# ---------------------------------------------------------------------------
# フィクスチャ: エンジン / DB セットアップ
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def engine():
    """モジュール全体で共有する SQLite in-memory エンジン。"""
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)


@pytest.fixture(scope="module")
def _instruments(engine):
    """テスト用 InstrumentMaster を 1 回だけ投入する。

    PriceDaily / SignalDaily の FK 依存を満たすためのダミーデータ。
    """
    _Session = sessionmaker(bind=engine)
    sess = _Session()
    sess.add(
        InstrumentMaster(
            ticker=_JP_TICKER,
            market="JP",
            instrument_name="NEXT FUNDS TOPIX-17素材",
            sector_name="素材",
        )
    )
    sess.commit()
    sess.close()


@pytest.fixture
def session(engine, _instruments) -> Session:
    """関数スコープのセッション。テスト終了後に各テーブルを全件削除する。"""
    _Session = sessionmaker(bind=engine)
    sess = _Session()
    yield sess
    # FK 依存順: 子 → 親
    sess.query(BacktestResultDaily).delete()
    sess.query(BacktestRun).delete()
    sess.query(SignalDaily).delete()
    sess.query(PriceDaily).delete()
    sess.commit()
    sess.close()


@pytest.fixture(scope="module")
def calendar_service():
    """実 CalendarService (pandas_market_calendars 使用)。"""
    return CalendarService()


# ---------------------------------------------------------------------------
# smoke test
# ---------------------------------------------------------------------------


def test_backtest_runs_with_paper_v2_signal_type(
    session: Session,
    calendar_service: CalendarService,
) -> None:
    """paper_v2 signal_type で BacktestService が end-to-end で動作すること。

    セットアップ:
      - PriceDaily: 1617.T の 2 営業日分 (2024-01-04 / 2024-01-05)
      - SignalDaily: signal_type="paper_v2", target_ticker="1617.T",
                    jp_execution_date=2024-01-05, suggested_side="long" の 1 件

    期待:
      - BacktestService.run() が正常終了する
      - daily_results に 2024-01-05 の結果が含まれる
      - long_return が計算される (1617.T open=100 → close=102: +2%)
      - daily_return = long_return / 2 (short 欠損のため 50:50 ドル中立: (0.02 + 0) / 2 = 0.01)
    """
    # ── 1. Price データ投入 ──────────────────────────────────────────────
    # 2024-01-04: 前日 (us_signal_date 相当; CalendarService のループ軸には含めない)
    # 2024-01-05: jp_execution_date (OC リターン計算対象)
    price_jan04 = PriceDaily(
        id=next(_price_id_gen),
        ticker=_JP_TICKER,
        market="JP",
        business_date=date(2024, 1, 4),
        open_price=Decimal("98"),
        close_price=Decimal("99"),
        data_source="test",
        fetched_at=_FETCHED_AT,
    )
    price_jan05 = PriceDaily(
        id=next(_price_id_gen),
        ticker=_JP_TICKER,
        market="JP",
        business_date=date(2024, 1, 5),
        open_price=Decimal("100"),
        close_price=Decimal("102"),  # +2% OC リターン
        data_source="test",
        fetched_at=_FETCHED_AT,
    )
    session.add_all([price_jan04, price_jan05])
    session.flush()

    # ── 2. SignalDaily データ投入 ────────────────────────────────────────
    signal = SignalDaily(
        id=next(_signal_id_gen),
        signal_type=SIGNAL_TYPE_PAPER_V2,
        target_ticker=_JP_TICKER,
        us_signal_date=_US_SIGNAL_DATE,
        jp_execution_date=_JP_EXEC_DATE,
        suggested_side=SuggestedSide.LONG.value,
        signal_rank=1,
        signal_score=Decimal("0.5"),
    )
    session.add(signal)
    session.flush()

    # ── 3. BacktestService を実行 ────────────────────────────────────────
    # SQLite BigInteger PK 回避のため、_SQLiteBacktestRepository を注入する。
    service = BacktestService(
        session=session,
        calendar_service=calendar_service,
        backtest_repository=_SQLiteBacktestRepository(session),
    )
    result = service.run(
        start=_JP_EXEC_DATE,
        end=_JP_EXEC_DATE,
        signal_type=SIGNAL_TYPE_PAPER_V2,
        cost_params=CostParams(),
    )

    # ── 4. アサーション ──────────────────────────────────────────────────
    # run が正常完了すること (例外なし・daily_results が存在すること)
    assert len(result.daily_results) >= 1, "daily_results が空: run が正常完了していない"

    # 2024-01-05 の結果が存在すること
    dr = result.daily_results[0]
    assert dr.jp_execution_date == _JP_EXEC_DATE, (
        f"jp_execution_date が一致しない: {dr.jp_execution_date}"
    )

    # long_return が計算されていること (1617.T: open=100, close=102 → +2%)
    assert dr.long_return is not None, "long_return が None: long ブックが計算されていない"

    # daily_return が計算されていること
    # short 欠損のため daily_return = (long_return + 0) / 2 = long_return / 2
    assert dr.daily_return is not None, "daily_return が None: P&L 計算に失敗している"

    # trading_days が 1 であること (1 日間のバックテスト)
    assert result.trading_days == 1, f"trading_days が 1 でない: {result.trading_days}"
