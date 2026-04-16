"""
PriceRepository のユニットテスト。

# テスト方針
  - SQLite in-memory DB を使用 (PostgreSQL / Docker 不要)
  - StaticPool により全セッションが同一 in-memory DB を共有する
  - InstrumentMaster を先投入し FK 依存を満たす (SQLite は FK 非強制だが現実に即す)
  - 各テストの後に price_daily を DELETE してテスト間の干渉を防ぐ
  - 先読み防止観点のテストを最優先で含む

# テスト用日付の根拠
  2025 年 1 月の月〜金曜を使用 (2025-01-06〜2025-01-10)。
  カレンダーの正確性は本テストでは問わない
  (calendar_service の単体テストで別途検証済み)。

# SQLite + BigInteger 制約の回避
  SQLAlchemy 2.0 + SQLite 3.35+ の組み合わせでは INSERT ... RETURNING id が生成される。
  SQLite は BigInteger (BIGINT) PRIMARY KEY を rowid alias として扱わないため、
  id を省略すると NOT NULL 制約違反が発生する。
  そのため _make_price() では itertools.count で明示的な id を付与する。
  本番 (PostgreSQL) では id=None のまま upsert_many() を呼び、SERIAL が採番する。
"""

import itertools
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.instrument import InstrumentMaster
from app.models.price import PriceDaily
from app.repositories.price_repository import PriceRepository

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_JAN_06 = date(2025, 1, 6)   # 月
_JAN_07 = date(2025, 1, 7)   # 火
_JAN_08 = date(2025, 1, 8)   # 水
_JAN_09 = date(2025, 1, 9)   # 木
_JAN_10 = date(2025, 1, 10)  # 金

_TICKER_US = "XLB"
_TICKER_JP = "1617.T"

_FETCHED_AT = datetime(2025, 1, 11, 0, 0, 0, tzinfo=timezone.utc)

# [テスト専用の回避策] SQLite + BigInteger PK の自動採番差異を吸収する id カウンター
#
# 背景:
#   SQLAlchemy 2.0 + SQLite 3.35+ では INSERT ... RETURNING id が生成される。
#   PriceDaily.id は BigInteger (BIGINT) だが、SQLite の rowid alias として
#   扱われるのは INTEGER PRIMARY KEY のみであるため自動採番が機能しない。
#
# 対応:
#   _make_price() で明示的な id を付与することで NOT NULL 制約違反を回避する。
#   本番 (PostgreSQL) では id=None のまま upsert_many() を呼び、SERIAL が採番する。
#
# この回避策はテストコード専用であり、ORM モデルや本番コードは変更しない。
_price_id_gen = itertools.count(1)


# ---------------------------------------------------------------------------
# フィクスチャ
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

    price_daily の FK 依存を満たすためのダミーデータ。
    SQLite は FK を強制しないが、実環境に即した構成にする。
    """
    _Session = sessionmaker(bind=engine)
    sess = _Session()
    sess.add_all([
        InstrumentMaster(
            ticker=_TICKER_US,
            market="US",
            instrument_name="Materials Select Sector SPDR Fund",
            sector_name="Materials",
        ),
        InstrumentMaster(
            ticker=_TICKER_JP,
            market="JP",
            instrument_name="NEXT FUNDS TOPIX-17素材",
            sector_name="素材",
        ),
    ])
    sess.commit()
    sess.close()


@pytest.fixture
def session(engine, _instruments) -> Session:
    """関数スコープのセッション。テスト終了後に price_daily を削除する。"""
    _Session = sessionmaker(bind=engine)
    sess = _Session()
    yield sess
    sess.query(PriceDaily).delete()
    sess.commit()
    sess.close()


@pytest.fixture
def repo(session: Session) -> PriceRepository:
    return PriceRepository(session)


def _make_price(
    ticker: str = _TICKER_US,
    business_date: date = _JAN_06,
    open: float | None = None,
    close: float = 100.0,
    market: str = "US",
    data_source: str = "test",
) -> PriceDaily:
    """テスト用 PriceDaily を生成するヘルパー。

    id は _price_id_gen から自動採番する。
    SQLite では BigInteger PRIMARY KEY が rowid alias にならないため、
    明示的な id が必要 (本番の PostgreSQL では id=None → SERIAL が採番)。

    open: open_price を指定する (デフォルト None)。
          get_oc_on_date テストでは明示的に指定する。
    """
    return PriceDaily(
        id=next(_price_id_gen),
        ticker=ticker,
        market=market,
        business_date=business_date,
        open_price=Decimal(str(open)) if open is not None else None,
        close_price=Decimal(str(close)),
        data_source=data_source,
        fetched_at=_FETCHED_AT,
    )


def _seed(session: Session, prices: list[PriceDaily]) -> None:
    """テスト用データを一括 add + flush する。"""
    session.add_all(prices)
    session.flush()


# ---------------------------------------------------------------------------
# 1. get_by_ticker_and_date
# ---------------------------------------------------------------------------

class TestGetByTickerAndDate:
    def test_found(self, session: Session, repo: PriceRepository) -> None:
        """存在するレコードを正しく返す。"""
        _seed(session, [_make_price(close=123.45)])
        result = repo.get_by_ticker_and_date(_TICKER_US, _JAN_06)
        assert result is not None
        assert result.ticker == _TICKER_US
        assert result.business_date == _JAN_06
        assert result.close_price == Decimal("123.45")

    def test_not_found_returns_none(self, repo: PriceRepository) -> None:
        """レコードが存在しない場合は None を返す (例外なし)。"""
        result = repo.get_by_ticker_and_date(_TICKER_US, _JAN_06)
        assert result is None

    def test_different_ticker_returns_none(self, session: Session, repo: PriceRepository) -> None:
        """同じ日付でも ticker が異なれば None を返す。"""
        _seed(session, [_make_price(ticker=_TICKER_JP, market="JP")])
        result = repo.get_by_ticker_and_date(_TICKER_US, _JAN_06)
        assert result is None

    def test_different_date_returns_none(self, session: Session, repo: PriceRepository) -> None:
        """同じ ticker でも日付が異なれば None を返す。"""
        _seed(session, [_make_price(business_date=_JAN_07)])
        result = repo.get_by_ticker_and_date(_TICKER_US, _JAN_06)
        assert result is None


# ---------------------------------------------------------------------------
# 2. list_by_ticker
# ---------------------------------------------------------------------------

class TestListByTicker:
    def _seed_week(self, session: Session) -> None:
        _seed(session, [
            _make_price(business_date=_JAN_06, close=100.0),
            _make_price(business_date=_JAN_07, close=101.0),
            _make_price(business_date=_JAN_08, close=102.0),
            _make_price(business_date=_JAN_09, close=103.0),
            _make_price(business_date=_JAN_10, close=104.0),
        ])

    def test_returns_all_when_no_filter(self, session: Session, repo: PriceRepository) -> None:
        """フィルタなしは全件を返す。"""
        self._seed_week(session)
        result = repo.list_by_ticker(_TICKER_US)
        assert len(result) == 5

    def test_ascending_order(self, session: Session, repo: PriceRepository) -> None:
        """返り値は business_date 昇順。"""
        self._seed_week(session)
        result = repo.list_by_ticker(_TICKER_US)
        dates = [r.business_date for r in result]
        assert dates == sorted(dates)

    def test_with_start(self, session: Session, repo: PriceRepository) -> None:
        """start 指定: start 以降のみ返す (start inclusive)。"""
        self._seed_week(session)
        result = repo.list_by_ticker(_TICKER_US, start=_JAN_08)
        assert len(result) == 3
        assert result[0].business_date == _JAN_08

    def test_with_end(self, session: Session, repo: PriceRepository) -> None:
        """end 指定: end 以前のみ返す (end inclusive)。"""
        self._seed_week(session)
        result = repo.list_by_ticker(_TICKER_US, end=_JAN_08)
        assert len(result) == 3
        assert result[-1].business_date == _JAN_08

    def test_with_start_and_end(self, session: Session, repo: PriceRepository) -> None:
        """start + end 指定: 閉区間。"""
        self._seed_week(session)
        result = repo.list_by_ticker(_TICKER_US, start=_JAN_07, end=_JAN_09)
        assert len(result) == 3
        assert result[0].business_date == _JAN_07
        assert result[-1].business_date == _JAN_09

    def test_empty_for_unknown_ticker(self, session: Session, repo: PriceRepository) -> None:
        """存在しない ticker は空リスト (例外なし)。"""
        self._seed_week(session)
        result = repo.list_by_ticker("UNKNOWN")
        assert result == []


# ---------------------------------------------------------------------------
# 3. get_prices_up_to  ★先読み防止の核心
# ---------------------------------------------------------------------------

class TestGetPricesUpTo:
    def _seed_week(self, session: Session) -> None:
        _seed(session, [
            _make_price(business_date=_JAN_06, close=100.0),
            _make_price(business_date=_JAN_07, close=101.0),
            _make_price(business_date=_JAN_08, close=102.0),
            _make_price(business_date=_JAN_09, close=103.0),
            _make_price(business_date=_JAN_10, close=104.0),
        ])

    def test_returns_only_up_to_as_of_date(self, session: Session, repo: PriceRepository) -> None:
        """★先読み防止: as_of_date より後のデータを絶対に返さない。

        DB に Jan 6〜10 が存在する状態で as_of_date=Jan 8 を指定した場合、
        Jan 9, Jan 10 は返ってはならない。
        """
        self._seed_week(session)
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_08)

        returned_dates = [r.business_date for r in result]
        assert _JAN_09 not in returned_dates, "Jan 9 は as_of_date より後: 返してはならない"
        assert _JAN_10 not in returned_dates, "Jan 10 は as_of_date より後: 返してはならない"
        assert len(result) == 3

    def test_as_of_date_is_inclusive(self, session: Session, repo: PriceRepository) -> None:
        """as_of_date 当日は返り値に含まれる (inclusive)。"""
        self._seed_week(session)
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_08)
        dates = [r.business_date for r in result]
        assert _JAN_08 in dates

    def test_all_returned_dates_lte_as_of_date(self, session: Session, repo: PriceRepository) -> None:
        """★構造的先読み防止: 全返り値の business_date <= as_of_date が成立する。

        signal 層は as_of_date = us_signal_date として呼ぶ。
        このアサーションが成立する限り jp_execution_date 以降の価格は
        signal 計算に混入しない。
        """
        self._seed_week(session)
        as_of = _JAN_08
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=as_of)

        violations = [r for r in result if r.business_date > as_of]
        assert violations == [], (
            f"先読みバイアス: as_of_date={as_of} より後のデータが含まれる: "
            f"{[r.business_date for r in violations]}"
        )

    def test_result_is_ascending(self, session: Session, repo: PriceRepository) -> None:
        """返り値は business_date 昇順。"""
        self._seed_week(session)
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_10)
        dates = [r.business_date for r in result]
        assert dates == sorted(dates)

    def test_with_limit_returns_most_recent(self, session: Session, repo: PriceRepository) -> None:
        """limit=2 は as_of_date 以前の最新 2 件を返す。

        DB: Jan 6, 7, 8 (as_of_date=Jan 8) → 最新 2 件 = Jan 7, Jan 8
        """
        self._seed_week(session)
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_08, limit=2)
        assert len(result) == 2
        assert result[0].business_date == _JAN_07
        assert result[1].business_date == _JAN_08

    def test_with_limit_result_is_ascending(self, session: Session, repo: PriceRepository) -> None:
        """limit 指定時も返り値は昇順。"""
        self._seed_week(session)
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_10, limit=3)
        dates = [r.business_date for r in result]
        assert dates == sorted(dates)

    def test_limit_does_not_bypass_as_of_date(self, session: Session, repo: PriceRepository) -> None:
        """★先読み防止 + limit: limit があっても as_of_date は無視されない。

        limit=10 を指定しても as_of_date=Jan 7 より後は返らない。
        """
        self._seed_week(session)
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_07, limit=10)
        assert all(r.business_date <= _JAN_07 for r in result), (
            "limit=10 でも as_of_date=Jan 7 より後のデータが混入してはならない"
        )

    def test_no_data_returns_empty_list(self, repo: PriceRepository) -> None:
        """対象データがない場合は空リスト (例外なし)。"""
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_10)
        assert result == []

    def test_future_only_data_returns_empty(self, session: Session, repo: PriceRepository) -> None:
        """as_of_date より後のデータしかない場合は空リストを返す。"""
        _seed(session, [_make_price(business_date=_JAN_09)])
        result = repo.get_prices_up_to(_TICKER_US, as_of_date=_JAN_07)
        assert result == []


# ---------------------------------------------------------------------------
# 4. get_prices_between
# ---------------------------------------------------------------------------

class TestGetPricesBetween:
    def _seed_week(self, session: Session) -> None:
        _seed(session, [
            _make_price(business_date=_JAN_06, close=100.0),
            _make_price(business_date=_JAN_07, close=101.0),
            _make_price(business_date=_JAN_08, close=102.0),
            _make_price(business_date=_JAN_09, close=103.0),
            _make_price(business_date=_JAN_10, close=104.0),
        ])

    def test_inclusive_range(self, session: Session, repo: PriceRepository) -> None:
        """start_date, end_date の両端が含まれる (inclusive)。"""
        self._seed_week(session)
        result = repo.get_prices_between(_TICKER_US, _JAN_07, _JAN_09)
        dates = [r.business_date for r in result]
        assert dates == [_JAN_07, _JAN_08, _JAN_09]

    def test_ascending_order(self, session: Session, repo: PriceRepository) -> None:
        """返り値は business_date 昇順。"""
        self._seed_week(session)
        result = repo.get_prices_between(_TICKER_US, _JAN_06, _JAN_10)
        dates = [r.business_date for r in result]
        assert dates == sorted(dates)

    def test_no_data_in_range_returns_empty(self, repo: PriceRepository) -> None:
        """範囲内にデータがない場合は空リスト。"""
        result = repo.get_prices_between(_TICKER_US, _JAN_06, _JAN_10)
        assert result == []

    def test_start_equals_end(self, session: Session, repo: PriceRepository) -> None:
        """start == end は単一日付のみ返す。"""
        self._seed_week(session)
        result = repo.get_prices_between(_TICKER_US, _JAN_08, _JAN_08)
        assert len(result) == 1
        assert result[0].business_date == _JAN_08


# ---------------------------------------------------------------------------
# 5. upsert_many
# ---------------------------------------------------------------------------

class TestUpsertMany:
    def test_insert_new_records(self, session: Session, repo: PriceRepository) -> None:
        """存在しないレコードを INSERT する。"""
        rows = [
            _make_price(business_date=_JAN_06, close=100.0),
            _make_price(business_date=_JAN_07, close=101.0),
        ]
        count = repo.upsert_many(rows)
        assert count == 2

        r06 = repo.get_by_ticker_and_date(_TICKER_US, _JAN_06)
        r07 = repo.get_by_ticker_and_date(_TICKER_US, _JAN_07)
        assert r06 is not None and r06.close_price == Decimal("100.0")
        assert r07 is not None and r07.close_price == Decimal("101.0")

    def test_update_existing_records(self, session: Session, repo: PriceRepository) -> None:
        """既存レコードを UPDATE する。"""
        _seed(session, [_make_price(business_date=_JAN_06, close=100.0)])

        updated = [_make_price(business_date=_JAN_06, close=999.0)]
        repo.upsert_many(updated)

        result = repo.get_by_ticker_and_date(_TICKER_US, _JAN_06)
        assert result is not None
        assert result.close_price == Decimal("999.0"), "close_price が更新されていない"

    def test_mixed_insert_and_update(self, session: Session, repo: PriceRepository) -> None:
        """既存レコードは UPDATE、新規は INSERT の混在パターン。"""
        _seed(session, [_make_price(business_date=_JAN_06, close=100.0)])

        rows = [
            _make_price(business_date=_JAN_06, close=200.0),  # UPDATE
            _make_price(business_date=_JAN_07, close=101.0),  # INSERT
        ]
        count = repo.upsert_many(rows)
        assert count == 2

        r06 = repo.get_by_ticker_and_date(_TICKER_US, _JAN_06)
        r07 = repo.get_by_ticker_and_date(_TICKER_US, _JAN_07)
        assert r06 is not None and r06.close_price == Decimal("200.0")
        assert r07 is not None and r07.close_price == Decimal("101.0")

    def test_empty_input_returns_zero(self, repo: PriceRepository) -> None:
        """空リストは 0 を返す (例外なし)。"""
        assert repo.upsert_many([]) == 0

    def test_returns_total_row_count(self, repo: PriceRepository) -> None:
        """返り値は insert + update の合計行数。"""
        rows = [
            _make_price(business_date=_JAN_06),
            _make_price(business_date=_JAN_07),
            _make_price(business_date=_JAN_08),
        ]
        count = repo.upsert_many(rows)
        assert count == 3

    def test_upsert_preserves_unique_constraint(self, session: Session, repo: PriceRepository) -> None:
        """同じ (ticker, date) を異なる id で 2 回 upsert しても重複行が生じない。

        1 回目: INSERT (id=N)
        2 回目: 同じ (ticker, business_date) で別 id → ON CONFLICT DO UPDATE → 既存行を更新
        結果: テーブルには 1 行のみ
        """
        rows_first = [_make_price(business_date=_JAN_06, close=100.0)]
        repo.upsert_many(rows_first)

        # 別 id, 同じ (ticker, business_date) を再投入 → ON CONFLICT DO UPDATE
        rows_second = [_make_price(business_date=_JAN_06, close=100.0)]
        repo.upsert_many(rows_second)

        all_rows = repo.list_by_ticker(_TICKER_US)
        assert len(all_rows) == 1, "ON CONFLICT DO UPDATE のため重複行が生じてはならない"


# ---------------------------------------------------------------------------
# 6. get_oc_on_date  ★先読み防止 + バックテスト用
# ---------------------------------------------------------------------------


class TestGetOcOnDate:
    """get_oc_on_date のテスト群。

    # テスト観点
      先読み防止: WHERE business_date = :date のみで取得すること。
                 指定日以外のデータは絶対に返さない。
      欠損ルール: open または close が None の ticker は None を返す。
      行なし:     対応行がない ticker も None を返す。
      正常系:     (open_price, close_price) のタプルを返す。
    """

    # ------------------------------------------------------------------
    # 正常系
    # ------------------------------------------------------------------

    def test_returns_open_close_tuple(self, session: Session, repo: PriceRepository) -> None:
        """open/close が揃っている場合は (open, close) タプルを返すこと。"""
        _seed(session, [_make_price(open=95.0, close=100.0)])
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US])
        assert result[_TICKER_US] == (Decimal("95.0"), Decimal("100.0"))

    def test_multiple_tickers(self, session: Session, repo: PriceRepository) -> None:
        """複数 ticker を一度に取得できること。"""
        _seed(session, [
            _make_price(ticker=_TICKER_US, open=95.0, close=100.0, market="US"),
            _make_price(ticker=_TICKER_JP, open=200.0, close=205.0, market="JP"),
        ])
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US, _TICKER_JP])
        assert _TICKER_US in result
        assert _TICKER_JP in result
        assert result[_TICKER_US] == (Decimal("95.0"), Decimal("100.0"))
        assert result[_TICKER_JP] == (Decimal("200.0"), Decimal("205.0"))

    def test_all_input_tickers_appear_in_result(
        self, session: Session, repo: PriceRepository
    ) -> None:
        """入力 tickers の全要素が結果のキーに含まれること (欠損含む)。"""
        _seed(session, [_make_price(ticker=_TICKER_US, open=95.0, close=100.0)])
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US, _TICKER_JP])
        assert _TICKER_US in result
        assert _TICKER_JP in result  # データなし → None

    # ------------------------------------------------------------------
    # ★先読み防止: 指定日以外を返さない
    # ------------------------------------------------------------------

    def test_only_returns_exact_date(self, session: Session, repo: PriceRepository) -> None:
        """★先読み防止: 指定日以外のデータを絶対に返さない。

        DB に Jan 6 と Jan 7 が存在する状態で Jan 6 を指定した場合、
        Jan 7 のデータが返ってはならない。
        """
        _seed(session, [
            _make_price(business_date=_JAN_06, open=100.0, close=105.0),
            _make_price(business_date=_JAN_07, open=110.0, close=115.0),
        ])
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US])
        # Jan 7 の価格 (110, 115) ではなく Jan 6 の (100, 105) が返ること
        assert result[_TICKER_US] == (Decimal("100.0"), Decimal("105.0"))

    def test_returns_none_when_no_data_on_date(
        self, session: Session, repo: PriceRepository
    ) -> None:
        """★先読み防止: 隣接日にデータがあっても指定日のデータがなければ None。

        DB に Jan 7 のみ存在し、Jan 6 を指定した場合は None が返ること。
        Jan 7 のデータをフォールバックとして返してはならない。
        """
        _seed(session, [_make_price(business_date=_JAN_07, open=110.0, close=115.0)])
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US])
        assert result[_TICKER_US] is None, (
            "先読みバイアス: 指定日以外のデータが返ってはならない"
        )

    # ------------------------------------------------------------------
    # 欠損ルール
    # ------------------------------------------------------------------

    def test_returns_none_when_open_price_is_null(
        self, session: Session, repo: PriceRepository
    ) -> None:
        """open_price が None の ticker は None を返すこと。"""
        _seed(session, [_make_price(open=None, close=100.0)])  # open=None のまま
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US])
        assert result[_TICKER_US] is None

    def test_returns_none_when_close_price_is_null(
        self, session: Session, repo: PriceRepository
    ) -> None:
        """close_price が None の ticker は None を返すこと。

        PriceDaily.close_price は nullable なため、
        データが存在しても close=None の場合は計算不能。
        """
        # close_price=None を作るため直接 ORM オブジェクトを構築
        row = PriceDaily(
            id=next(_price_id_gen),
            ticker=_TICKER_US,
            market="US",
            business_date=_JAN_06,
            open_price=Decimal("95.0"),
            close_price=None,  # ← close が欠損
            data_source="test",
            fetched_at=_FETCHED_AT,
        )
        _seed(session, [row])
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US])
        assert result[_TICKER_US] is None

    def test_returns_none_for_unknown_ticker(self, repo: PriceRepository) -> None:
        """DB に存在しない ticker は None を返すこと (例外なし)。"""
        result = repo.get_oc_on_date(_JAN_06, ["UNKNOWN"])
        assert result["UNKNOWN"] is None

    # ------------------------------------------------------------------
    # 境界値 / その他
    # ------------------------------------------------------------------

    def test_empty_tickers_returns_empty_dict(self, repo: PriceRepository) -> None:
        """tickers が空の場合は空 dict を返すこと。"""
        result = repo.get_oc_on_date(_JAN_06, [])
        assert result == {}

    def test_partial_missing_tickers(
        self, session: Session, repo: PriceRepository
    ) -> None:
        """一部 ticker にデータがない混在ケース。

        XLB はデータあり → (open, close)
        1617.T はデータなし → None
        """
        _seed(session, [_make_price(ticker=_TICKER_US, open=95.0, close=100.0)])
        result = repo.get_oc_on_date(_JAN_06, [_TICKER_US, _TICKER_JP])
        assert result[_TICKER_US] is not None
        assert result[_TICKER_JP] is None
