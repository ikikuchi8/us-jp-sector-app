"""
tests/test_backtest_repository.py — BacktestRepository のユニットテスト。

# テスト方針
  - SQLite in-memory DB を使用 (PostgreSQL / Docker 不要)
  - StaticPool により全セッションが同一 in-memory DB を共有する
  - Base.metadata.create_all() で最新 ORM モデルからテーブルを生成する
    (= alembic migrate 後のスキーマと一致させる)
  - 各テスト後に backtest_result_daily / backtest_run を DELETE して
    テスト間の干渉を防ぐ

# SQLite + BigInteger PK の制約
  BacktestResultDaily.id は BigInteger。SQLite は BIGINT PRIMARY KEY を
  rowid alias として扱わないため、session.add() + flush() で
  id=None のオブジェクトを渡すと NOT NULL 制約違反が発生する。
  _result_id_gen で明示的な id を付与して回避する。
  BacktestRun.id は Integer のため autoincrement が正常に機能する。

# テスト対象
  - BacktestRepository.create_run: run 作成・id 採番・status 初期値
  - BacktestRepository.finalize_run: 統計書き込み・status="done"・finished_at
  - BacktestRepository.save_daily_results: 一括保存・件数返却・順序
  - BacktestRepository.get_run: 存在 / 不在
  - BacktestRepository.list_daily_results: 昇順・run_id 分離
"""

from __future__ import annotations

import itertools
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.backtest import BacktestResultDaily, BacktestRun, BacktestStatus
from app.repositories.backtest_repository import BacktestRepository

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_JAN_06 = date(2025, 1, 6)
_JAN_07 = date(2025, 1, 7)
_JAN_08 = date(2025, 1, 8)
_JAN_09 = date(2025, 1, 9)
_JAN_10 = date(2025, 1, 10)

_SIGNAL_TYPE = "simple_v1"

# [テスト専用] BacktestResultDaily (BigInteger PK) の明示的 id カウンター
_result_id_gen = itertools.count(1)


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


@pytest.fixture
def session(engine) -> Session:
    """関数スコープのセッション。テスト終了後にテーブルを全件削除する。"""
    _Session = sessionmaker(bind=engine)
    sess = _Session()
    yield sess
    # FK 依存順: 子 → 親
    sess.query(BacktestResultDaily).delete()
    sess.query(BacktestRun).delete()
    sess.commit()
    sess.close()


@pytest.fixture
def repo(session: Session) -> BacktestRepository:
    return BacktestRepository(session)


def _make_run(
    repo: BacktestRepository,
    signal_type: str = _SIGNAL_TYPE,
    start: date = _JAN_06,
    end: date = _JAN_10,
    commission: float = 0.0,
    slippage: float = 0.0,
    run_name: str | None = None,
) -> BacktestRun:
    """テスト用 BacktestRun を作成するヘルパー。"""
    return repo.create_run(
        signal_type=signal_type,
        start_date=start,
        end_date=end,
        commission_rate=commission,
        slippage_rate=slippage,
        run_name=run_name,
    )


def _make_daily(
    run_id: int,
    jp_date: date,
    daily_return: float | None = 0.01,
    cumulative_return: float | None = 0.01,
    long_return: float | None = None,
    short_return: float | None = None,
    long_count: int | None = 3,
    short_count: int | None = 3,
) -> BacktestResultDaily:
    """テスト用 BacktestResultDaily を生成するヘルパー。

    SQLite BigInteger PK 回避のため明示的な id を付与する。
    """

    def _d(v: float | None) -> Decimal | None:
        return Decimal(str(v)) if v is not None else None

    return BacktestResultDaily(
        id=next(_result_id_gen),
        backtest_run_id=run_id,
        jp_execution_date=jp_date,
        daily_return=_d(daily_return),
        cumulative_return=_d(cumulative_return),
        long_return=_d(long_return),
        short_return=_d(short_return),
        long_count=long_count,
        short_count=short_count,
    )


# ---------------------------------------------------------------------------
# 1. create_run
# ---------------------------------------------------------------------------


class TestCreateRun:
    def test_returns_backtest_run_instance(self, repo: BacktestRepository) -> None:
        """create_run は BacktestRun を返すこと。"""
        run = _make_run(repo)
        assert isinstance(run, BacktestRun)

    def test_run_id_is_assigned(self, repo: BacktestRepository) -> None:
        """flush 後に id が採番されること。"""
        run = _make_run(repo)
        assert run.id is not None
        assert run.id > 0

    def test_status_is_running(self, repo: BacktestRepository) -> None:
        """初期 status は 'running' であること。"""
        run = _make_run(repo)
        assert run.status == BacktestStatus.RUNNING.value

    def test_signal_type_is_stored(self, repo: BacktestRepository) -> None:
        """signal_type が正しく保存されること。"""
        run = _make_run(repo, signal_type="pca_v1")
        assert run.signal_type == "pca_v1"

    def test_date_range_is_stored(self, repo: BacktestRepository) -> None:
        """start_date / end_date が保存されること。"""
        run = _make_run(repo, start=_JAN_06, end=_JAN_10)
        assert run.start_date == _JAN_06
        assert run.end_date == _JAN_10

    def test_cost_params_are_stored(self, repo: BacktestRepository) -> None:
        """commission_rate / slippage_rate が保存されること。"""
        run = _make_run(repo, commission=0.001, slippage=0.001)
        assert run.commission_rate == Decimal("0.001")
        assert run.slippage_rate == Decimal("0.001")

    def test_default_cost_is_zero(self, repo: BacktestRepository) -> None:
        """コスト未指定時はデフォルト 0.0 になること。"""
        run = _make_run(repo)
        assert run.commission_rate == Decimal("0.0")
        assert run.slippage_rate == Decimal("0.0")

    def test_run_name_optional(self, repo: BacktestRepository) -> None:
        """run_name は None でも保存できること。"""
        run = _make_run(repo, run_name=None)
        assert run.run_name is None

    def test_run_name_stored(self, repo: BacktestRepository) -> None:
        """run_name 指定時は保存されること。"""
        run = _make_run(repo, run_name="test run")
        assert run.run_name == "test run"

    def test_parameter_json_defaults_to_empty(self, repo: BacktestRepository) -> None:
        """parameters_json 未指定時は空 dict になること。"""
        run = _make_run(repo)
        assert run.parameter_json == {}

    def test_summary_stats_are_null_on_creation(self, repo: BacktestRepository) -> None:
        """作成直後のサマリー統計は全て None であること。"""
        run = _make_run(repo)
        assert run.trading_days is None
        assert run.total_return is None
        assert run.annual_return is None
        assert run.annual_vol is None
        assert run.sharpe_ratio is None
        assert run.max_drawdown is None
        assert run.win_rate is None
        assert run.finished_at is None


# ---------------------------------------------------------------------------
# 2. finalize_run
# ---------------------------------------------------------------------------


class TestFinalizeRun:
    def test_status_becomes_done(self, repo: BacktestRepository) -> None:
        """finalize_run 後は status が 'done' になること。"""
        run = _make_run(repo)
        repo.finalize_run(run.id, trading_days=3)
        assert run.status == BacktestStatus.DONE.value

    def test_finished_at_is_set(self, repo: BacktestRepository) -> None:
        """finalize_run 後は finished_at が設定されること。"""
        run = _make_run(repo)
        repo.finalize_run(run.id, trading_days=3)
        assert run.finished_at is not None

    def test_trading_days_is_stored(self, repo: BacktestRepository) -> None:
        """trading_days が正しく保存されること。"""
        run = _make_run(repo)
        repo.finalize_run(run.id, trading_days=5)
        assert run.trading_days == 5

    def test_summary_stats_are_stored(self, repo: BacktestRepository) -> None:
        """全サマリー統計が正しく保存されること。"""
        run = _make_run(repo)
        repo.finalize_run(
            run.id,
            trading_days=100,
            total_return=0.15,
            annual_return=0.18,
            annual_vol=0.12,
            sharpe_ratio=1.5,
            max_drawdown=0.05,
            win_rate=0.6,
        )
        assert run.total_return == Decimal("0.15")
        assert run.annual_return == Decimal("0.18")
        assert run.annual_vol == Decimal("0.12")
        assert run.sharpe_ratio == Decimal("1.5")
        assert run.max_drawdown == Decimal("0.05")
        assert run.win_rate == Decimal("0.6")

    def test_none_stats_remain_none(self, repo: BacktestRepository) -> None:
        """統計を渡さない場合は None のままであること。"""
        run = _make_run(repo)
        repo.finalize_run(run.id, trading_days=0)
        assert run.total_return is None
        assert run.sharpe_ratio is None

    def test_returns_none_for_unknown_run_id(self, repo: BacktestRepository) -> None:
        """存在しない run_id は None を返すこと (例外なし)。"""
        result = repo.finalize_run(99999, trading_days=0)
        assert result is None

    def test_custom_status_failed(self, repo: BacktestRepository) -> None:
        """status="failed" で finalize できること。"""
        run = _make_run(repo)
        repo.finalize_run(run.id, trading_days=0, status=BacktestStatus.FAILED.value)
        assert run.status == BacktestStatus.FAILED.value


# ---------------------------------------------------------------------------
# 3. save_daily_results
# ---------------------------------------------------------------------------


class TestSaveDailyResults:
    def test_returns_row_count(self, repo: BacktestRepository) -> None:
        """保存した行数を返すこと。"""
        run = _make_run(repo)
        rows = [_make_daily(run.id, _JAN_06), _make_daily(run.id, _JAN_07)]
        count = repo.save_daily_results(rows)
        assert count == 2

    def test_empty_input_returns_zero(self, repo: BacktestRepository) -> None:
        """空リストは 0 を返すこと (例外なし)。"""
        count = repo.save_daily_results([])
        assert count == 0

    def test_daily_return_nullable(self, repo: BacktestRepository) -> None:
        """daily_return=None でも保存できること。"""
        run = _make_run(repo)
        row = _make_daily(run.id, _JAN_06, daily_return=None, cumulative_return=None)
        repo.save_daily_results([row])
        results = repo.list_daily_results(run.id)
        assert len(results) == 1
        assert results[0].daily_return is None

    def test_long_short_return_stored(self, repo: BacktestRepository) -> None:
        """long_return / short_return が正しく保存されること。"""
        run = _make_run(repo)
        row = _make_daily(
            run.id, _JAN_06,
            daily_return=0.005,
            cumulative_return=0.005,
            long_return=0.012,
            short_return=-0.002,
        )
        repo.save_daily_results([row])
        results = repo.list_daily_results(run.id)
        assert results[0].long_return == Decimal("0.012")
        assert results[0].short_return == Decimal("-0.002")

    def test_long_short_return_nullable(self, repo: BacktestRepository) -> None:
        """long_return / short_return が None でも保存できること。"""
        run = _make_run(repo)
        row = _make_daily(run.id, _JAN_06, long_return=None, short_return=None)
        repo.save_daily_results([row])
        results = repo.list_daily_results(run.id)
        assert results[0].long_return is None
        assert results[0].short_return is None


# ---------------------------------------------------------------------------
# 4. get_run
# ---------------------------------------------------------------------------


class TestGetRun:
    def test_returns_run_by_id(self, repo: BacktestRepository) -> None:
        """存在する run_id で BacktestRun を返すこと。"""
        run = _make_run(repo)
        fetched = repo.get_run(run.id)
        assert fetched is not None
        assert fetched.id == run.id

    def test_returns_none_for_unknown_id(self, repo: BacktestRepository) -> None:
        """存在しない run_id は None を返すこと (例外なし)。"""
        result = repo.get_run(99999)
        assert result is None

    def test_signal_type_persisted(self, repo: BacktestRepository, session: Session) -> None:
        """get_run で取得した run の signal_type が正しいこと。"""
        run = _make_run(repo, signal_type="pca_v1")
        session.commit()

        # 別 session で取得して確認
        other_repo = BacktestRepository(session)
        fetched = other_repo.get_run(run.id)
        assert fetched is not None
        assert fetched.signal_type == "pca_v1"


# ---------------------------------------------------------------------------
# 5. list_daily_results
# ---------------------------------------------------------------------------


class TestListDailyResults:
    def test_returns_results_for_run(
        self, repo: BacktestRepository
    ) -> None:
        """指定 run_id の日次結果を返すこと。"""
        run = _make_run(repo)
        rows = [
            _make_daily(run.id, _JAN_06, daily_return=0.01),
            _make_daily(run.id, _JAN_07, daily_return=0.02),
        ]
        repo.save_daily_results(rows)
        results = repo.list_daily_results(run.id)
        assert len(results) == 2

    def test_results_are_ascending_by_jp_execution_date(
        self, repo: BacktestRepository
    ) -> None:
        """結果は jp_execution_date 昇順で返ること。"""
        run = _make_run(repo)
        # 逆順で登録
        rows = [
            _make_daily(run.id, _JAN_09),
            _make_daily(run.id, _JAN_07),
            _make_daily(run.id, _JAN_08),
        ]
        repo.save_daily_results(rows)
        results = repo.list_daily_results(run.id)
        dates = [r.jp_execution_date for r in results]
        assert dates == sorted(dates)

    def test_empty_when_no_results(self, repo: BacktestRepository) -> None:
        """日次結果がない場合は空リストを返すこと。"""
        run = _make_run(repo)
        results = repo.list_daily_results(run.id)
        assert results == []

    def test_run_isolation(self, repo: BacktestRepository) -> None:
        """run_id が異なる結果が混入しないこと。

        run_a と run_b を別々に作成し、list_daily_results が
        指定した run_id の結果だけを返すことを確認する。
        """
        run_a = _make_run(repo, run_name="run_a")
        run_b = _make_run(repo, run_name="run_b")

        repo.save_daily_results([
            _make_daily(run_a.id, _JAN_06, daily_return=0.01),
            _make_daily(run_a.id, _JAN_07, daily_return=0.02),
        ])
        repo.save_daily_results([
            _make_daily(run_b.id, _JAN_06, daily_return=-0.05),
        ])

        results_a = repo.list_daily_results(run_a.id)
        results_b = repo.list_daily_results(run_b.id)

        assert len(results_a) == 2
        assert len(results_b) == 1
        assert all(r.backtest_run_id == run_a.id for r in results_a)
        assert all(r.backtest_run_id == run_b.id for r in results_b)

    def test_returns_none_for_unknown_run(self, repo: BacktestRepository) -> None:
        """存在しない run_id は空リストを返すこと。"""
        results = repo.list_daily_results(99999)
        assert results == []
