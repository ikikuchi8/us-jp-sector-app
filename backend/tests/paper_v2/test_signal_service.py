"""
tests/paper_v2/test_signal_service.py — PaperV2SignalService の単体テスト。

テスト戦略:
  - DB / ネットワーク接続なし: PriceRepository / SignalRepository / Session をモック注入
  - CalendarService もモックして build_date_alignment() 戻り値を制御する
  - artifact は実 c0_v1.npz を使う (load_c0_artifact の正常系)

カテゴリ:
  [A] 初期化 / artifact self-check (5 tests)
  [B] OOS 境界 (2 tests)
  [C] pipeline 各段 (6 tests)
  [D] 先読み防止 (3 tests)
  [E] input_metadata_json (3 tests)
  [F] skip_reasons 集計 (3 tests)
  [G] range 実行 (2 tests)
  [H] 冪等性 / 決定性 (2 tests)
  [I] constants / SkipReason カバレッジ (4 tests)
  [J] Codex review 指摘 P1/P2 (4 tests)
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
import pytest

from app.models.price import PriceDaily
from app.models.signal import SignalDaily, SuggestedSide
from app.repositories.price_repository import PriceRepository
from app.repositories.signal_repository import SignalRepository
from app.seed_data.sector_mapping import ALL_JP_TICKERS, JP_TICKER_TO_US_TICKERS
from app.services.calendar_service import (
    COL_JP_EXECUTION_DATE,
    COL_US_SIGNAL_DATE,
    CalendarService,
)
from app.services.paper_v2.artifact_loader import LoadedC0Artifact, load_c0_artifact
from app.services.paper_v2.constants import (
    C0_ARTIFACT_PATH,
    C0_META_PATH,
    K,
    LAMBDA,
    N_JP,
    N_LONG,
    N_SHORT,
    N_US,
    PAPER_V2_OOS_START,
    SIGNAL_TYPE_PAPER_V2,
    UNIVERSE_SIZE,
    WINDOW_SIZE,
)
from app.services.paper_v2.signal_service import (
    PaperV2GenerationResult,
    PaperV2SignalService,
    _ALL_JP_TICKERS,
    _ALL_US_TICKERS,
)
from app.services.paper_v2.skip_reasons import SkipReason
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_US_DATE = date(2024, 1, 3)   # OOS 開始前日
_JP_DATE = date(2024, 1, 4)   # OOS 開始日 = PAPER_V2_OOS_START

_N_US = len(_ALL_US_TICKERS)  # 11
_N_JP = len(_ALL_JP_TICKERS)  # 17

# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------


def _make_alignment(pairs: list[tuple[date, date]]) -> pd.DataFrame:
    """(us_date, jp_date) タプルのリストから build_date_alignment() 相当の DataFrame を返す。"""
    return pd.DataFrame(
        [{COL_US_SIGNAL_DATE: us, COL_JP_EXECUTION_DATE: jp} for us, jp in pairs]
    )


def _make_adj_close(adj_close: float, business_date: date | None = None) -> MagicMock:
    """adjusted_close_price を持つ PriceDaily モックを返す。"""
    p = MagicMock(spec=PriceDaily)
    p.adjusted_close_price = Decimal(str(adj_close))
    p.close_price = Decimal(str(adj_close))
    p.business_date = business_date or _US_DATE
    return p


def _load_real_artifact() -> LoadedC0Artifact:
    """実 artifact をロードして返す。"""
    with open(C0_META_PATH, encoding="utf-8") as f:
        meta = json.load(f)
    us_tickers = tuple(meta["us_tickers"])
    jp_tickers = tuple(meta["jp_tickers"])
    return load_c0_artifact(
        expected_us_tickers=us_tickers,
        expected_jp_tickers=jp_tickers,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_session() -> MagicMock:
    return MagicMock(spec=Session)


@pytest.fixture
def mock_price_repo() -> MagicMock:
    return MagicMock(spec=PriceRepository)


@pytest.fixture
def mock_signal_repo() -> MagicMock:
    return MagicMock(spec=SignalRepository)


@pytest.fixture
def mock_calendar() -> MagicMock:
    return MagicMock(spec=CalendarService)


@pytest.fixture
def real_artifact() -> LoadedC0Artifact:
    return _load_real_artifact()


@pytest.fixture
def service(
    mock_session: MagicMock,
    mock_price_repo: MagicMock,
    mock_signal_repo: MagicMock,
    mock_calendar: MagicMock,
    real_artifact: LoadedC0Artifact,
) -> PaperV2SignalService:
    """実 artifact を注入した PaperV2SignalService。"""
    return PaperV2SignalService(
        session=mock_session,
        calendar_service=mock_calendar,
        price_repository=mock_price_repo,
        signal_repository=mock_signal_repo,
        artifact=real_artifact,
    )


def _setup_window_data(
    mock_calendar: MagicMock,
    mock_price_repo: MagicMock,
    outer_pair: tuple[date, date],
    n_training_rows: int = 80,
) -> None:
    """
    テスト用の訓練窓データをモックに設定する。
    outer_pair: (us_date, jp_date) の外側アラインメント用タプル
    n_training_rows: 訓練行数 (WINDOW_SIZE=60 以上なら成功)
    """
    us_date, jp_date = outer_pair
    outer_alignment = _make_alignment([outer_pair])

    # 訓練用 alignment: n_training_rows ペア
    base_us = us_date - timedelta(days=n_training_rows + 10)
    base_jp = jp_date - timedelta(days=n_training_rows + 10)
    training_pairs = [
        (base_us + timedelta(days=i), base_jp + timedelta(days=i))
        for i in range(n_training_rows)
    ]
    inner_alignment = _make_alignment(training_pairs)

    mock_calendar.build_date_alignment.side_effect = [outer_alignment, inner_alignment]

    # US バッチ取得 (get_prices_between): 各ティッカーに n+5 行の価格
    def make_us_batch(ticker: str, start: date, end: date) -> list[MagicMock]:
        n_days = (end - start).days + 5
        return [
            _make_adj_close(100.0 + j * 0.01, start + timedelta(days=j))
            for j in range(n_days)
        ]

    # JP バッチ取得 (list_by_ticker): 各ティッカーに n+5 行の価格
    def make_jp_batch(ticker: str, *, start: date, end: date) -> list[MagicMock]:
        n_days = (end - start).days + 5
        return [
            _make_adj_close(200.0 + j * 0.02, start + timedelta(days=j))
            for j in range(n_days)
        ]

    mock_price_repo.get_prices_between.side_effect = make_us_batch
    mock_price_repo.list_by_ticker.side_effect = make_jp_batch

    # 現在 US リターン (get_prices_up_to): 2 行
    mock_price_repo.get_prices_up_to.return_value = [
        _make_adj_close(100.0, us_date - timedelta(days=1)),
        _make_adj_close(101.0, us_date),
    ]


# ---------------------------------------------------------------------------
# [A] 初期化 / artifact self-check
# ---------------------------------------------------------------------------


class TestServiceInit:
    def test_service_init_loads_artifact(
        self,
        mock_session: MagicMock,
        mock_calendar: MagicMock,
        real_artifact: LoadedC0Artifact,
    ) -> None:
        """実 artifact を注入した場合、サービスが初期化できること。"""
        svc = PaperV2SignalService(
            session=mock_session,
            calendar_service=mock_calendar,
            artifact=real_artifact,
        )
        assert svc._artifact is real_artifact

    def test_service_init_auto_loads_artifact(
        self,
        mock_session: MagicMock,
        mock_calendar: MagicMock,
    ) -> None:
        """artifact=None の場合、自動的に実 artifact がロードされること。"""
        svc = PaperV2SignalService(
            session=mock_session,
            calendar_service=mock_calendar,
        )
        assert svc._artifact is not None
        assert svc._artifact.c0.shape == (UNIVERSE_SIZE, UNIVERSE_SIZE)

    def test_service_init_fails_on_artifact_missing(
        self,
        mock_session: MagicMock,
        mock_calendar: MagicMock,
        tmp_path: Path,
    ) -> None:
        """artifact ファイルが存在しない場合、RuntimeError で初期化失敗。"""
        fake_npz = tmp_path / "nonexistent.npz"
        with (
            patch(
                "app.services.paper_v2.signal_service.load_c0_artifact",
                side_effect=RuntimeError("[check 1] npz が見つかりません"),
            ),
            pytest.raises(RuntimeError),
        ):
            PaperV2SignalService(
                session=mock_session,
                calendar_service=mock_calendar,
            )

    def test_service_init_fails_on_sha_mismatch(
        self,
        mock_session: MagicMock,
        mock_calendar: MagicMock,
    ) -> None:
        """SHA-256 不一致でロードが失敗する場合、初期化も失敗すること。"""
        with (
            patch(
                "app.services.paper_v2.signal_service.load_c0_artifact",
                side_effect=RuntimeError("[check 3] SHA-256 が一致しません"),
            ),
            pytest.raises(RuntimeError, match="check 3"),
        ):
            PaperV2SignalService(
                session=mock_session,
                calendar_service=mock_calendar,
            )

    def test_service_init_fails_on_ticker_mismatch(
        self,
        mock_session: MagicMock,
        mock_calendar: MagicMock,
    ) -> None:
        """ticker 不一致でロードが失敗する場合、初期化も失敗すること。"""
        with (
            patch(
                "app.services.paper_v2.signal_service.load_c0_artifact",
                side_effect=RuntimeError("[check 4] us_tickers が一致しません"),
            ),
            pytest.raises(RuntimeError, match="check 4"),
        ):
            PaperV2SignalService(
                session=mock_session,
                calendar_service=mock_calendar,
            )


# ---------------------------------------------------------------------------
# [B] OOS 境界
# ---------------------------------------------------------------------------


class TestOOSBoundary:
    def test_skip_before_oos_start(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
    ) -> None:
        """jp_date = 2023-12-29 (OOS 開始前) → BEFORE_OOS_START でスキップ。"""
        before_oos_jp = date(2023, 12, 29)
        before_oos_us = date(2023, 12, 28)

        mock_calendar.build_date_alignment.return_value = _make_alignment(
            [(before_oos_us, before_oos_jp)]
        )

        result = service.generate_signals_for_range(before_oos_jp, before_oos_jp)

        assert before_oos_jp in result.skipped
        assert result.skip_reasons.get(before_oos_jp) == SkipReason.BEFORE_OOS_START
        assert len(result.succeeded) == 0

    def test_process_date_at_oos_boundary(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
    ) -> None:
        """jp_date = PAPER_V2_OOS_START = 2024-01-04 は OOS チェックを通過すること。
        (データ不足などの別理由でスキップされることはあるが、BEFORE_OOS_START ではない)。
        """
        oos_jp = PAPER_V2_OOS_START
        oos_us = oos_jp - timedelta(days=1)

        _setup_window_data(mock_calendar, mock_price_repo, (oos_us, oos_jp))

        result = service.generate_signals_for_range(oos_jp, oos_jp)

        # BEFORE_OOS_START では skip されていないこと
        if oos_jp in result.skip_reasons:
            assert result.skip_reasons[oos_jp] != SkipReason.BEFORE_OOS_START


# ---------------------------------------------------------------------------
# [C] pipeline 各段
# ---------------------------------------------------------------------------


class TestPipeline:
    def test_insufficient_window_skip(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
    ) -> None:
        """訓練窓が WINDOW_SIZE 未満 → INSUFFICIENT_WINDOW でスキップ。"""
        mock_calendar.build_date_alignment.side_effect = [
            _make_alignment([(_US_DATE, _JP_DATE)]),
            pd.DataFrame(),  # 内側の build_date_alignment が空 → 行数 0
        ]
        mock_price_repo.get_prices_between.return_value = []
        mock_price_repo.list_by_ticker.return_value = []

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.skipped
        assert result.skip_reasons.get(_JP_DATE) == SkipReason.INSUFFICIENT_WINDOW

    def test_missing_us_prices_skip(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
    ) -> None:
        """現在 US 価格が 1 行しかない → MISSING_US_PRICES でスキップ。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))
        # get_prices_up_to を 1 行のみに上書き
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close(100.0, _US_DATE)
        ]

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.skipped
        assert result.skip_reasons.get(_JP_DATE) == SkipReason.MISSING_US_PRICES

    def test_successful_generation_writes_17_rows(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """成功時は JP 17 業種分の SignalDaily が upsert_many に渡ること。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.succeeded
        assert result.saved_rows == _N_JP

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        assert len(saved_rows) == _N_JP

    def test_rank_distribution(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """1 日で long 5 / short 5 / neutral 7 の分布になること。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        sides = [r.suggested_side for r in saved_rows]
        assert sides.count(SuggestedSide.LONG.value) == N_LONG
        assert sides.count(SuggestedSide.SHORT.value) == N_SHORT
        assert sides.count(SuggestedSide.NEUTRAL.value) == _N_JP - N_LONG - N_SHORT

    def test_tie_break_alphabetical(
        self,
        real_artifact: LoadedC0Artifact,
    ) -> None:
        """同スコア時は ticker 昇順でランク付けされること。"""
        svc = PaperV2SignalService.__new__(PaperV2SignalService)
        svc._artifact = real_artifact

        # 全業種に同じスコアを与える
        signal = np.ones(N_JP)
        rank_side = svc._rank_and_side(signal)

        # rank=1 の ticker が最小の ticker であること
        rank1_ticker = next(t for t, (rank, _) in rank_side.items() if rank == 1)
        assert rank1_ticker == min(_ALL_JP_TICKERS)

    def test_signal_score_is_standardized(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """signal_score が z スコア空間の値 (極端でないこと) であること。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        scores = [float(r.signal_score) for r in saved_rows if r.signal_score is not None]
        # z スコア空間なのでほとんどの値は -10 ~ 10 の範囲に収まるはず
        assert all(abs(s) < 100.0 for s in scores), f"異常なスコア: {scores}"


# ---------------------------------------------------------------------------
# [D] 先読み防止
# ---------------------------------------------------------------------------


class TestLookaheadPrevention:
    def test_lookahead_prevention_build_window(
        self,
        service: PaperV2SignalService,
        mock_price_repo: MagicMock,
        mock_calendar: MagicMock,
    ) -> None:
        """_build_window が jp_date 当日の価格に触れないこと。
        training_end = jp_execution_date - 1 日 (paper_v1 と同じ契約) で、
        get_prices_between の end 引数が jp_execution_date - 1 日以内であること。
        """
        us_date = _US_DATE
        jp_date = _JP_DATE

        training_end = jp_date - timedelta(days=1)

        # 内側 alignment が 0 行 → 窓が作れないが、呼び出し引数を確認できる
        mock_calendar.build_date_alignment.return_value = pd.DataFrame()
        mock_price_repo.get_prices_between.return_value = []

        service._build_window(us_date, jp_date)

        # get_prices_between が呼ばれた場合、end が training_end (jp_date - 1) を超えていないこと
        for call_args in mock_price_repo.get_prices_between.call_args_list:
            args, kwargs = call_args
            # positional args: (ticker, start_date, end_date)
            if len(args) >= 3:
                end_arg = args[2]
                assert end_arg <= training_end, (
                    f"_build_window が jp_date ({jp_date}) 以降を参照しています: end={end_arg}"
                )

    def test_lookahead_prevention_current_us(
        self,
        service: PaperV2SignalService,
        mock_price_repo: MagicMock,
    ) -> None:
        """_fetch_current_us_returns が as_of_date=us_signal_date のみを渡すこと。"""
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close(100.0, _US_DATE - timedelta(days=1)),
            _make_adj_close(101.0, _US_DATE),
        ]

        service._fetch_current_us_returns(_US_DATE)

        for call_args in mock_price_repo.get_prices_up_to.call_args_list:
            _, kwargs = call_args
            as_of = kwargs.get("as_of_date")
            assert as_of == _US_DATE, (
                f"as_of_date が us_signal_date={_US_DATE} ではなく {as_of} が渡されています"
            )
            assert as_of != _JP_DATE, "jp_execution_date が as_of_date として渡されています (先読み)"

    def test_pipeline_end_to_end_lookahead_safe(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """jp_date 当日に異常価格があっても、シグナルは us_date 以前のデータのみ参照する。
        具体的には: jp_date を get_prices_up_to の as_of_date として渡さないこと。
        """
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        # get_prices_up_to が jp_date を as_of_date として呼ばれていないこと
        for call_args in mock_price_repo.get_prices_up_to.call_args_list:
            _, kwargs = call_args
            as_of = kwargs.get("as_of_date")
            assert as_of != _JP_DATE, (
                f"jp_execution_date={_JP_DATE} が as_of_date として渡されています (先読みバイアス)"
            )


# ---------------------------------------------------------------------------
# [E] input_metadata_json
# ---------------------------------------------------------------------------


class TestMetadata:
    def _get_saved_rows(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> list[SignalDaily]:
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))
        service.generate_signals_for_range(_JP_DATE, _JP_DATE)
        return mock_signal_repo.upsert_many.call_args[0][0]

    def test_metadata_contains_c0_version(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """input_metadata_json に c0_version が含まれること。"""
        rows = self._get_saved_rows(
            service, mock_calendar, mock_price_repo, mock_signal_repo, mock_session
        )
        for row in rows:
            meta = row.input_metadata_json
            assert "c0_version" in meta
            assert isinstance(meta["c0_version"], str)

    def test_metadata_contains_diagnostics(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """K, lambda, top_k_eigenvalues, condition_number, n_train,
        c_full_train_start/end, oos_start が含まれること。"""
        rows = self._get_saved_rows(
            service, mock_calendar, mock_price_repo, mock_signal_repo, mock_session
        )
        required_keys = (
            "K",
            "lambda",
            "top_k_eigenvalues",
            "condition_number",
            "n_train",
            "c_full_train_start",
            "c_full_train_end",
            "oos_start",
        )
        for row in rows:
            meta = row.input_metadata_json
            for key in required_keys:
                assert key in meta, f"key={key!r} が metadata にありません"

        # 数値チェック
        sample_meta = rows[0].input_metadata_json
        assert sample_meta["K"] == K
        assert sample_meta["lambda"] == LAMBDA
        assert len(sample_meta["top_k_eigenvalues"]) == K
        assert isinstance(sample_meta["condition_number"], float)
        assert isinstance(sample_meta["n_train"], int)

    def test_metadata_universe_size_28(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """universe_size が 28 であること。"""
        rows = self._get_saved_rows(
            service, mock_calendar, mock_price_repo, mock_signal_repo, mock_session
        )
        for row in rows:
            assert row.input_metadata_json["universe_size"] == UNIVERSE_SIZE


# ---------------------------------------------------------------------------
# [F] skip_reasons 集計
# ---------------------------------------------------------------------------


class TestSkipReasonsAggregation:
    def test_skip_reasons_populated_for_oos(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
    ) -> None:
        """OOS 前日付でスキップされた場合、skip_reasons に記録されること。"""
        before_oos = date(2023, 12, 28)
        mock_calendar.build_date_alignment.return_value = _make_alignment(
            [(before_oos - timedelta(days=1), before_oos)]
        )

        result = service.generate_signals_for_range(before_oos, before_oos)

        assert before_oos in result.skip_reasons
        assert result.skip_reasons[before_oos] == SkipReason.BEFORE_OOS_START

    def test_skip_reasons_summary_by_count(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
    ) -> None:
        """skip_reasons の件数が skipped の件数と一致すること。"""
        dates = [date(2023, 12, 27), date(2023, 12, 28)]
        pairs = [(d - timedelta(days=1), d) for d in dates]
        mock_calendar.build_date_alignment.return_value = _make_alignment(pairs)

        result = service.generate_signals_for_range(dates[0], dates[-1])

        assert len(result.skip_reasons) == len(result.skipped)

    def test_succeeded_not_in_skip_reasons(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """成功した日付は skip_reasons に含まれないこと。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        for succeeded_date in result.succeeded:
            assert succeeded_date not in result.skip_reasons


# ---------------------------------------------------------------------------
# [G] range 実行
# ---------------------------------------------------------------------------


class TestGenerateForRange:
    def test_generate_signals_for_range_empty_alignment(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
    ) -> None:
        """alignment が空の場合は requested=0、succeeded=[] を返すこと。"""
        mock_calendar.build_date_alignment.return_value = pd.DataFrame()

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert result.requested == 0
        assert result.succeeded == []
        assert isinstance(result, PaperV2GenerationResult)

    def test_generate_signals_for_range_mixed_success_skip(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """成功日と skip 日の混在パターン。"""
        oos_jp = _JP_DATE
        before_oos_jp = date(2023, 12, 29)
        before_oos_us = date(2023, 12, 28)

        outer_alignment = _make_alignment([
            (before_oos_us, before_oos_jp),  # skip: BEFORE_OOS_START
            (_US_DATE, oos_jp),              # attempt OOS
        ])

        # 訓練窓用 inner alignment
        base_us = _US_DATE - timedelta(days=90)
        base_jp = _JP_DATE - timedelta(days=90)
        training_pairs = [
            (base_us + timedelta(days=i), base_jp + timedelta(days=i))
            for i in range(80)
        ]
        inner_alignment = _make_alignment(training_pairs)

        mock_calendar.build_date_alignment.side_effect = [
            outer_alignment,
            inner_alignment,
        ]

        def make_us_batch(ticker: str, start: date, end: date) -> list[MagicMock]:
            n_days = (end - start).days + 5
            return [
                _make_adj_close(100.0 + j * 0.01, start + timedelta(days=j))
                for j in range(n_days)
            ]

        def make_jp_batch(ticker: str, *, start: date, end: date) -> list[MagicMock]:
            n_days = (end - start).days + 5
            return [
                _make_adj_close(200.0 + j * 0.02, start + timedelta(days=j))
                for j in range(n_days)
            ]

        mock_price_repo.get_prices_between.side_effect = make_us_batch
        mock_price_repo.list_by_ticker.side_effect = make_jp_batch
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close(100.0, _US_DATE - timedelta(days=1)),
            _make_adj_close(101.0, _US_DATE),
        ]

        result = service.generate_signals_for_range(before_oos_jp, oos_jp)

        assert result.requested == 2
        assert before_oos_jp in result.skipped
        assert result.skip_reasons[before_oos_jp] == SkipReason.BEFORE_OOS_START


# ---------------------------------------------------------------------------
# [H] 冪等性 / 決定性
# ---------------------------------------------------------------------------


class TestDeterminismAndIdempotency:
    def _setup_for_direct_call(
        self,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
    ) -> None:
        """
        _generate_for_date を直接呼ぶ場合のモック設定。
        外側の build_date_alignment は呼ばれないため、内側の 1 回分だけ設定。
        """
        us_date, jp_date = _US_DATE, _JP_DATE
        base_us = us_date - timedelta(days=90)
        base_jp = jp_date - timedelta(days=90)
        training_pairs = [
            (base_us + timedelta(days=i), base_jp + timedelta(days=i))
            for i in range(80)
        ]
        inner_alignment = _make_alignment(training_pairs)
        mock_calendar.build_date_alignment.return_value = inner_alignment

        def make_us_batch(ticker: str, start: date, end: date) -> list[MagicMock]:
            n_days = (end - start).days + 5
            return [
                _make_adj_close(100.0 + j * 0.01, start + timedelta(days=j))
                for j in range(n_days)
            ]

        def make_jp_batch(ticker: str, *, start: date, end: date) -> list[MagicMock]:
            n_days = (end - start).days + 5
            return [
                _make_adj_close(200.0 + j * 0.02, start + timedelta(days=j))
                for j in range(n_days)
            ]

        mock_price_repo.get_prices_between.side_effect = make_us_batch
        mock_price_repo.list_by_ticker.side_effect = make_jp_batch
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close(100.0, us_date - timedelta(days=1)),
            _make_adj_close(101.0, us_date),
        ]

    def test_deterministic_same_input_same_signals(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """同じ入力データで 2 回実行した場合、スコアが一致すること。"""
        self._setup_for_direct_call(mock_calendar, mock_price_repo)
        rows1, _ = service._generate_for_date(_JP_DATE, _US_DATE)

        # モックをリセットして再設定
        mock_calendar.reset_mock()
        mock_price_repo.reset_mock()
        self._setup_for_direct_call(mock_calendar, mock_price_repo)
        rows2, _ = service._generate_for_date(_JP_DATE, _US_DATE)

        assert len(rows1) == len(rows2) == _N_JP
        for r1, r2 in zip(rows1, rows2):
            assert r1.target_ticker == r2.target_ticker
            assert r1.signal_score == r2.signal_score
            assert r1.signal_rank == r2.signal_rank
            assert r1.suggested_side == r2.suggested_side

    def test_upsert_overwrites_existing(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """同じ日に 2 回 generate した場合、upsert_many が 2 回呼ばれ、
        2 回目も同じ行数を渡すこと (重複行にならない)。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))
        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))
        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert mock_signal_repo.upsert_many.call_count == 2
        rows_call1 = mock_signal_repo.upsert_many.call_args_list[0][0][0]
        rows_call2 = mock_signal_repo.upsert_many.call_args_list[1][0][0]
        assert len(rows_call1) == _N_JP
        assert len(rows_call2) == _N_JP


# ---------------------------------------------------------------------------
# [I] constants カバレッジ / signal_type 確認
# ---------------------------------------------------------------------------


class TestConstants:
    def test_signal_type_paper_v2(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """保存される SignalDaily の signal_type が "paper_v2" であること
        (constants.SIGNAL_TYPE_PAPER_V2 のカバレッジを兼ねる)。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        assert all(r.signal_type == SIGNAL_TYPE_PAPER_V2 for r in saved_rows)

    def test_all_17_jp_tickers_in_output(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """出力に全 17 JP ティッカーが含まれること。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        output_tickers = {r.target_ticker for r in saved_rows}
        assert output_tickers == set(_ALL_JP_TICKERS)

    def test_rank_and_side_covers_all_tickers(
        self,
        real_artifact: LoadedC0Artifact,
    ) -> None:
        """_rank_and_side が全 17 ティッカーを result に含めること。"""
        svc = PaperV2SignalService.__new__(PaperV2SignalService)
        svc._artifact = real_artifact

        rng = np.random.default_rng(42)
        signal = rng.normal(0.0, 1.0, size=N_JP)
        rank_side = svc._rank_and_side(signal)

        assert set(rank_side.keys()) == set(_ALL_JP_TICKERS)
        assert len(rank_side) == _N_JP

    def test_result_is_paper_v2_generation_result(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
    ) -> None:
        """generate_signals_for_range の返値が PaperV2GenerationResult であること。"""
        mock_calendar.build_date_alignment.return_value = pd.DataFrame()

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert isinstance(result, PaperV2GenerationResult)
        assert hasattr(result, "skip_reasons")

    def test_skip_reasons_cover_all_constants(self) -> None:
        """ALL_SKIP_REASONS が SkipReason の全定数を含むこと。"""
        from app.services.paper_v2.skip_reasons import ALL_SKIP_REASONS
        assert SkipReason.BEFORE_OOS_START in ALL_SKIP_REASONS
        assert SkipReason.INSUFFICIENT_WINDOW in ALL_SKIP_REASONS
        assert SkipReason.MISSING_US_PRICES in ALL_SKIP_REASONS
        assert SkipReason.MISSING_JP_PRICES in ALL_SKIP_REASONS
        assert SkipReason.ARTIFACT_CHECK_FAILED in ALL_SKIP_REASONS

    def test_all_jp_tickers_count(self) -> None:
        """_ALL_JP_TICKERS が 17 件であること。"""
        assert len(_ALL_JP_TICKERS) == 17

    def test_all_us_tickers_count(self) -> None:
        """_ALL_US_TICKERS が 11 件であること。"""
        assert len(_ALL_US_TICKERS) == 11


# ---------------------------------------------------------------------------
# [J] Codex review 指摘 P1/P2
# ---------------------------------------------------------------------------


class TestCodexReviewFixes:
    def test_missing_current_us_business_date_mismatch_skips(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
    ) -> None:
        """get_prices_up_to が返す prices[-1].business_date が us_signal_date と異なる場合、
        MISSING_US_PRICES として skip されること (P1 strict 日付 check)。
        """
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        # prices[-1].business_date が us_signal_date より古い → 当日価格なし
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close(99.0, _US_DATE - timedelta(days=2)),
            _make_adj_close(100.0, _US_DATE - timedelta(days=1)),  # ← us_signal_date と不一致
        ]

        result = service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        assert _JP_DATE in result.skipped
        assert result.skip_reasons.get(_JP_DATE) == SkipReason.MISSING_US_PRICES

    def test_training_window_uses_jp_execution_date_minus_1(
        self,
        service: PaperV2SignalService,
        mock_price_repo: MagicMock,
        mock_calendar: MagicMock,
    ) -> None:
        """_build_window の training_end が jp_execution_date - 1 日であること。
        build_date_alignment に渡される end 引数が jp_execution_date - 1 日であることを確認
        (paper_v1 と同じ契約)。
        """
        us_date = _US_DATE
        jp_date = _JP_DATE

        # alignment が空 → 呼び出し引数だけ確認
        mock_calendar.build_date_alignment.return_value = pd.DataFrame()
        mock_price_repo.get_prices_between.return_value = []

        service._build_window(us_date, jp_date)

        # build_date_alignment の呼び出し引数を確認
        call_args_list = mock_calendar.build_date_alignment.call_args_list
        assert len(call_args_list) >= 1
        args, kwargs = call_args_list[0]
        # _build_window は build_date_alignment(buffer_start, training_end) と呼ぶ
        end_arg = args[1] if len(args) >= 2 else kwargs.get("end")
        expected_training_end = jp_date - timedelta(days=1)
        assert end_arg == expected_training_end, (
            f"training_end={end_arg} が jp_execution_date-1={expected_training_end} と異なります"
        )

    def test_jp_adjusted_close_strict_no_fallback(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
    ) -> None:
        """JP の adjusted_close_price=None, close_price=有効値 の場合、
        fallback なしで行がスキップされること (strict adj-close-only ポリシー)。
        訓練行が WINDOW_SIZE 未満になるため INSUFFICIENT_WINDOW か MISSING_JP_PRICES で skip。
        """
        us_date = _US_DATE
        jp_date = _JP_DATE

        # 訓練用 alignment: WINDOW_SIZE 行用意 (JP 価格が全て adj_close=None なら 0 行 valid)
        base_us = us_date - timedelta(days=WINDOW_SIZE + 15)
        base_jp = jp_date - timedelta(days=WINDOW_SIZE + 15)
        training_pairs = [
            (base_us + timedelta(days=i), base_jp + timedelta(days=i))
            for i in range(WINDOW_SIZE + 5)
        ]
        inner_alignment = _make_alignment(training_pairs)
        outer_alignment = _make_alignment([(us_date, jp_date)])
        mock_calendar.build_date_alignment.side_effect = [outer_alignment, inner_alignment]

        # US 価格は正常
        def make_us_batch(ticker: str, start: date, end: date) -> list[MagicMock]:
            n_days = (end - start).days + 5
            return [
                _make_adj_close(100.0 + j * 0.01, start + timedelta(days=j))
                for j in range(n_days)
            ]
        mock_price_repo.get_prices_between.side_effect = make_us_batch

        # JP 価格: adjusted_close_price=None, close_price=有効値
        def make_jp_batch_no_adj(ticker: str, *, start: date, end: date) -> list[MagicMock]:
            n_days = (end - start).days + 5
            result_list = []
            for j in range(n_days):
                p = MagicMock(spec=__import__('app.models.price', fromlist=['PriceDaily']).PriceDaily)
                p.business_date = start + timedelta(days=j)
                p.adjusted_close_price = None   # ← adj_close なし
                p.close_price = Decimal(str(200.0 + j * 0.02))  # close_price は有効
                result_list.append(p)
            return result_list
        mock_price_repo.list_by_ticker.side_effect = make_jp_batch_no_adj

        # 現在 US リターン
        mock_price_repo.get_prices_up_to.return_value = [
            _make_adj_close(100.0, us_date - timedelta(days=1)),
            _make_adj_close(101.0, us_date),
        ]

        result = service.generate_signals_for_range(jp_date, jp_date)

        # fallback なし → JP 欠損で行スキップ → INSUFFICIENT_WINDOW or MISSING_JP_PRICES
        assert jp_date in result.skipped
        assert result.skip_reasons.get(jp_date) in (
            SkipReason.INSUFFICIENT_WINDOW,
            SkipReason.MISSING_JP_PRICES,
        )

    def test_signal_rank_is_complete_1_to_17(
        self,
        service: PaperV2SignalService,
        mock_calendar: MagicMock,
        mock_price_repo: MagicMock,
        mock_signal_repo: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """1 日分の signal_rank を集めると sorted で list(range(1, 18)) になること。"""
        _setup_window_data(mock_calendar, mock_price_repo, (_US_DATE, _JP_DATE))

        service.generate_signals_for_range(_JP_DATE, _JP_DATE)

        saved_rows: list[SignalDaily] = mock_signal_repo.upsert_many.call_args[0][0]
        ranks = sorted(r.signal_rank for r in saved_rows)
        assert ranks == list(range(1, 18)), f"signal_rank が 1-17 の完全セットでありません: {ranks}"
