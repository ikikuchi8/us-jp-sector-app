"""
BacktestRepository: backtest_run / backtest_result_daily テーブルへの CRUD 操作。

# 責務
  - backtest_run の作成・統計更新・取得
  - backtest_result_daily の一括保存・取得
  - commit は呼び出し側 (BacktestService) が行う

# commit 方針
  create_run / save_daily_results / finalize_run は flush のみ行う。
  トランザクション境界は BacktestService 側で管理する。

# save_daily_results の設計
  各 run_id は create_run で新規発行されるため、同一 run_id に対する
  重複保存は通常発生しない。session.add_all() + flush() で実装する。
  UNIQUE (backtest_run_id, jp_execution_date) が重複保護の最終防衛線となる。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.backtest import BacktestResultDaily, BacktestRun, BacktestStatus


class BacktestRepository:
    """backtest_run / backtest_result_daily テーブルへのアクセスを担う Repository。

    Args:
        session: SQLAlchemy Session。commit / rollback は呼び出し側が行う。
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # BacktestRun の書き込み
    # ------------------------------------------------------------------

    def create_run(
        self,
        signal_type: str,
        start_date: date,
        end_date: date,
        commission_rate: float = 0.0,
        slippage_rate: float = 0.0,
        run_name: str | None = None,
        parameters_json: dict | None = None,
    ) -> BacktestRun:
        """BacktestRun を新規作成して DB に flush する。

        flush により run.id が採番されるが、commit は呼び出し側が行う。
        status は "running" で作成される。

        Args:
            signal_type:      使用するシグナル種別
            start_date:       バックテスト開始日 (jp_execution_date 基準)
            end_date:         バックテスト終了日 (jp_execution_date 基準)
            commission_rate:  ラウンドトリップ手数料率 (デフォルト 0.0)
            slippage_rate:    片道エントリースリッページ率 (デフォルト 0.0)
            run_name:         実行名称 (任意)
            parameters_json:  追加パラメータ dict (None の場合は空 dict)

        Returns:
            flush 後の BacktestRun インスタンス (id が採番されている)。
        """
        run = BacktestRun(
            signal_type=signal_type,
            start_date=start_date,
            end_date=end_date,
            commission_rate=Decimal(str(commission_rate)),
            slippage_rate=Decimal(str(slippage_rate)),
            run_name=run_name,
            parameter_json=parameters_json or {},
            status=BacktestStatus.RUNNING.value,
        )
        self._session.add(run)
        self._session.flush()
        return run

    def finalize_run(
        self,
        run_id: int,
        *,
        trading_days: int,
        total_return: float | None = None,
        annual_return: float | None = None,
        annual_vol: float | None = None,
        sharpe_ratio: float | None = None,
        max_drawdown: float | None = None,
        win_rate: float | None = None,
        status: str = BacktestStatus.DONE.value,
    ) -> BacktestRun | None:
        """BacktestRun にサマリー統計を書き込み、status を更新する。

        BacktestService が計算完了後に呼び出す。
        flush のみ行い、commit は呼び出し側が行う。

        Args:
            run_id:        更新対象の BacktestRun id
            trading_days:  実際に計算できた日数
            total_return:  最終累積リターン
            annual_return: 年率リターン
            annual_vol:    年率ボラティリティ
            sharpe_ratio:  シャープレシオ
            max_drawdown:  最大ドローダウン (正値)
            win_rate:      勝率
            status:        "done" | "failed" (デフォルト "done")

        Returns:
            更新後の BacktestRun。run_id が存在しない場合は None。
        """
        run = self.get_run(run_id)
        if run is None:
            return None

        def _d(v: float | None) -> Decimal | None:
            return Decimal(str(v)) if v is not None else None

        run.trading_days = trading_days
        run.total_return = _d(total_return)
        run.annual_return = _d(annual_return)
        run.annual_vol = _d(annual_vol)
        run.sharpe_ratio = _d(sharpe_ratio)
        run.max_drawdown = _d(max_drawdown)
        run.win_rate = _d(win_rate)
        run.status = status
        run.finished_at = datetime.now(timezone.utc)

        self._session.flush()
        return run

    # ------------------------------------------------------------------
    # BacktestResultDaily の書き込み
    # ------------------------------------------------------------------

    def save_daily_results(
        self,
        rows: Sequence[BacktestResultDaily],
    ) -> int:
        """BacktestResultDaily を一括 add して flush する。

        各 run_id は create_run で新規発行されるため、
        同一 run_id に対する重複は通常発生しない。
        UNIQUE (backtest_run_id, jp_execution_date) が重複保護の最終防衛線。

        Args:
            rows: 保存対象の BacktestResultDaily リスト。

        Returns:
            追加した行数。空リストは 0。

        Note:
            commit は呼び出し側が行う。
        """
        if not rows:
            return 0
        self._session.add_all(rows)
        self._session.flush()
        return len(rows)

    # ------------------------------------------------------------------
    # 読み取り
    # ------------------------------------------------------------------

    def get_run(self, run_id: int) -> BacktestRun | None:
        """run_id で BacktestRun を取得する。

        Args:
            run_id: 取得対象の id

        Returns:
            該当 BacktestRun。存在しない場合は None。
        """
        return self._session.get(BacktestRun, run_id)

    def list_daily_results(
        self,
        run_id: int,
    ) -> list[BacktestResultDaily]:
        """run_id に対応する BacktestResultDaily を jp_execution_date 昇順で返す。

        Args:
            run_id: 取得対象の BacktestRun id

        Returns:
            BacktestResultDaily のリスト。該当なしは空リスト。
        """
        stmt = (
            select(BacktestResultDaily)
            .where(BacktestResultDaily.backtest_run_id == run_id)
            .order_by(BacktestResultDaily.jp_execution_date.asc())
        )
        return list(self._session.scalars(stmt).all())
