"""
backtest_run / backtest_result_daily テーブル。

バックテストの実行管理と日次損益結果を保存する。

backtest_run         : バックテスト実行の親レコード (1 回の実行 = 1 レコード)
backtest_result_daily: バックテスト実行ごとの日次損益 (1 日 1 レコード)

# カラム命名規則
  backtest_result_daily では signal_daily との整合性を保つため
  jp_execution_date (= signal_daily.jp_execution_date) を使用する。
  initial migration で作成した business_date カラムは
  20260416_b1c2d3e4f5a6 で jp_execution_date にリネームした。

# 先読み防止
  BacktestService はシグナルと価格を jp_execution_date 軸で参照する。
  BacktestResultDaily は jp_execution_date の open / close のみを使って
  計算されたリターンを保存する。
"""

import enum
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

# リターンの精度: 小数点以下 6 桁 (例: 0.012345)
_RETURN_PRECISION = 12
_RETURN_SCALE = 6

# 手数料・スリッページの精度: 小数点以下 6 桁 (例: 0.001000)
_COST_PRECISION = 8
_COST_SCALE = 6


class BacktestStatus(str, enum.Enum):
    """バックテスト実行ステータス。

    str を継承することで JSON シリアライズ時に文字列として扱われる。
    """

    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class BacktestRun(Base):
    """バックテスト実行管理テーブル。

    1 回の backtest 実行につき 1 レコードを作成する。
    実行条件 (パラメータ) と完了後のサマリー統計を保持する。

    Columns:
        id:              サロゲートキー
        run_name:        実行名称 (任意。UI からの識別用)
        signal_type:     使用したシグナル種別 (例: 'simple_v1')
        start_date:      バックテスト開始日 (jp_execution_date 基準)
        end_date:        バックテスト終了日 (jp_execution_date 基準)
        commission_rate: ラウンドトリップ手数料率 (例: 0.001 = 0.1%)
        slippage_rate:   片道エントリースリッページ率 (例: 0.001 = 0.1%)
        parameter_json:  追加パラメータ (将来の拡張用)
        started_at:      実行開始日時 (UTC)
        finished_at:     実行完了日時 (UTC) -- 完了前は NULL
        status:          実行ステータス ('running' | 'done' | 'failed')
        trading_days:    実際に計算できた日数 (skipped 除く)
        total_return:    最終累積リターン
        annual_return:   年率リターン (252 日ベース)
        annual_vol:      年率ボラティリティ (252 日ベース)
        sharpe_ratio:    シャープレシオ (annual_return / annual_vol)
        max_drawdown:    最大ドローダウン (正値、例: 0.15 = 15%)
        win_rate:        勝率 (daily_return > 0 の日数比率)
    """

    __tablename__ = "backtest_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    run_name: Mapped[str | None] = mapped_column(
        String(200),
        nullable=True,
        comment="実行名称 (任意)",
    )

    signal_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="使用したシグナル種別",
    )

    start_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
        comment="バックテスト開始日 (jp_execution_date 基準)",
    )

    end_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
        comment="バックテスト終了日 (jp_execution_date 基準)",
    )

    commission_rate: Mapped[Decimal | None] = mapped_column(
        Numeric(_COST_PRECISION, _COST_SCALE),
        nullable=True,
        comment="ラウンドトリップ手数料率 (例: 0.001000 = 0.1%)",
    )

    slippage_rate: Mapped[Decimal | None] = mapped_column(
        Numeric(_COST_PRECISION, _COST_SCALE),
        nullable=True,
        comment="片道エントリースリッページ率 (例: 0.001000 = 0.1%)",
    )

    parameter_json: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        comment="追加実行パラメータ (将来の拡張用)",
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="実行開始日時 (UTC)",
    )

    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="実行完了日時 (UTC)  -- 実行中は NULL",
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=BacktestStatus.RUNNING.value,
        comment="実行ステータス (running | done | failed)",
    )

    # ---------- サマリー統計 (finalize_run で書き込む) ----------

    trading_days: Mapped[int | None] = mapped_column(
        SmallInteger,
        nullable=True,
        comment="実際に計算できた日数 (skipped 日を除く)",
    )

    total_return: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="最終累積リターン",
    )

    annual_return: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="年率リターン (252 日ベース)",
    )

    annual_vol: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="年率ボラティリティ (252 日ベース)",
    )

    sharpe_ratio: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="シャープレシオ (annual_return / annual_vol)",
    )

    max_drawdown: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="最大ドローダウン (正値, 例: 0.15 = 15%)",
    )

    win_rate: Mapped[Decimal | None] = mapped_column(
        Numeric(_COST_PRECISION, _COST_SCALE),
        nullable=True,
        comment="勝率 (daily_return > 0 の日数 / trading_days)",
    )

    # ---------- リレーション ----------

    results: Mapped[list["BacktestResultDaily"]] = relationship(
        "BacktestResultDaily",
        back_populates="run",
        cascade="all, delete-orphan",
        lazy="select",
        order_by="BacktestResultDaily.jp_execution_date",
    )

    def __repr__(self) -> str:
        return (
            f"<BacktestRun id={self.id!r}"
            f" signal_type={self.signal_type!r}"
            f" status={self.status!r}>"
        )


class BacktestResultDaily(Base):
    """バックテスト日次損益テーブル。

    backtest_run 1 件に対して、評価期間の各営業日に 1 レコードを作成する。

    # 先読み防止
      BacktestService は jp_execution_date の open_price / close_price のみを
      使って各フィールドを計算する。翌日以降の価格は一切参照しない。

    Columns:
        id:                サロゲートキー
        backtest_run_id:   親 BacktestRun の id
        jp_execution_date: 対象営業日 (signal_daily.jp_execution_date と対応)
        daily_return:      その日のポートフォリオ日次リターン (コスト考慮後)。
                           シグナルも価格も全欠損の場合は None (skipped)。
        cumulative_return: 期間開始からの累積リターン。スキップ日はキャリーオーバー。
        long_return:       long ブックの平均リターン (有効銘柄のみ)。欠損時は None。
        short_return:      short ブックの平均リターン (有効銘柄のみ)。欠損時は None。
        long_count:        その日の long ポジション数 (欠損除き)。
        short_count:       その日の short ポジション数 (欠損除き)。

    Constraints:
        UNIQUE (backtest_run_id, jp_execution_date)

    Indexes:
        ix_backtest_result_daily_run_id
        ix_backtest_result_daily_jp_execution_date
    """

    __tablename__ = "backtest_result_daily"

    __table_args__ = (
        UniqueConstraint(
            "backtest_run_id",
            "jp_execution_date",
            name="uq_backtest_result_run_date",
        ),
        Index("ix_backtest_result_daily_run_id", "backtest_run_id"),
        Index("ix_backtest_result_daily_jp_execution_date", "jp_execution_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    backtest_run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("backtest_run.id", ondelete="CASCADE"),
        nullable=False,
        comment="親 BacktestRun の id",
    )

    jp_execution_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="対象営業日 (signal_daily.jp_execution_date 基準)",
    )

    daily_return: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="日次ポートフォリオリターン (コスト考慮後)。全欠損日は None。",
    )

    cumulative_return: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="期間開始からの累積リターン。スキップ日はキャリーオーバー。",
    )

    long_return: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="long ブック平均リターン。有効銘柄なし / 欠損時は None。",
    )

    short_return: Mapped[Decimal | None] = mapped_column(
        Numeric(_RETURN_PRECISION, _RETURN_SCALE),
        nullable=True,
        comment="short ブック平均リターン。有効銘柄なし / 欠損時は None。",
    )

    long_count: Mapped[int | None] = mapped_column(
        SmallInteger,
        nullable=True,
        comment="その日の long ポジション数 (欠損除き)",
    )

    short_count: Mapped[int | None] = mapped_column(
        SmallInteger,
        nullable=True,
        comment="その日の short ポジション数 (欠損除き)",
    )

    # ---------- リレーション ----------

    run: Mapped["BacktestRun"] = relationship(
        "BacktestRun",
        back_populates="results",
    )

    def __repr__(self) -> str:
        return (
            f"<BacktestResultDaily run_id={self.backtest_run_id!r}"
            f" jp_date={self.jp_execution_date!r}"
            f" daily_ret={self.daily_return!r}>"
        )
