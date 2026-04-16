"""
ORM モデルパッケージ。

Alembic の env.py から `Base.metadata` を参照するとき、
すべてのモデルが import 済みである必要がある。
このモジュールを import するだけで全テーブルが登録される。

Usage (alembic/env.py)::

    from app.models import *  # noqa: F401, F403
    from app.database import Base
    target_metadata = Base.metadata
"""

from app.models.backtest import BacktestResultDaily, BacktestRun  # noqa: F401
from app.models.instrument import InstrumentMaster  # noqa: F401
from app.models.price import PriceDaily  # noqa: F401
from app.models.signal import SignalDaily  # noqa: F401

__all__ = [
    "InstrumentMaster",
    "PriceDaily",
    "SignalDaily",
    "BacktestRun",
    "BacktestResultDaily",
]
