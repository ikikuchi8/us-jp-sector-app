"""paper_v2-lite 固定ハイパーパラメータ (v2.2-lite)."""
from __future__ import annotations
from datetime import date
from pathlib import Path
from typing import Final

SIGNAL_TYPE_PAPER_V2: Final[str] = "paper_v2"

# 論文 §3.2 固定値
WINDOW_SIZE: Final[int] = 60
K: Final[int] = 3
LAMBDA: Final[float] = 0.9
Q: Final[float] = 0.3

# NJ=17 に対する丸め (q × NJ = 5.1 → 5)
N_LONG: Final[int] = 5
N_SHORT: Final[int] = 5

# C_full / OOS (Option A)
C_FULL_TRAIN_START: Final[date] = date(2021, 1, 12)
C_FULL_TRAIN_END: Final[date] = date(2023, 12, 31)
PAPER_V2_OOS_START: Final[date] = date(2024, 1, 4)

# artifact パス
C0_VERSION: Final[str] = "v1"
_PAPER_V2_DIR: Final[Path] = Path(__file__).resolve().parent
C0_ARTIFACT_PATH: Final[Path] = _PAPER_V2_DIR / "data" / "c0_v1.npz"
C0_META_PATH: Final[Path] = _PAPER_V2_DIR / "data" / "c0_v1.meta.json"

# データ取得バッファ (カレンダー日数) — paper_v1 と同じ
_FETCH_BUFFER_DAYS: Final[int] = 120

# 標準化のゼロ除算防止
_EPSILON: Final[float] = 1e-8

# universe サイズ
UNIVERSE_SIZE: Final[int] = 28
N_US: Final[int] = 11
N_JP: Final[int] = 17
