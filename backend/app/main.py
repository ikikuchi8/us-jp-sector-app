"""
FastAPI application entry point.

v0_01 最小骨組み。
現時点では /health エンドポイントのみ実装。
Task 2-4 以降で各 Router を include_router で追加していく。
"""

import logging
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import backtest, prices, signals
from app.config import get_settings
from app.database import check_db_connection

logger = logging.getLogger(__name__)

settings = get_settings()

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------
app = FastAPI(
    title="US-JP Sector Lead-Lag API",
    description="米国業種→日本業種のリードラグ投資支援アプリ バックエンド API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(prices.router, prefix="/prices", tags=["prices"])
app.include_router(signals.router, prefix="/signals", tags=["signals"])
app.include_router(backtest.router, prefix="/backtest", tags=["backtest"])


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", tags=["system"])
def health_check() -> dict:
    """アプリケーションおよび DB の疎通確認。

    Returns:
        status: "ok" | "degraded"
        db: "connected" | "unreachable"
        timestamp: ISO 8601 UTC 時刻
    """
    db_ok = check_db_connection()
    overall = "ok" if db_ok else "degraded"

    return {
        "status": overall,
        "db": "connected" if db_ok else "unreachable",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": app.version,
        "env": settings.app_env,
    }
