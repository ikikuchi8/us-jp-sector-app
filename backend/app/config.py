"""
Application configuration.

All settings are read from environment variables (or .env file).
Values with defaults work out-of-the-box for local development.
"""

from functools import lru_cache

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration loaded from environment / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Application
    # ------------------------------------------------------------------
    app_env: str = Field(default="development", description="Runtime environment")
    app_debug: bool = Field(default=True)
    log_level: str = Field(default="INFO")

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    postgres_user: str = Field(default="sector_user")
    postgres_password: str = Field(default="sector_pass")
    postgres_db: str = Field(default="sector_db")
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url(self) -> str:
        """Construct synchronous PostgreSQL URL from individual fields."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def async_database_url(self) -> str:
        """Async variant using asyncpg driver (reserved for future use)."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # ------------------------------------------------------------------
    # CORS
    # ------------------------------------------------------------------
    cors_origins: list[str] = Field(
        default=["http://localhost:3000"],
        description="Allowed CORS origins (comma-separated in env)",
    )

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------
    price_data_source: str = Field(
        default="yfinance",
        description="Data source identifier for price fetching",
    )


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton Settings instance.

    Use as a FastAPI dependency::

        def some_endpoint(settings: Annotated[Settings, Depends(get_settings)]):
            ...
    """
    return Settings()
