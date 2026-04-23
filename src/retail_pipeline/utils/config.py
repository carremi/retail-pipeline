"""Centralized configuration loaded from environment variables."""
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (3 levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


class Config:
    """Project-wide configuration."""

    # Paths
    PROJECT_ROOT: Path = PROJECT_ROOT
    DATA_DIR: Path = PROJECT_ROOT / "data"
    DROPS_DIR: Path = DATA_DIR / "drops"
    REFERENCE_DIR: Path = DATA_DIR / "reference"
    LOGS_DIR: Path = PROJECT_ROOT / "logs"
    SQL_DIR: Path = PROJECT_ROOT / "sql"

    # PostgreSQL
    PG_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    PG_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    PG_DB: str = os.getenv("POSTGRES_DB", "retail_pipeline")
    PG_USER: str = os.getenv("POSTGRES_USER", "retail_user")
    PG_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")

    @property
    def pg_url(self) -> str:
        """SQLAlchemy 2.0 connection URL for psycopg3."""
        return (
            f"postgresql+psycopg://{self.PG_USER}:{self.PG_PASSWORD}"
            f"@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DB}"
        )


config = Config()
