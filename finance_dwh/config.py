"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.engine import make_url

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)

DEFAULT_DATABASE_URL = (
    "postgresql+psycopg2://dwh_user:dwh_password@localhost:5433/finance_dwh"
)


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the pipeline and dashboard."""

    database_url: str
    data_dir: Path
    analytics_sql_path: Path
    data_dictionary_path: Path

    @property
    def safe_database_url(self) -> str:
        """Return a database URL that is safe to display in logs and the UI."""
        return make_url(self.database_url).render_as_string(hide_password=True)

    @property
    def database_backend(self) -> str:
        return make_url(self.database_url).get_backend_name()

    def source_path(self, filename: str) -> Path:
        return self.data_dir / filename


def get_settings() -> Settings:
    """Build settings from the environment, with portfolio-demo defaults."""
    database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL).strip()
    data_dir = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data" / "raw")).expanduser()
    return Settings(
        database_url=database_url,
        data_dir=data_dir.resolve(),
        analytics_sql_path=PROJECT_ROOT / "sql" / "analytics_queries.sql",
        data_dictionary_path=PROJECT_ROOT / "docs" / "data_dictionary.md",
    )
