"""Parse and execute the named analytical SQL library."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class AnalyticsQuery:
    name: str
    title: str
    description: str
    chart: str
    sql: str


_METADATA_PREFIXES = {
    "-- title:": "title",
    "-- description:": "description",
    "-- chart:": "chart",
}


def load_analytics_queries(path: Path) -> list[AnalyticsQuery]:
    """Read the ``-- name:`` separated SQL file into query objects."""
    if not path.exists():
        raise FileNotFoundError(f"Analytics SQL file not found: {path}")

    queries: list[AnalyticsQuery] = []
    current: dict[str, str] = {}
    sql_lines: list[str] = []

    def flush() -> None:
        if not current:
            return
        sql = "\n".join(sql_lines).strip().rstrip(";").strip()
        if sql:
            queries.append(
                AnalyticsQuery(
                    name=current["name"],
                    title=current.get(
                        "title", current["name"].replace("_", " ").title()
                    ),
                    description=current.get("description", ""),
                    chart=current.get("chart", "table"),
                    sql=sql,
                )
            )

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            flush()
            current = {"name": stripped.split(":", 1)[1].strip()}
            sql_lines = []
            continue
        matched = False
        for prefix, key in _METADATA_PREFIXES.items():
            if stripped.startswith(prefix):
                if current:
                    current[key] = stripped.split(":", 1)[1].strip()
                matched = True
                break
        if matched:
            continue
        if current:
            sql_lines.append(line)
    flush()
    if not queries:
        raise ValueError(f"No named analytics queries found in {path}")
    return queries


def run_analytics_query(engine: Engine, query: AnalyticsQuery) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(query.sql), conn)
