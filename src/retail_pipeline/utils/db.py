"""Database helpers: engine creation and SQL file execution."""
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from retail_pipeline.utils.config import config


def get_engine() -> Engine:
    """Return a SQLAlchemy engine for the project's Postgres database."""
    return create_engine(config.pg_url, pool_pre_ping=True)


def run_sql_file(path: Path, engine: Engine | None = None) -> None:
    """Execute a .sql file against the database.

    Splits on semicolons naively; fine for our DDL files which don't use
    procedural blocks. For anything more complex, use psql directly.
    """
    engine = engine or get_engine()
    sql = path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    print(f"[db] Executed {path.name} ({len(statements)} statements)")
