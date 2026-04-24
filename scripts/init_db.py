"""Initialize database schemas and tables by running all SQL files in order."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from retail_pipeline.utils.config import config  # noqa: E402
from retail_pipeline.utils.db import get_engine, run_sql_file  # noqa: E402


def main():
    engine = get_engine()
    sql_files = sorted(config.SQL_DIR.glob("*.sql"))

    if not sql_files:
        print(f"[!] No SQL files found in {config.SQL_DIR}")
        sys.exit(1)

    for path in sql_files:
        run_sql_file(path, engine)

    print("[OK] Database initialized.")


if __name__ == "__main__":
    main()
