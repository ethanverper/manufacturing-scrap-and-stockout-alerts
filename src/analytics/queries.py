"""The KPI queries: one .sql file per business question, in sql/ at the repo root.

Why the SQL lives in files and not in Python strings: analysts read and reuse it directly (in the
Athena console, or in any SQL tool), and a diff of a .sql file is a reviewable change to a KPI.
"""

from __future__ import annotations

from pathlib import Path

# sql/ at the repository root (this file is src/analytics/queries.py)
SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def list_queries(sql_dir: Path = SQL_DIR) -> list[str]:
    """Names of every query, in file order (e.g. "01_reject_rate_by_location").

    Args:
        sql_dir: Folder with the .sql files.

    Returns:
        File names without the .sql extension, sorted.
    """
    return sorted(path.stem for path in sql_dir.glob("*.sql"))


def load_query(name: str, sql_dir: Path = SQL_DIR) -> str:
    """Read one query and drop the trailing semicolon (the Athena API and Spark both reject it).

    Args:
        name: Query name without extension.
        sql_dir: Folder with the .sql files.

    Returns:
        The SQL text.
    """
    return (sql_dir / f"{name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
