"""Read/write workflow issue state (SQLite)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent / "workflow_state.db"


def _conn() -> sqlite3.Connection:
    if not DB_PATH.is_file():
        from .init_state import init_db

        init_db()
    return sqlite3.connect(DB_PATH)


def upsert_issue(
    issue_id: str,
    *,
    processed: bool | None = None,
    status: str | None = None,
    retry_count: int | None = None,
    cluster: str | None = None,
    github_issue: str | None = None,
    confidence: float | None = None,
    workflow_id: str | None = None,
    odf_version: str | None = None,
    notes: str | None = None,
) -> None:
    fields: dict[str, Any] = {"issue_id": issue_id}
    if processed is not None:
        fields["processed"] = int(processed)
    for key, val in (
        ("status", status),
        ("retry_count", retry_count),
        ("cluster", cluster),
        ("github_issue", github_issue),
        ("confidence", confidence),
        ("workflow_id", workflow_id),
        ("odf_version", odf_version),
        ("notes", notes),
    ):
        if val is not None:
            fields[key] = val

    cols = ", ".join(fields)
    placeholders = ", ".join("?" for _ in fields)
    updates = ", ".join(f"{k}=excluded.{k}" for k in fields if k != "issue_id")
    sql = f"""
        INSERT INTO issue_state ({cols}) VALUES ({placeholders})
        ON CONFLICT(issue_id) DO UPDATE SET {updates}, updated_at=datetime('now')
    """
    with _conn() as conn:
        conn.execute(sql, tuple(fields.values()))
        conn.commit()


def get_issue(issue_id: str) -> dict[str, Any] | None:
    with _conn() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM issue_state WHERE issue_id = ?", (issue_id,)
        ).fetchone()
    return dict(row) if row else None


def snapshot_outcome(issue_id: str, outcome: dict[str, Any]) -> Path:
    history = Path(__file__).resolve().parent / "issue-history"
    history.mkdir(parents=True, exist_ok=True)
    path = history / f"{issue_id}.json"
    path.write_text(json.dumps(outcome, indent=2) + "\n")
    return path
