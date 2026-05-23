#!/usr/bin/env python3
"""Initialize SQLite workflow state database."""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "workflow_state.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS issue_state (
    issue_id TEXT PRIMARY KEY,
    processed INTEGER DEFAULT 0,
    status TEXT,
    retry_count INTEGER DEFAULT 0,
    cluster TEXT,
    github_issue TEXT,
    confidence REAL,
    workflow_id TEXT,
    odf_version TEXT,
    notes TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);
"""


def init_db() -> Path:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(SCHEMA)
        conn.commit()
    return DB_PATH


if __name__ == "__main__":
    path = init_db()
    print(f"Initialized {path}")
