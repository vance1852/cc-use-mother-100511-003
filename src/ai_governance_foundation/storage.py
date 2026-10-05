"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    submission_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','paused','interrupted','completed','failed')),
    submitted_by TEXT NOT NULL,
    current_boundary TEXT NOT NULL,
    last_step_no INTEGER NOT NULL DEFAULT 0,
    last_step_key TEXT,
    last_output_hash TEXT,
    attempt_no INTEGER NOT NULL DEFAULT 1,
    result_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_attempts (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES ledger_tasks(task_id),
    attempt_no INTEGER NOT NULL,
    triggered_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','abandoned')),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    UNIQUE(task_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS ledger_leases (
    lease_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES ledger_tasks(task_id),
    resource_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('read','write','exclusive')),
    status TEXT NOT NULL CHECK(status IN ('granted','released','revoked')),
    granted_by TEXT NOT NULL,
    granted_at TEXT NOT NULL,
    released_at TEXT,
    released_by TEXT,
    release_reason TEXT
);
CREATE TABLE IF NOT EXISTS ledger_steps (
    task_id TEXT NOT NULL REFERENCES ledger_tasks(task_id),
    step_no INTEGER NOT NULL,
    step_key TEXT NOT NULL,
    action_type TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    output_hash TEXT,
    status TEXT NOT NULL CHECK(status IN ('started','confirmed')),
    started_at TEXT NOT NULL,
    confirmed_at TEXT,
    PRIMARY KEY(task_id, step_no),
    UNIQUE(task_id, step_key)
);
CREATE TABLE IF NOT EXISTS ledger_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL REFERENCES ledger_tasks(task_id),
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    resource_id TEXT,
    step_no INTEGER,
    detail_json TEXT NOT NULL,
    input_hash TEXT,
    output_hash TEXT,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL,
    UNIQUE(task_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_ledger_events_task ON ledger_events(task_id, seq);
CREATE INDEX IF NOT EXISTS idx_ledger_events_resource ON ledger_events(resource_id);
CREATE INDEX IF NOT EXISTS idx_ledger_events_actor ON ledger_events(actor_id);
CREATE INDEX IF NOT EXISTS idx_ledger_leases_resource ON ledger_leases(resource_id, status);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
