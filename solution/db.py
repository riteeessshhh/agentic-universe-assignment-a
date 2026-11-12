"""SQLite database schema, pragmas, and disposition catalogue setup."""
from __future__ import annotations

import sqlite3
from pathlib import Path
import json

from .config import DispositionSpec, WorkflowSchema
from .crypto import PIICipher
from .normalizers import normalize_phone

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ingested_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    workflow_id TEXT,
    filename TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_metrics (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    lead_rows_total INTEGER NOT NULL DEFAULT 0,
    duplicate_lead_rows_merged INTEGER NOT NULL DEFAULT 0,
    field_warnings INTEGER NOT NULL DEFAULT 0,
    events_total INTEGER NOT NULL DEFAULT 0,
    malformed_events INTEGER NOT NULL DEFAULT 0,
    duplicate_events_dropped INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO ingestion_metrics (id) VALUES (1);

CREATE TABLE IF NOT EXISTS dispositions (
    workflow_id TEXT NOT NULL,
    code TEXT NOT NULL,
    label TEXT NOT NULL,
    is_terminal INTEGER NOT NULL,
    is_success INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, code)
);

CREATE TABLE IF NOT EXISTS leads (
    lead_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    phone_hmac BLOB NOT NULL,
    phone_enc BLOB NOT NULL,
    pii_enc BLOB NOT NULL,
    external_ref TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    UNIQUE (workflow_id, phone_hmac)
);

CREATE INDEX IF NOT EXISTS idx_leads_wf_ref ON leads(workflow_id, external_ref);
CREATE INDEX IF NOT EXISTS idx_leads_wf_batch ON leads(workflow_id, batch_id);

CREATE TABLE IF NOT EXISTS lead_rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rejections_sort
    ON lead_rejections(workflow_id, batch_id, row_number);

CREATE TABLE IF NOT EXISTS interaction_ledger (
    ledger_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    occurred_at_ist TEXT NOT NULL,
    occurred_at_us INTEGER NOT NULL,
    ist_date TEXT NOT NULL,
    phone_hmac BLOB,
    disposition_code TEXT,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_wf_phone
    ON interaction_ledger(workflow_id, phone_hmac);
CREATE INDEX IF NOT EXISTS idx_ledger_wf_call
    ON interaction_ledger(workflow_id, call_id);
CREATE INDEX IF NOT EXISTS idx_ledger_type_wf_call
    ON interaction_ledger(event_type, workflow_id, call_id);

CREATE TRIGGER IF NOT EXISTS trg_ledger_no_update
BEFORE UPDATE ON interaction_ledger
BEGIN
    SELECT RAISE(ABORT, 'interaction_ledger is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_ledger_no_delete
BEFORE DELETE ON interaction_ledger
BEGIN
    SELECT RAISE(ABORT, 'interaction_ledger is append-only');
END;
"""


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection configured for correctness and high throughput."""
    db_str = str(db_path)
    if db_str != ":memory:":
        Path(db_str).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_str, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA cache_size = -65536;")
    return conn


def init_db(conn: sqlite3.Connection, dispositions: list[DispositionSpec]) -> None:
    """Initialize tables, triggers, and sync the disposition catalogue."""
    conn.executescript(SCHEMA_SQL)
    conn.execute("BEGIN")
    try:
        conn.executemany(
            """
            INSERT INTO dispositions (workflow_id, code, label, is_terminal, is_success)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(workflow_id, code) DO UPDATE SET
                label = excluded.label,
                is_terminal = excluded.is_terminal,
                is_success = excluded.is_success
            """,
            [
                (d.workflow_id, d.code, d.label, int(d.is_terminal), int(d.is_success))
                for d in dispositions
            ],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def get_lead_profile(
    conn: sqlite3.Connection,
    schema: WorkflowSchema,
    raw_phone: str,
    cipher: PIICipher,
) -> dict[str, str | None] | None:
    """
    Decrypt a stored lead and apply schema defaults to any still-null fields.
    Returns None if the phone is invalid or not found.
    """
    canonical = normalize_phone(raw_phone)
    if canonical is None:
        return None
    phone_hmac = cipher.blind_index(canonical)
    row = conn.execute(
        """
        SELECT phone_enc, pii_enc, external_ref, batch_id, attributes_json
        FROM leads
        WHERE workflow_id = ? AND phone_hmac = ?
        """,
        (schema.workflow_id, phone_hmac),
    ).fetchone()
    if row is None:
        return None

    pii_obj = cipher.decrypt_json(row["pii_enc"])
    attrs_obj = json.loads(row["attributes_json"])

    profile: dict[str, str | None] = {
        "batch_id": row["batch_id"],
        schema.phone_field: cipher.decrypt_text(row["phone_enc"]),
        schema.reference_field: row["external_ref"],
    }
    for f in schema.fields:
        if f.name in profile:
            continue
        if f.pii:
            val = pii_obj.get(f.name)
        else:
            val = attrs_obj.get(f.name)
        if val is None and f.default is not None:
            val = f.default
        profile[f.name] = val
    return profile
