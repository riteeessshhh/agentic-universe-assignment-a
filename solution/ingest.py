"""Idempotent, incremental ingestion for lead CSVs and call-event JSONL files."""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .config import FieldSpec, WorkflowSchema
from .crypto import PIICipher
from .normalizers import (
    normalize_date,
    normalize_decimal,
    normalize_enum,
    normalize_header,
    normalize_phone,
    parse_timestamp_ist,
)

_HMAC_CACHE_LIMIT = 300_000


def _sha256_file(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def resolve_drops(paths: list[str | Path]) -> list[Path]:
    """
    Resolve CLI path arguments into an ordered list of drop directories.
    Accepts either direct drop directories (containing leads/ or events/)
    or parent directories containing drop_* subdirectories.
    """
    drops: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if not p.is_dir():
            raise FileNotFoundError(f"Ingest path is not a directory: {p}")
        if (p / "leads").is_dir() or (p / "events").is_dir():
            drops.append(p)
        else:
            children = sorted(
                c for c in p.iterdir()
                if c.is_dir() and ((c / "leads").is_dir() or (c / "events").is_dir())
            )
            if not children:
                raise ValueError(f"Directory does not contain leads/ or events/: {p}")
            drops.extend(children)
    return drops


def _normalize_field_value(raw: str, field_type: str, values: list[str]) -> str | None:
    if field_type == "string":
        return raw
    if field_type == "phone":
        return normalize_phone(raw)
    if field_type == "date":
        return normalize_date(raw)
    if field_type == "decimal":
        return normalize_decimal(raw)
    if field_type == "enum":
        return normalize_enum(raw, values)
    raise ValueError(f"Unsupported field type: {field_type}")


def _map_headers(header: list[str], schema: WorkflowSchema) -> dict[str, int]:
    """Map schema field names to CSV column indices, preferring exact field names."""
    col_map: dict[str, int] = {}
    exact_names = {normalize_header(f.name): f.name for f in schema.fields}
    for idx, col in enumerate(header):
        norm = normalize_header(col)
        if norm in exact_names:
            col_map.setdefault(exact_names[norm], idx)
    for idx, col in enumerate(header):
        norm = normalize_header(col)
        if norm in schema.alias_lookup:
            col_map.setdefault(schema.alias_lookup[norm], idx)
    return col_map


def _cached_hmac_for_canonical(
    canonical_phone: str,
    cipher: PIICipher,
    cache: dict[str, bytes | None],
) -> bytes:
    hmac_val = cache.get(canonical_phone)
    if hmac_val is None:
        hmac_val = cipher.blind_index(canonical_phone)
        if len(cache) < _HMAC_CACHE_LIMIT:
            cache[canonical_phone] = hmac_val
    return hmac_val


def _cached_hmac_for_raw(
    raw_phone: str,
    cipher: PIICipher,
    cache: dict[str, bytes | None],
) -> bytes | None:
    if raw_phone in cache:
        return cache[raw_phone]
    norm = normalize_phone(raw_phone)
    hmac_val = _cached_hmac_for_canonical(norm, cipher, cache) if norm else None
    if len(cache) < _HMAC_CACHE_LIMIT:
        cache[raw_phone] = hmac_val
    return hmac_val


def _parse_lead_row(
    row: list[str],
    field_specs: list[tuple[FieldSpec, int | None]],
) -> tuple[dict[str, str | None] | None, str | None, int]:
    """Validate and normalize one non-blank CSV row in schema order."""
    parsed: dict[str, str | None] = {}
    warnings = 0
    row_len = len(row)

    for spec, col_idx in field_specs:
        raw = row[col_idx].strip() if (col_idx is not None and col_idx < row_len) else ""
        if raw == "":
            if spec.required:
                return None, f"missing_required:{spec.name}", 0
            parsed[spec.name] = None
            continue

        val = _normalize_field_value(raw, spec.field_type, spec.values)
        if val is None:
            if spec.required:
                reason = "invalid_phone" if spec.field_type == "phone" else f"invalid_{spec.name}"
                return None, reason, 0
            parsed[spec.name] = None
            warnings += 1
        else:
            parsed[spec.name] = val

    return parsed, None, warnings


def _apply_lead_updates(
    conn: sqlite3.Connection,
    wf_id: str,
    pending_updates: dict[bytes, list[dict]],
    cipher: PIICipher,
) -> None:
    existing_map: dict[bytes, sqlite3.Row] = {}
    keys = list(pending_updates.keys())
    for start in range(0, len(keys), 500):
        chunk = keys[start : start + 500]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT lead_id, phone_hmac, pii_enc, attributes_json
            FROM leads
            WHERE workflow_id = ? AND phone_hmac IN ({placeholders})
            """,
            [wf_id, *chunk],
        ).fetchall()
        for r in rows:
            existing_map[bytes(r["phone_hmac"])] = r

    update_rows = []
    for phone_hmac, updates in pending_updates.items():
        existing = existing_map.get(phone_hmac)
        if existing is None:
            raise RuntimeError("Expected existing lead during duplicate merge")
        pii_obj = cipher.decrypt_json(existing["pii_enc"])
        attrs_obj = json.loads(existing["attributes_json"])
        ext_ref = ""
        for upd in updates:
            ext_ref = upd["external_ref"]
            pii_obj.update(upd["pii"])
            attrs_obj.update(upd["attrs"])
        update_rows.append(
            (
                cipher.encrypt_json(pii_obj),
                ext_ref,
                json.dumps(attrs_obj, ensure_ascii=False, separators=(",", ":")),
                existing["lead_id"],
            )
        )

    conn.executemany(
        """
        UPDATE leads
        SET pii_enc = ?, external_ref = ?, attributes_json = ?
        WHERE lead_id = ?
        """,
        update_rows,
    )


def _ingest_lead_csv(
    conn: sqlite3.Connection,
    csv_path: Path,
    schema: WorkflowSchema,
    cipher: PIICipher,
    existing_hmacs: set[bytes],
    phone_hmac_cache: dict[str, bytes | None],
) -> None:
    sha256 = _sha256_file(csv_path)
    wf_id = schema.workflow_id
    batch_id = csv_path.stem
    file_key = f"leads:{wf_id}:{csv_path.name}:{sha256}"

    if conn.execute("SELECT 1 FROM ingested_files WHERE file_key = ?", (file_key,)).fetchone():
        return

    lead_rows_total = 0
    duplicate_lead_rows_merged = 0
    field_warnings = 0
    rejections: list[tuple[str, str, int, str]] = []
    pending_inserts: dict[bytes, dict] = {}
    pending_updates: dict[bytes, list[dict]] = defaultdict(list)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        if header is not None:
            col_map = _map_headers(header, schema)
            field_specs = [(spec, col_map.get(spec.name)) for spec in schema.fields]
            row_number = 0

            for row in reader:
                if not row or all(cell.strip() == "" for cell in row):
                    continue
                row_number += 1
                lead_rows_total += 1

                parsed, reason, row_warnings = _parse_lead_row(row, field_specs)
                if reason is not None:
                    rejections.append((wf_id, batch_id, row_number, reason))
                    continue

                assert parsed is not None
                field_warnings += row_warnings
                canonical_phone = parsed[schema.phone_field]
                assert canonical_phone is not None
                phone_hmac = _cached_hmac_for_canonical(canonical_phone, cipher, phone_hmac_cache)
                external_ref = parsed[schema.reference_field]
                assert external_ref is not None

                pii_updates: dict[str, str] = {}
                attr_updates: dict[str, str] = {}
                for spec, _ in field_specs:
                    val = parsed[spec.name]
                    if val is None or spec.name == schema.phone_field or spec.reference:
                        continue
                    if spec.pii:
                        pii_updates[spec.name] = val
                    else:
                        attr_updates[spec.name] = val

                if phone_hmac in pending_inserts:
                    duplicate_lead_rows_merged += 1
                    rec = pending_inserts[phone_hmac]
                    rec["external_ref"] = external_ref
                    rec["pii"].update(pii_updates)
                    rec["attrs"].update(attr_updates)
                elif phone_hmac in existing_hmacs:
                    duplicate_lead_rows_merged += 1
                    pending_updates[phone_hmac].append(
                        {"external_ref": external_ref, "pii": pii_updates, "attrs": attr_updates}
                    )
                else:
                    pending_inserts[phone_hmac] = {
                        "phone": canonical_phone,
                        "external_ref": external_ref,
                        "batch_id": batch_id,
                        "pii": pii_updates,
                        "attrs": attr_updates,
                    }

    if pending_inserts:
        conn.executemany(
            """
            INSERT INTO leads (
                workflow_id, phone_hmac, phone_enc, pii_enc,
                external_ref, batch_id, attributes_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    wf_id,
                    phone_hmac,
                    cipher.encrypt_text(rec["phone"]),
                    cipher.encrypt_json(rec["pii"]),
                    rec["external_ref"],
                    rec["batch_id"],
                    json.dumps(rec["attrs"], ensure_ascii=False, separators=(",", ":")),
                )
                for phone_hmac, rec in pending_inserts.items()
            ],
        )

    if pending_updates:
        _apply_lead_updates(conn, wf_id, pending_updates, cipher)

    if rejections:
        conn.executemany(
            """
            INSERT INTO lead_rejections (workflow_id, batch_id, row_number, reason)
            VALUES (?, ?, ?, ?)
            """,
            rejections,
        )

    conn.execute(
        """
        UPDATE ingestion_metrics
        SET lead_rows_total = lead_rows_total + ?,
            duplicate_lead_rows_merged = duplicate_lead_rows_merged + ?,
            field_warnings = field_warnings + ?
        WHERE id = 1
        """,
        (lead_rows_total, duplicate_lead_rows_merged, field_warnings),
    )

    conn.execute(
        """
        INSERT INTO ingested_files (file_key, kind, workflow_id, filename, sha256, ingested_at)
        VALUES (?, 'leads', ?, ?, ?, ?)
        """,
        (file_key, wf_id, csv_path.name, sha256, datetime.now(timezone.utc).isoformat()),
    )

    existing_hmacs.update(pending_inserts.keys())


def _ingest_events_jsonl(
    conn: sqlite3.Connection,
    jsonl_path: Path,
    cipher: PIICipher,
    seen_event_ids: set[str],
    phone_hmac_cache: dict[str, bytes | None],
) -> None:
    sha256 = _sha256_file(jsonl_path)
    file_key = f"events::{jsonl_path.name}:{sha256}"

    if conn.execute("SELECT 1 FROM ingested_files WHERE file_key = ?", (file_key,)).fetchone():
        return

    events_total = 0
    malformed_events = 0
    duplicate_events_dropped = 0
    event_rows: list[tuple] = []

    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            events_total += 1

            try:
                ev = json.loads(stripped)
            except json.JSONDecodeError:
                malformed_events += 1
                continue

            if not isinstance(ev, dict):
                malformed_events += 1
                continue

            event_id = ev.get("event_id")
            event_type = ev.get("event_type")
            workflow_id = ev.get("workflow_id")
            call_id = ev.get("call_id")
            occurred_at_raw = ev.get("occurred_at")

            if (
                not isinstance(event_id, str)
                or not event_id.strip()
                or not isinstance(event_type, str)
                or not event_type.strip()
                or not isinstance(workflow_id, str)
                or not workflow_id.strip()
                or not isinstance(call_id, str)
                or not call_id.strip()
                or not isinstance(occurred_at_raw, str)
            ):
                malformed_events += 1
                continue

            ts = parse_timestamp_ist(occurred_at_raw)
            if ts is None:
                malformed_events += 1
                continue

            event_id = event_id.strip()
            if event_id in seen_event_ids:
                duplicate_events_dropped += 1
                continue
            seen_event_ids.add(event_id)

            event_type = event_type.strip()
            workflow_id = workflow_id.strip()
            call_id = call_id.strip()
            occurred_at_ist, occurred_at_us, ist_date = ts

            raw_phone = ev.get("phone")
            phone_hmac = (
                _cached_hmac_for_raw(raw_phone, cipher, phone_hmac_cache)
                if isinstance(raw_phone, str) and raw_phone.strip()
                else None
            )

            payload = ev.get("payload")
            if not isinstance(payload, dict):
                payload = {}

            disposition_code: str | None = None
            if event_type.startswith("disposition."):
                dc = payload.get("disposition_code")
                if isinstance(dc, str) and dc.strip():
                    disposition_code = dc.strip()

            event_rows.append(
                (
                    event_id,
                    event_type,
                    workflow_id,
                    call_id,
                    occurred_at_ist,
                    occurred_at_us,
                    ist_date,
                    phone_hmac,
                    disposition_code,
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                )
            )

    if event_rows:
        conn.executemany(
            """
            INSERT INTO interaction_ledger (
                event_id, event_type, workflow_id, call_id,
                occurred_at_ist, occurred_at_us, ist_date,
                phone_hmac, disposition_code, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            event_rows,
        )

    conn.execute(
        """
        UPDATE ingestion_metrics
        SET events_total = events_total + ?,
            malformed_events = malformed_events + ?,
            duplicate_events_dropped = duplicate_events_dropped + ?
        WHERE id = 1
        """,
        (events_total, malformed_events, duplicate_events_dropped),
    )

    conn.execute(
        """
        INSERT INTO ingested_files (file_key, kind, workflow_id, filename, sha256, ingested_at)
        VALUES (?, 'events', NULL, ?, ?, ?)
        """,
        (file_key, jsonl_path.name, sha256, datetime.now(timezone.utc).isoformat()),
    )


def ingest_paths(
    conn: sqlite3.Connection,
    paths: list[str | Path],
    workflows: dict[str, WorkflowSchema],
    cipher: PIICipher,
) -> dict[str, int]:
    """Ingest all lead CSVs and event JSONL files from the resolved drops in order."""
    drops = resolve_drops(paths)

    hmacs_by_wf: dict[str, set[bytes]] = {}
    seen_event_ids: set[str] | None = None
    phone_hmac_cache: dict[str, bytes | None] = {}
    files_processed = 0

    for drop in drops:
        conn.execute("BEGIN")
        try:
            leads_root = drop / "leads"
            if leads_root.is_dir():
                for wf_dir in sorted(p for p in leads_root.iterdir() if p.is_dir()):
                    wf_id = wf_dir.name
                    if wf_id not in workflows:
                        raise ValueError(f"Unknown workflow directory '{wf_id}' in {leads_root}")
                    if wf_id not in hmacs_by_wf:
                        rows = conn.execute(
                            "SELECT phone_hmac FROM leads WHERE workflow_id = ?",
                            (wf_id,),
                        ).fetchall()
                        hmacs_by_wf[wf_id] = {bytes(r["phone_hmac"]) for r in rows}
                    schema = workflows[wf_id]
                    for csv_path in sorted(wf_dir.glob("*.csv")):
                        _ingest_lead_csv(
                            conn,
                            csv_path,
                            schema,
                            cipher,
                            hmacs_by_wf[wf_id],
                            phone_hmac_cache,
                        )
                        files_processed += 1

            events_root = drop / "events"
            if events_root.is_dir():
                event_files = sorted(events_root.glob("*.jsonl"))
                if event_files and seen_event_ids is None:
                    seen_event_ids = {
                        r["event_id"]
                        for r in conn.execute("SELECT event_id FROM interaction_ledger")
                    }
                for jsonl_path in event_files:
                    assert seen_event_ids is not None
                    _ingest_events_jsonl(
                        conn,
                        jsonl_path,
                        cipher,
                        seen_event_ids,
                        phone_hmac_cache,
                    )
                    files_processed += 1

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {"drops": len(drops), "files_checked": files_processed}
