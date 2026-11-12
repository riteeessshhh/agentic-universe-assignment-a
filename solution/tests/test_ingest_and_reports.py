"""Targeted tests for rejection order, merging, effective disposition, buckets, PII, and idempotency."""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from solution.config import load_schemas
from solution.crypto import PIICipher, load_or_create_master_key
from solution.db import connect_db, get_lead_profile, init_db
from solution.ingest import ingest_paths
from solution.report import generate_reports

SCHEMAS_DIR = Path(__file__).resolve().parents[2] / "assignment-a-data" / "schemas"


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)


def _write_jsonl(path: Path, lines: list[str | dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for item in lines:
            if isinstance(item, dict):
                f.write(json.dumps(item) + "\n")
            else:
                f.write(item + "\n")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _setup_pipeline(tmp_path: Path):
    workflows, dispositions = load_schemas(SCHEMAS_DIR)
    db_path = tmp_path / "test.sqlite"
    cipher = PIICipher(load_or_create_master_key(db_path))
    conn = connect_db(db_path)
    init_db(conn, dispositions)
    return workflows, cipher, conn, db_path


def test_rejection_order_blank_rows_and_warnings(tmp_path: Path) -> None:
    workflows, cipher, conn, _ = _setup_pipeline(tmp_path)
    try:
        drop = tmp_path / "drop_1"
        _write_csv(
            drop / "leads" / "motor_renewal" / "batch_001.csv",
            [
                ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "preferred_language", "extra_col"],
                ["", "", "", "", "", "", ""],                                        # blank row -> skipped & not counted
                ["", "bad_phone", "", "31/02/2026", "abc", "Hindi", "ignored"],      # row 1: missing_required:customer_name (no warnings counted)
                ["Aarav", "", "MOT/1", "", "", "", ""],                              # row 2: missing_required:phone
                ["Diya", "12345", "MOT/2", "", "", "", ""],                          # row 3: invalid_phone
                ["Rohan", "9876543210", "", "", "", "", ""],                         # row 4: missing_required:policy_number
                ["Kavya", "09876543211", "MOT/3", "31/02/2026", "-10", "Hindi", ""], # row 5: valid lead, 3 field warnings!
            ],
        )

        ingest_paths(conn, [drop], workflows, cipher)
        dq = generate_reports(conn, tmp_path / "out", cipher)

        assert dq["lead_rows_total"] == 5
        assert dq["lead_rows_rejected"] == 4
        assert dq["field_warnings"] == 3
        assert dq["rejections_by_reason"] == {
            "invalid_phone": 1,
            "missing_required:customer_name": 1,
            "missing_required:phone": 1,
            "missing_required:policy_number": 1,
        }

        rejs = _read_csv(tmp_path / "out" / "rejections.csv")
        assert [(int(r["row_number"]), r["reason"]) for r in rejs] == [
            (1, "missing_required:customer_name"),
            (2, "missing_required:phone"),
            (3, "invalid_phone"),
            (4, "missing_required:policy_number"),
        ]
    finally:
        conn.close()


def test_duplicate_merging_permanent_batch_and_defaults(tmp_path: Path) -> None:
    workflows, cipher, conn, _ = _setup_pipeline(tmp_path)
    try:
        drop1 = tmp_path / "drop_1"
        drop2 = tmp_path / "drop_2"
        _write_csv(
            drop1 / "leads" / "motor_renewal" / "batch_001.csv",
            [
                ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "preferred_language"],
                ["Aarav", "9876543210", "MOT/1", "2026-11-15", "₹10,000", "ta"],
                ["Meera", "9876543211", "MOT/2", "", "", ""],
            ],
        )
        # Same batch_002 in drop_1 + batch_003 in drop_2 with alias headers
        _write_csv(
            drop2 / "leads" / "motor_renewal" / "batch_003.csv",
            [
                ["Insured Name", "Mobile No", "Policy No", "Expiry Date", "Premium", "Language"],
                # Overwrites name, policy_no, premium; leaves expiry & language null -> must NOT overwrite "ta"!
                ["Aarav Sharma", "+91 98765 43210", "MOT/1B", "31/02/2026", "Rs. 12500", "Hindi"],
                # Overwrites language from null -> "kn"
                ["Meera Nair", "09876543211", "MOT/2", "01-Dec-2026", "", "KN"],
                # Brand new lead with no language -> gets default "en"
                ["Arjun", "91-98765-43212", "MOT/3", "", "", ""],
            ],
        )

        ingest_paths(conn, [drop1], workflows, cipher)
        ingest_paths(conn, [drop2], workflows, cipher)
        dq = generate_reports(conn, tmp_path / "out", cipher)

        assert dq["lead_rows_total"] == 5
        assert dq["duplicate_lead_rows_merged"] == 2
        assert dq["field_warnings"] == 2  # Aarav's bad expiry + bad language

        aarav = get_lead_profile(conn, workflows["motor_renewal"], "9876543210", cipher)
        assert aarav == {
            "batch_id": "batch_001",  # permanent!
            "customer_name": "Aarav Sharma",
            "phone": "+919876543210",
            "policy_number": "MOT/1B",
            "policy_expiry": "2026-11-15",
            "premium_amount": "12500.00",
            "preferred_language": "ta",
        }

        meera = get_lead_profile(conn, workflows["motor_renewal"], "9876543211", cipher)
        assert meera is not None
        assert meera["preferred_language"] == "kn"
        assert meera["policy_expiry"] == "2026-12-01"

        arjun = get_lead_profile(conn, workflows["motor_renewal"], "9876543212", cipher)
        assert arjun is not None
        assert arjun["batch_id"] == "batch_003"
        assert arjun["preferred_language"] == "en"  # default applied after merge
    finally:
        conn.close()


def test_effective_disposition_buckets_late_leads_and_precedence(tmp_path: Path) -> None:
    workflows, cipher, conn, _ = _setup_pipeline(tmp_path)
    try:
        drop1 = tmp_path / "drop_1"
        drop2 = tmp_path / "drop_2"

        # Drop 1 has leads L1..L4; L5 arrives ONLY in Drop 2 (late-arriving lead!)
        _write_csv(
            drop1 / "leads" / "motor_renewal" / "batch_001.csv",
            [
                ["customer_name", "phone", "policy_number"],
                ["L1", "9876500001", "MOT/001"],  # yet_to_reach_out
                ["L2", "9876500002", "MOT/002"],  # all calls telephony_failed
                ["L3", "9876500003", "MOT/003"],  # call 1 had disposition, call 2 no_answer -> current_disposition from call 1!
                ["L4", "9876500004", "MOT/004"],  # override out of order, unknown disposition ignored, completed + ever_success
            ],
        )

        # Notice: L5's call events + override appear in drop_1 BEFORE L5 arrives in drop_2!
        # Also test orphan + unknown disposition precedence, duplicate, malformed, empty line, and connected without initiated!
        _write_jsonl(
            drop1 / "events" / "events_0001.jsonl",
            [
                "",  # empty line -> ignored, not counted in events_total
                "{broken json",  # malformed #1
                {"event_type": "call.initiated"},  # malformed #2 (no event_id)
                # L2: two telephony_failed calls
                {"event_id": "e1", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c_l2_1", "occurred_at": "2026-10-01T10:00:00+05:30", "phone": "9876500002", "payload": {}},
                {"event_id": "e2", "event_type": "call.telephony_failed", "workflow_id": "motor_renewal", "call_id": "c_l2_1", "occurred_at": "2026-10-01T10:00:05+05:30", "phone": "9876500002", "payload": {"reason": "sip_503"}},
                # L3: call 1 connected + callback_requested; call 2 no_answer (no disposition)
                {"event_id": "e3", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c_l3_1", "occurred_at": "2026-10-01T11:00:00+05:30", "phone": "9876500003", "payload": {}},
                {"event_id": "e4", "event_type": "call.connected", "workflow_id": "motor_renewal", "call_id": "c_l3_1", "occurred_at": "2026-10-01T11:00:10+05:30", "phone": "9876500003", "payload": {}},
                {"event_id": "e5", "event_type": "disposition.set", "workflow_id": "motor_renewal", "call_id": "c_l3_1", "occurred_at": "2026-10-01T11:02:00+05:30", "phone": "9876500003", "payload": {"disposition_code": "callback_requested"}},
                {"event_id": "e6", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c_l3_2", "occurred_at": "2026-10-02T12:00:00+05:30", "phone": "9876500003", "payload": {}},
                {"event_id": "e7", "event_type": "call.ended", "workflow_id": "motor_renewal", "call_id": "c_l3_2", "occurred_at": "2026-10-02T12:00:35+05:30", "phone": "9876500003", "payload": {"duration_sec": 0, "end_reason": "no_answer"}},
                # L4: override arrives BEFORE disposition.set in file; earlier override vs later override; unknown override ignored
                {"event_id": "e8", "event_type": "disposition.override", "workflow_id": "motor_renewal", "call_id": "c_l4_1", "occurred_at": "2026-10-02T09:00:00+05:30", "payload": {"disposition_code": "renewed", "overridden_by": "sup1", "reason": "qa"}},
                {"event_id": "e9", "event_type": "disposition.override", "workflow_id": "motor_renewal", "call_id": "c_l4_1", "occurred_at": "2026-10-02T08:00:00+05:30", "payload": {"disposition_code": "not_interested", "overridden_by": "sup2", "reason": "qa"}},
                {"event_id": "e10", "event_type": "disposition.override", "workflow_id": "motor_renewal", "call_id": "c_l4_1", "occurred_at": "2026-10-03T08:00:00+05:30", "payload": {"disposition_code": "BOGUS_CODE", "overridden_by": "sup3", "reason": "typo"}},
                {"event_id": "e11", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c_l4_1", "occurred_at": "2026-10-01T18:40:00Z", "phone": "+919876500004", "payload": {}}, # 2026-10-02T00:10:00+05:30 IST!
                {"event_id": "e12", "event_type": "call.connected", "workflow_id": "motor_renewal", "call_id": "c_l4_1", "occurred_at": "2026-10-01T18:40:10Z", "phone": "09876500004", "payload": {}},
                {"event_id": "e13", "event_type": "disposition.set", "workflow_id": "motor_renewal", "call_id": "c_l4_1", "occurred_at": "2026-10-01T18:42:00Z", "phone": "9876500004", "payload": {"disposition_code": "payment_link_sent"}},
                # L1 orphan-ish call.connected WITHOUT call.initiated -> must NOT count as a call!
                {"event_id": "e14", "event_type": "call.connected", "workflow_id": "motor_renewal", "call_id": "c_l1_no_init", "occurred_at": "2026-10-01 10:00:00", "phone": "9876500001", "payload": {}},
                # Duplicate event_id within drop_1
                {"event_id": "e1", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c_l2_1", "occurred_at": "2026-10-01T10:00:00+05:30", "phone": "9876500002", "payload": {}},
                # Late-arriving lead L5 events in drop_1 (orphan after drop_1, linked after drop_2!)
                {"event_id": "e15", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c_l5_1", "occurred_at": "2026-10-03 15:00:00", "phone": "9876500005", "payload": {}},
                {"event_id": "e16", "event_type": "call.connected", "workflow_id": "motor_renewal", "call_id": "c_l5_1", "occurred_at": "2026-10-03 15:00:10", "phone": "9876500005", "payload": {}},
                {"event_id": "e17", "event_type": "disposition.override", "workflow_id": "motor_renewal", "call_id": "c_l5_1", "occurred_at": "2026-10-04 10:00:00", "payload": {"disposition_code": "not_interested", "overridden_by": "s1", "reason": "qa"}},
                # True orphan with unknown disposition -> must count ONLY in orphan_events, NOT unknown_disposition_events!
                {"event_id": "e18", "event_type": "disposition.set", "workflow_id": "motor_renewal", "call_id": "c_orphan", "occurred_at": "2026-10-03 16:00:00", "phone": "9876599999", "payload": {"disposition_code": "RENEWD"}},
            ],
        )

        ingest_paths(conn, [drop1], workflows, cipher)
        dq1 = generate_reports(conn, tmp_path / "out1", cipher)
        assert dq1["events_total"] == 21
        assert dq1["malformed_events"] == 2
        assert dq1["duplicate_events_dropped"] == 1
        assert dq1["orphan_events"] == 4  # e15, e16, e17 (L5 not yet arrived) + e18
        assert dq1["unknown_disposition_events"] == 1  # only e10 (e18 is orphan first!)

        # Now L5 arrives in drop_2!
        _write_csv(
            drop2 / "leads" / "motor_renewal" / "batch_003.csv",
            [
                ["customer_name", "phone", "policy_number"],
                ["L5", "9876500005", "MOT/005"],
            ],
        )
        ingest_paths(conn, [drop2], workflows, cipher)
        dq2 = generate_reports(conn, tmp_path / "out2", cipher)

        assert dq2["orphan_events"] == 1  # only e18 remains an orphan!
        assert dq2["unknown_disposition_events"] == 1

        leads_rep = {r["external_ref"]: r for r in _read_csv(tmp_path / "out2" / "lead_report.csv")}
        assert leads_rep["MOT/001"]["bucket"] == "yet_to_reach_out"
        assert leads_rep["MOT/001"]["total_calls"] == "0"

        assert leads_rep["MOT/002"]["bucket"] == "in_progress_telephony_failed"
        assert leads_rep["MOT/002"]["total_calls"] == "1"
        assert leads_rep["MOT/002"]["connected_calls"] == "0"

        assert leads_rep["MOT/003"]["bucket"] == "in_progress_dialed"
        assert leads_rep["MOT/003"]["total_calls"] == "2"
        assert leads_rep["MOT/003"]["connected_calls"] == "1"
        assert leads_rep["MOT/003"]["current_disposition"] == "callback_requested"

        assert leads_rep["MOT/004"]["bucket"] == "completed"
        assert leads_rep["MOT/004"]["last_call_at"] == "2026-10-02T00:10:00+05:30"
        assert leads_rep["MOT/004"]["current_disposition"] == "renewed"
        assert leads_rep["MOT/004"]["ever_success"] == "true"

        assert leads_rep["MOT/005"]["bucket"] == "completed"
        assert leads_rep["MOT/005"]["current_disposition"] == "not_interested"
        assert leads_rep["MOT/005"]["ever_success"] == "false"

        # Verify daily_overview used IST date 2026-10-02 for MOT/004 (18:40Z -> 00:10+05:30)
        daily = {(r["workflow_id"], r["date"]): r for r in _read_csv(tmp_path / "out2" / "daily_overview.csv")}
        assert daily[("motor_renewal", "2026-10-02")]["total_calls"] == "2"  # c_l3_2 + c_l4_1
        assert daily[("motor_renewal", "2026-10-02")]["connected_calls"] == "1"
        assert daily[("motor_renewal", "2026-10-02")]["connectivity_rate"] == "0.5000"
        assert daily[("motor_renewal", "2026-10-02")]["success_leads"] == "1"
    finally:
        conn.close()


def test_pii_at_rest_and_append_only_ledger(tmp_path: Path) -> None:
    workflows, cipher, conn, db_path = _setup_pipeline(tmp_path)
    try:
        drop = tmp_path / "drop_1"
        _write_csv(
            drop / "leads" / "motor_renewal" / "batch_001.csv",
            [
                ["customer_name", "phone", "policy_number"],
                ["SecretCustomerName", "9876543210", "MOT/999"],
                ["SecretRejectedName", "badphone", "MOT/998"],
            ],
        )
        _write_jsonl(
            drop / "events" / "events_0001.jsonl",
            [
                {"event_id": "evt_sec", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c_sec", "occurred_at": "2026-10-01T10:00:00+05:30", "phone": "9876543210", "payload": {}},
            ],
        )
        ingest_paths(conn, [drop], workflows, cipher)

        # Append-only trigger check
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute("UPDATE interaction_ledger SET event_type = 'x'")
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute("DELETE FROM interaction_ledger")
    finally:
        conn.close()

    # Check raw SQLite file bytes contain NO plaintext PII
    raw_bytes = db_path.read_bytes()
    assert b"SecretCustomerName" not in raw_bytes
    assert b"SecretRejectedName" not in raw_bytes
    assert b"9876543210" not in raw_bytes


def test_idempotent_reingest(tmp_path: Path) -> None:
    workflows, cipher, conn, _ = _setup_pipeline(tmp_path)
    try:
        drop = tmp_path / "drop_1"
        _write_csv(
            drop / "leads" / "motor_renewal" / "batch_001.csv",
            [
                ["customer_name", "phone", "policy_number"],
                ["Aarav", "9876543210", "MOT/1"],
                ["Aarav", "9876543210", "MOT/1"],
                ["Bad", "123", "MOT/2"],
            ],
        )
        _write_jsonl(
            drop / "events" / "events_0001.jsonl",
            [
                {"event_id": "e1", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c1", "occurred_at": "2026-10-01T10:00:00+05:30", "phone": "9876543210", "payload": {}},
            ],
        )

        ingest_paths(conn, [drop], workflows, cipher)
        out1 = tmp_path / "out1"
        generate_reports(conn, out1, cipher)

        # Re-ingest same drop
        ingest_paths(conn, [drop], workflows, cipher)
        out2 = tmp_path / "out2"
        generate_reports(conn, out2, cipher)

        for fname in ("batch_summary.csv", "daily_overview.csv", "lead_report.csv", "rejections.csv", "data_quality.json"):
            assert (out1 / fname).read_bytes() == (out2 / fname).read_bytes()
    finally:
        conn.close()


def test_empty_db_reports_and_numeric_rejection_sort(tmp_path: Path) -> None:
    workflows, cipher, conn, _ = _setup_pipeline(tmp_path)
    try:
        # Empty DB report should succeed cleanly with zero counts
        empty_out = tmp_path / "empty_out"
        dq_empty = generate_reports(conn, empty_out, cipher)
        assert dq_empty["lead_rows_total"] == 0
        assert dq_empty["events_total"] == 0
        assert _read_csv(empty_out / "batch_summary.csv") == []
        assert _read_csv(empty_out / "lead_report.csv") == []

        # Verify numeric row_number ordering (row 2 before row 10) + cross-drop event dedupe
        drop1 = tmp_path / "drop_1"
        drop2 = tmp_path / "drop_2"
        rows = [["customer_name", "phone", "policy_number"]]
        for i in range(1, 12):
            if i in (2, 10):
                rows.append(["Bad", "123", f"MOT/{i}"])
            else:
                rows.append([f"User{i}", f"98765432{i:02d}", f"MOT/{i}"])
        _write_csv(drop1 / "leads" / "motor_renewal" / "batch_001.csv", rows)
        _write_jsonl(
            drop1 / "events" / "events_0001.jsonl",
            [{"event_id": "evt_dup", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c1", "occurred_at": "2026-10-01T10:00:00Z", "phone": "9876543201", "payload": {}}],
        )
        _write_jsonl(
            drop2 / "events" / "events_1001.jsonl",
            [{"event_id": "evt_dup", "event_type": "call.initiated", "workflow_id": "motor_renewal", "call_id": "c1", "occurred_at": "2026-10-01T10:00:00Z", "phone": "9876543201", "payload": {}}],
        )

        ingest_paths(conn, [drop1], workflows, cipher)
        ingest_paths(conn, [drop2], workflows, cipher)
        out = tmp_path / "out"
        dq = generate_reports(conn, out, cipher)

        rejs = _read_csv(out / "rejections.csv")
        assert [int(r["row_number"]) for r in rejs] == [2, 10]
        assert dq["events_total"] == 2
        assert dq["duplicate_events_dropped"] == 1
    finally:
        conn.close()
