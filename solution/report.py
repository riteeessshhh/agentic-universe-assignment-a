"""SQL-backed report generation matching the Assignment A output contract."""
from __future__ import annotations

import csv
import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path

from .crypto import PIICipher
from .normalizers import mask_phone


def _drop_temp_summaries(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS temp.tmp_lead_summary;")
    conn.execute("DROP TABLE IF EXISTS temp.tmp_call_summary;")
    conn.execute("DROP TABLE IF EXISTS temp.tmp_valid_calls;")


def _build_temp_summaries(conn: sqlite3.Connection) -> None:
    _drop_temp_summaries(conn)

    conn.execute(
        """
        CREATE TEMP TABLE tmp_valid_calls AS
        WITH ranked_init AS (
            SELECT
                e.workflow_id,
                e.call_id,
                l.lead_id,
                e.occurred_at_ist AS initiated_at_ist,
                e.occurred_at_us AS initiated_at_us,
                e.ist_date AS call_date,
                ROW_NUMBER() OVER (
                    PARTITION BY e.workflow_id, e.call_id
                    ORDER BY e.occurred_at_us ASC, e.ledger_seq ASC
                ) AS rn
            FROM interaction_ledger e
            JOIN leads l
              ON e.workflow_id = l.workflow_id
             AND e.phone_hmac = l.phone_hmac
            WHERE e.event_type = 'call.initiated'
              AND e.phone_hmac IS NOT NULL
        )
        SELECT workflow_id, call_id, lead_id, initiated_at_ist, initiated_at_us, call_date
        FROM ranked_init
        WHERE rn = 1;
        """
    )
    conn.execute("CREATE INDEX temp.idx_tvc_wf_call ON tmp_valid_calls(workflow_id, call_id);")

    conn.execute(
        """
        CREATE TEMP TABLE tmp_call_summary AS
        WITH call_flags AS (
            SELECT
                vc.workflow_id,
                vc.call_id,
                vc.lead_id,
                vc.initiated_at_ist,
                vc.initiated_at_us,
                vc.call_date,
                MAX(CASE WHEN e.event_type = 'call.connected' THEN 1 ELSE 0 END) AS is_connected,
                MAX(CASE WHEN e.event_type = 'call.telephony_failed' THEN 1 ELSE 0 END) AS is_telephony_failed
            FROM tmp_valid_calls vc
            JOIN interaction_ledger e
              ON e.workflow_id = vc.workflow_id
             AND e.call_id = vc.call_id
            GROUP BY vc.workflow_id, vc.call_id, vc.lead_id, vc.initiated_at_ist, vc.initiated_at_us, vc.call_date
        ),
        ranked_disp AS (
            SELECT
                e.workflow_id,
                e.call_id,
                e.disposition_code,
                d.is_terminal,
                d.is_success,
                ROW_NUMBER() OVER (
                    PARTITION BY e.workflow_id, e.call_id
                    ORDER BY
                        CASE WHEN e.event_type = 'disposition.override' THEN 1 ELSE 0 END DESC,
                        e.occurred_at_us DESC,
                        e.ledger_seq DESC
                ) AS rn
            FROM interaction_ledger e
            JOIN dispositions d
              ON d.workflow_id = e.workflow_id
             AND d.code = e.disposition_code
            WHERE e.event_type IN ('disposition.override', 'disposition.set')
        )
        SELECT
            cf.workflow_id,
            cf.call_id,
            cf.lead_id,
            cf.initiated_at_ist,
            cf.initiated_at_us,
            cf.call_date,
            cf.is_connected,
            cf.is_telephony_failed,
            rd.disposition_code AS effective_disposition,
            COALESCE(rd.is_terminal, 0) AS is_terminal,
            COALESCE(rd.is_success, 0) AS is_success
        FROM call_flags cf
        LEFT JOIN ranked_disp rd
          ON rd.workflow_id = cf.workflow_id
         AND rd.call_id = cf.call_id
         AND rd.rn = 1;
        """
    )
    conn.execute("CREATE INDEX temp.idx_tcs_lead ON tmp_call_summary(lead_id);")
    conn.execute("CREATE INDEX temp.idx_tcs_wf_date ON tmp_call_summary(workflow_id, call_date);")

    conn.execute(
        """
        CREATE TEMP TABLE tmp_lead_summary AS
        WITH agg AS (
            SELECT
                lead_id,
                COUNT(*) AS total_calls,
                SUM(is_connected) AS connected_calls,
                SUM(is_telephony_failed) AS telephony_failed_calls,
                MAX(is_terminal) AS has_terminal,
                MAX(is_success) AS ever_success
            FROM tmp_call_summary
            GROUP BY lead_id
        ),
        latest_call AS (
            SELECT
                lead_id,
                initiated_at_ist AS last_call_at,
                ROW_NUMBER() OVER (
                    PARTITION BY lead_id
                    ORDER BY initiated_at_us DESC, call_id DESC
                ) AS rn
            FROM tmp_call_summary
        ),
        latest_disp AS (
            SELECT
                lead_id,
                effective_disposition AS current_disposition,
                ROW_NUMBER() OVER (
                    PARTITION BY lead_id
                    ORDER BY initiated_at_us DESC, call_id DESC
                ) AS rn
            FROM tmp_call_summary
            WHERE effective_disposition IS NOT NULL
        )
        SELECT
            l.lead_id,
            l.workflow_id,
            l.external_ref,
            l.batch_id,
            l.phone_enc,
            CASE
                WHEN COALESCE(a.total_calls, 0) = 0 THEN 'yet_to_reach_out'
                WHEN COALESCE(a.has_terminal, 0) = 1 THEN 'completed'
                WHEN a.telephony_failed_calls = a.total_calls THEN 'in_progress_telephony_failed'
                ELSE 'in_progress_dialed'
            END AS bucket,
            COALESCE(a.total_calls, 0) AS total_calls,
            COALESCE(a.connected_calls, 0) AS connected_calls,
            COALESCE(lc.last_call_at, '') AS last_call_at,
            COALESCE(ld.current_disposition, '') AS current_disposition,
            COALESCE(a.ever_success, 0) AS ever_success
        FROM leads l
        LEFT JOIN agg a ON a.lead_id = l.lead_id
        LEFT JOIN latest_call lc ON lc.lead_id = l.lead_id AND lc.rn = 1
        LEFT JOIN latest_disp ld ON ld.lead_id = l.lead_id AND ld.rn = 1;
        """
    )


def _write_csv(path: Path, header: list[str], rows: Iterable[list[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def _write_batch_summary(conn: sqlite3.Connection, path: Path) -> None:
    rows = conn.execute(
        """
        SELECT
            workflow_id,
            batch_id,
            COUNT(*) AS total_leads,
            SUM(CASE WHEN bucket = 'yet_to_reach_out' THEN 1 ELSE 0 END) AS yet_to_reach_out,
            SUM(CASE WHEN bucket = 'in_progress_telephony_failed' THEN 1 ELSE 0 END) AS in_progress_telephony_failed,
            SUM(CASE WHEN bucket = 'in_progress_dialed' THEN 1 ELSE 0 END) AS in_progress_dialed,
            SUM(CASE WHEN bucket = 'completed' THEN 1 ELSE 0 END) AS completed
        FROM tmp_lead_summary
        GROUP BY workflow_id, batch_id
        ORDER BY workflow_id ASC, batch_id ASC;
        """
    )
    _write_csv(
        path,
        [
            "workflow_id",
            "batch_id",
            "total_leads",
            "yet_to_reach_out",
            "in_progress_telephony_failed",
            "in_progress_dialed",
            "completed",
        ],
        (
            [
                r["workflow_id"],
                r["batch_id"],
                r["total_leads"],
                r["yet_to_reach_out"],
                r["in_progress_telephony_failed"],
                r["in_progress_dialed"],
                r["completed"],
            ]
            for r in rows
        ),
    )


def _write_daily_overview(conn: sqlite3.Connection, path: Path) -> None:
    rows = conn.execute(
        """
        SELECT
            workflow_id,
            call_date AS date,
            COUNT(*) AS total_calls,
            SUM(is_connected) AS connected_calls,
            COUNT(DISTINCT CASE WHEN is_success = 1 THEN lead_id END) AS success_leads
        FROM tmp_call_summary
        GROUP BY workflow_id, call_date
        ORDER BY workflow_id ASC, call_date ASC;
        """
    )
    _write_csv(
        path,
        ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
        (
            [
                r["workflow_id"],
                r["date"],
                r["total_calls"],
                r["connected_calls"],
                f"{r['connected_calls'] / r['total_calls']:.4f}" if r["total_calls"] > 0 else "0.0000",
                r["success_leads"],
            ]
            for r in rows
        ),
    )


def _write_lead_report(conn: sqlite3.Connection, path: Path, cipher: PIICipher) -> None:
    rows = conn.execute(
        """
        SELECT
            workflow_id,
            external_ref,
            batch_id,
            phone_enc,
            bucket,
            total_calls,
            connected_calls,
            last_call_at,
            current_disposition,
            ever_success
        FROM tmp_lead_summary
        ORDER BY workflow_id ASC, external_ref ASC;
        """
    )
    _write_csv(
        path,
        [
            "workflow_id",
            "external_ref",
            "batch_id",
            "phone_masked",
            "bucket",
            "total_calls",
            "connected_calls",
            "last_call_at",
            "current_disposition",
            "ever_success",
        ],
        (
            [
                r["workflow_id"],
                r["external_ref"],
                r["batch_id"],
                mask_phone(cipher.decrypt_text(r["phone_enc"])),
                r["bucket"],
                r["total_calls"],
                r["connected_calls"],
                r["last_call_at"],
                r["current_disposition"],
                "true" if r["ever_success"] else "false",
            ]
            for r in rows
        ),
    )


def _write_rejections(conn: sqlite3.Connection, path: Path) -> None:
    rows = conn.execute(
        """
        SELECT workflow_id, batch_id, row_number, reason
        FROM lead_rejections
        ORDER BY workflow_id ASC, batch_id ASC, row_number ASC;
        """
    )
    _write_csv(
        path,
        ["workflow_id", "batch_id", "row_number", "reason"],
        ([r["workflow_id"], r["batch_id"], r["row_number"], r["reason"]] for r in rows),
    )


def _build_data_quality(conn: sqlite3.Connection) -> dict:
    metrics_row = conn.execute(
        """
        SELECT
            lead_rows_total,
            duplicate_lead_rows_merged,
            field_warnings,
            events_total,
            malformed_events,
            duplicate_events_dropped
        FROM ingestion_metrics
        WHERE id = 1;
        """
    ).fetchone()

    rej_rows = conn.execute(
        """
        SELECT reason, COUNT(*) AS cnt
        FROM lead_rejections
        GROUP BY reason
        ORDER BY reason ASC;
        """
    ).fetchall()
    rejections_by_reason = {r["reason"]: int(r["cnt"]) for r in rej_rows}

    event_quality = conn.execute(
        """
        WITH call_lead AS (
            SELECT e.workflow_id, e.call_id, MIN(l.lead_id) AS lead_id
            FROM interaction_ledger e
            JOIN leads l
              ON e.workflow_id = l.workflow_id
             AND e.phone_hmac = l.phone_hmac
            WHERE e.phone_hmac IS NOT NULL
            GROUP BY e.workflow_id, e.call_id
        ),
        classified AS (
            SELECT
                CASE
                    WHEN e.event_type = 'disposition.override' THEN
                        CASE WHEN cl.lead_id IS NOT NULL THEN 1 ELSE 0 END
                    ELSE
                        CASE WHEN l.lead_id IS NOT NULL THEN 1 ELSE 0 END
                END AS is_linked,
                CASE
                    WHEN SUBSTR(e.event_type, 1, 12) = 'disposition.'
                         AND d.code IS NULL THEN 1
                    ELSE 0
                END AS is_unknown_disp
            FROM interaction_ledger e
            LEFT JOIN leads l
              ON e.event_type != 'disposition.override'
             AND e.workflow_id = l.workflow_id
             AND e.phone_hmac = l.phone_hmac
            LEFT JOIN call_lead cl
              ON e.event_type = 'disposition.override'
             AND e.workflow_id = cl.workflow_id
             AND e.call_id = cl.call_id
            LEFT JOIN dispositions d
              ON SUBSTR(e.event_type, 1, 12) = 'disposition.'
             AND e.workflow_id = d.workflow_id
             AND e.disposition_code = d.code
        )
        SELECT
            COALESCE(SUM(CASE WHEN is_linked = 0 THEN 1 ELSE 0 END), 0) AS orphan_events,
            COALESCE(SUM(CASE WHEN is_linked = 1 AND is_unknown_disp = 1 THEN 1 ELSE 0 END), 0) AS unknown_disposition_events
        FROM classified;
        """
    ).fetchone()

    return {
        "lead_rows_total": int(metrics_row["lead_rows_total"]),
        "lead_rows_rejected": sum(rejections_by_reason.values()),
        "rejections_by_reason": rejections_by_reason,
        "duplicate_lead_rows_merged": int(metrics_row["duplicate_lead_rows_merged"]),
        "field_warnings": int(metrics_row["field_warnings"]),
        "events_total": int(metrics_row["events_total"]),
        "malformed_events": int(metrics_row["malformed_events"]),
        "duplicate_events_dropped": int(metrics_row["duplicate_events_dropped"]),
        "orphan_events": int(event_quality["orphan_events"]),
        "unknown_disposition_events": int(event_quality["unknown_disposition_events"]),
    }


def generate_reports(
    conn: sqlite3.Connection,
    out_dir: str | Path,
    cipher: PIICipher,
) -> dict:
    """Compute aggregations in SQL and write the 5 required report files into out_dir."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    _build_temp_summaries(conn)
    try:
        _write_batch_summary(conn, out / "batch_summary.csv")
        _write_daily_overview(conn, out / "daily_overview.csv")
        _write_lead_report(conn, out / "lead_report.csv", cipher)
        _write_rejections(conn, out / "rejections.csv")
        data_quality = _build_data_quality(conn)
        (out / "data_quality.json").write_text(
            json.dumps(data_quality, indent=2) + "\n",
            encoding="utf-8",
        )
    finally:
        _drop_temp_summaries(conn)

    return data_quality
