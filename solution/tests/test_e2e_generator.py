"""End-to-end tests using generate_data.py on an unseen seed."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from solution.cli import main as cli_main

REPO_ROOT = Path(__file__).resolve().parents[2]
GEN_SCRIPT = REPO_ROOT / "assignment-a-data" / "tools" / "generate_data.py"

REPORT_FILES = (
    "batch_summary.csv",
    "daily_overview.csv",
    "lead_report.csv",
    "rejections.csv",
    "data_quality.json",
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_e2e_incremental_equals_all_at_once_and_idempotent(tmp_path: Path) -> None:
    data_dir = tmp_path / "synthetic_data"
    subprocess.run(
        [sys.executable, str(GEN_SCRIPT), "--seed", "99", "--scale", "small", "--out", str(data_dir)],
        check=True,
    )

    # Path 1: incremental drop_1 then drop_2, then re-ingest drop_1 & drop_2
    db_inc = tmp_path / "inc.sqlite"
    out_inc = tmp_path / "reports_inc"
    cli_main(["--db", str(db_inc), "ingest", str(data_dir / "drop_1")])
    cli_main(["--db", str(db_inc), "ingest", str(data_dir / "drop_2")])
    cli_main(["--db", str(db_inc), "report", "--out", str(out_inc)])

    # Re-ingest both drops (idempotency check)
    out_reingest = tmp_path / "reports_reingest"
    cli_main(["--db", str(db_inc), "ingest", str(data_dir / "drop_1"), str(data_dir / "drop_2")])
    cli_main(["--db", str(db_inc), "report", "--out", str(out_reingest)])

    # Path 2: ingest everything at once
    db_all = tmp_path / "all.sqlite"
    out_all = tmp_path / "reports_all"
    cli_main(["--db", str(db_all), "ingest", str(data_dir)])
    cli_main(["--db", str(db_all), "report", "--out", str(out_all)])

    for fname in REPORT_FILES:
        b_inc = (out_inc / fname).read_bytes()
        b_re = (out_reingest / fname).read_bytes()
        b_all = (out_all / fname).read_bytes()
        assert b_inc == b_re, f"{fname} changed on re-ingest"
        assert b_inc == b_all, f"{fname} differed between incremental and all-at-once"

    # Cross-report invariants
    dq = json.loads((out_inc / "data_quality.json").read_text(encoding="utf-8"))
    batch_rows = _read_csv(out_inc / "batch_summary.csv")
    daily_rows = _read_csv(out_inc / "daily_overview.csv")
    lead_rows = _read_csv(out_inc / "lead_report.csv")
    rej_rows = _read_csv(out_inc / "rejections.csv")

    assert len(rej_rows) == dq["lead_rows_rejected"]
    assert sum(dq["rejections_by_reason"].values()) == dq["lead_rows_rejected"]
    assert len(lead_rows) + dq["lead_rows_rejected"] + dq["duplicate_lead_rows_merged"] == dq["lead_rows_total"]

    assert sum(int(r["total_leads"]) for r in batch_rows) == len(lead_rows)
    for r in batch_rows:
        bucket_sum = (
            int(r["yet_to_reach_out"])
            + int(r["in_progress_telephony_failed"])
            + int(r["in_progress_dialed"])
            + int(r["completed"])
        )
        assert bucket_sum == int(r["total_leads"])

    assert sum(int(r["total_calls"]) for r in daily_rows) == sum(int(r["total_calls"]) for r in lead_rows)
    assert sum(int(r["connected_calls"]) for r in daily_rows) == sum(int(r["connected_calls"]) for r in lead_rows)

