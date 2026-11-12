"""CLI entry point for the Interaction Ledger & Reporting Pipeline."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .config import load_schemas
from .crypto import PIICipher, load_or_create_master_key
from .db import connect_db, init_db
from .ingest import ingest_paths
from .report import generate_reports

DEFAULT_DB_PATH = "ledger.sqlite"


def _default_schemas_dir() -> Path:
    cwd_candidate = Path("assignment-a-data") / "schemas"
    if cwd_candidate.is_dir():
        return cwd_candidate
    repo_candidate = Path(__file__).resolve().parent.parent / "assignment-a-data" / "schemas"
    return repo_candidate


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m solution",
        description="Agentic Universe Interaction Ledger & Reporting Pipeline",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("AU_DB_PATH", DEFAULT_DB_PATH),
        help=f"SQLite database path (default: $AU_DB_PATH or {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--schemas",
        default=None,
        help="Path to workflow schemas & dispositions directory (default: assignment-a-data/schemas)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="Ingest one or more drop directories")
    p_ingest.add_argument("paths", nargs="+", help="Drop directory path(s) (e.g. data/drop_1)")

    p_report = sub.add_parser("report", help="Generate the 5 report files")
    p_report.add_argument("--out", required=True, help="Output directory for report files")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    schemas_dir = Path(args.schemas) if args.schemas else _default_schemas_dir()
    workflows, dispositions = load_schemas(schemas_dir)
    master_key = load_or_create_master_key(args.db)
    cipher = PIICipher(master_key)

    conn = connect_db(args.db)
    try:
        init_db(conn, dispositions)
        if args.command == "ingest":
            stats = ingest_paths(conn, args.paths, workflows, cipher)
            print(f"Ingested {stats['drops']} drop(s), checked {stats['files_checked']} file(s).")
        elif args.command == "report":
            dq = generate_reports(conn, args.out, cipher)
            print(
                f"Reports written to {args.out}: "
                f"leads={dq['lead_rows_total']} (rejected={dq['lead_rows_rejected']}, "
                f"merged={dq['duplicate_lead_rows_merged']}), "
                f"events={dq['events_total']} (orphans={dq['orphan_events']})"
            )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

