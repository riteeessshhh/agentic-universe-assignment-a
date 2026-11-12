# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
Antigravity (AI coding assistant).

## What I used them for
- Exploring the repository, schema files, and `generate_data.py` edge cases.
- Scaffolding the modular Python package under `solution/` (`normalizers.py`, `crypto.py`, `db.py`, `ingest.py`, `report.py`, `cli.py`).
- Generating unit and end-to-end `pytest` suites covering normalizers, SQL aggregation edge cases, PII encryption, and idempotency.
- Profiling `--scale large` ingestion with `cProfile` to optimize throughput.

## What I wrote or decided myself
- Chose SQLite (WAL mode) with SQL-based report aggregation (via materialized `TEMP TABLE`s and `ROW_NUMBER()` window functions) and database-level append-only triggers on `interaction_ledger`.
- Designed the PII-at-rest scheme combining AES-256-GCM encryption for `phone`/`customer_name` with HMAC-SHA256 blind indexing (`phone_hmac`) so lead deduplication and event linking work in SQL without storing plaintext PII.
- Decided to store un-defaulted optional attributes in `attributes_json` and resolve schema defaults after all merges so incremental drops always match all-at-once ingestion.

## Where the AI was wrong or unhelpful
- The initial ingestion implementation committed a separate SQLite transaction for every `.jsonl` file (65+ separate commits per drop) and ran per-row `SELECT` queries for duplicate lead updates, causing `--scale large` to take ~171 seconds (missing the 2-minute stretch target). Profiling with `cProfile` revealed that commit/query overhead and uncached HMAC setup were the main bottlenecks; batching transactions per drop, chunking `IN (...)` lookups, and sharing a bounded phone HMAC cache cut total runtime from 171s to ~43s.
- A test assertion in `test_effective_disposition_buckets_late_leads_and_precedence` initially miscounted `events_total` (20 vs 21 lines), caught immediately on the first `pytest` run.

## How I verified AI-generated code
- Ran `pytest -v` (61 unit, integration, and end-to-end tests, including unseen seed `--seed 99`).
- Verified byte-identical outputs across all 5 report files between incremental (`drop_1` then `drop_2`), re-ingested (idempotent), and all-at-once runs.
- Inspected raw `.sqlite` database file bytes in tests to confirm zero plaintext PII (`customer_name`, `phone`) is stored at rest, and verified SQLite `UPDATE`/`DELETE` triggers reject mutations on `interaction_ledger`.
- Benchmarked full ingestion and reporting on `--scale large` (~144k lead rows, ~632k event lines).
