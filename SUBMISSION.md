# Submission

## Candidate
- **Name:** Ritesh Sahablal Pal
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** ~6h

## How to run
```bash
# 1. Create virtual environment and install dependencies
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Generate synthetic data
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

# 3. Ingest drops incrementally and generate reports
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/

# (Optional) Specify custom DB path or schemas directory:
# python -m solution --db custom.sqlite --schemas assignment-a-data/schemas ingest data/drop_1

# 4. Run tests
pytest -v
```

## Design

### Architecture
The pipeline is organized as a modular Python package under `solution/`:
- `config.py`: Dynamically loads workflow JSON schemas and `dispositions.csv`.
- `normalizers.py`: Pure, stateless normalizers for headers, Indian mobile numbers, day-first dates, currency-prefixed decimals, enums, and IST timestamps.
- `crypto.py`: Protects PII at rest using **AES-256-GCM** authenticated encryption and deterministic **HMAC-SHA256** blind indexing (with separate domain-derived subkeys from a 32-byte master key).
- `db.py`: SQLite schema initialization (WAL mode, `NORMAL` sync, append-only SQLite triggers) and disposition catalogue syncing.
- `ingest.py`: Idempotent, transactional drop ingestion for lead CSVs and event JSONLs.
- `report.py`: Performs all report aggregations in SQL (via temporary materialized tables and window functions) and writes the five output files.
- `cli.py` / `__main__.py`: CLI exposing `ingest` and `report`.

### Database Schema (SQLite)
```
ingested_files (id PK, file_key UNIQUE, kind, workflow_id, filename, sha256, ingested_at)
ingestion_metrics (id PK=1, lead_rows_total, duplicate_lead_rows_merged, field_warnings,
                   events_total, malformed_events, duplicate_events_dropped)
dispositions (workflow_id, code, label, is_terminal, is_success)  -- PK (workflow_id, code)

leads (lead_id PK, workflow_id, phone_hmac BLOB, phone_enc BLOB, pii_enc BLOB,
       external_ref, batch_id, attributes_json)                   -- UNIQUE (workflow_id, phone_hmac)

lead_rejections (id PK, workflow_id, batch_id, row_number, reason)

interaction_ledger (ledger_seq PK, event_id UNIQUE, event_type, workflow_id, call_id,
                    occurred_at_ist, occurred_at_us, ist_date, phone_hmac BLOB,
                    disposition_code, payload_json)
```

**Why this schema:**
1. **Append-only ledger (`interaction_ledger`)**: Every valid, deduplicated event—including events whose lead has not arrived yet—is inserted once and never mutated. `BEFORE UPDATE` and `BEFORE DELETE` triggers enforce append-only immutability at the database engine level.
2. **PII protection at rest**: `customer_name` (and any `"pii": true` field) and canonical `phone` are encrypted with AES-256-GCM (`pii_enc`, `phone_enc`) and never stored in plaintext. Matching leads across batches and linking events to leads uses `phone_hmac` (keyed HMAC-SHA256 blind index on `(workflow_id, phone_hmac)`), which allows SQL joins and uniqueness constraints without exposing plaintext numbers.
3. **Late-arriving leads & out-of-order overrides**: Because `interaction_ledger` stores all deduplicated events regardless of whether the lead exists yet, linking happens dynamically at `report` time via SQL joins (`leads` on `(workflow_id, phone_hmac)` and `disposition.override` via `(workflow_id, call_id)`). When `drop_2` introduces a lead whose calls arrived in `drop_1`, those events automatically resolve and drop out of `orphan_events`.
4. **Post-merge defaults**: `leads.attributes_json` stores only explicitly provided valid non-null field values, so later duplicate rows with blank/invalid optional fields never clobber valid earlier values. Schema defaults (such as `preferred_language -> en`) are applied when reading/materializing the lead profile after merging.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| SQLite (WAL mode) with `ledger.sqlite` default | Zero external dependencies for grading from a clean clone; fast bulk inserts and SQL window functions; explicitly allowed alongside Postgres. | PostgreSQL via `docker-compose.yml` (trivial to swap SQL dialect if concurrent writers are needed). |
| Key management via `AU_PII_KEY` env var with `<db>.key` fallback | Allows multi-command CLI runs (`ingest`, `ingest`, `report`) to work out-of-the-box while keeping the key outside the SQLite DB file; production would inject via KMS/Secrets Manager. | Hardcoded key (insecure) or requiring env var on every invocation (breaks simple CLI grading). |
| `ingested_files` keyed by `(kind, workflow_id, filename, sha256)` | Guarantees re-ingesting the exact same file is a no-op regardless of relative/absolute path, while still detecting a modified or different file with the same name. | Tracking only directory path (breaks if path changes or files are added). |
| One transaction per drop | Atomic drop ingestion (crash mid-drop rolls back cleanly) and avoids 60+ fsync/WAL commit stalls on Windows NTFS, cutting `--scale large` time by ~4x. | Per-file transaction (slower on many small `.jsonl` files) or per-row commit (far too slow). |
| Structural validation (`event_type`, `workflow_id`, `call_id`, parseable `occurred_at`) treated as malformed before duplicate check | Prevents corrupt event records from polluting the ledger or `seen_event_ids`. | Checking only valid JSON + `event_id` (risks crashing later during timestamp parsing). |
| `Phone` blind-index cache bounded at 300k entries in `ingest_paths` | Avoids redundant regex/HMAC work across repeated calls to the same phone number without unbounded memory growth. | Unbounded cache or no cache (~2x more CPU on 600k events). |

## Failure modes considered
- **Re-running `ingest` on already-ingested drops:** Checked against `ingested_files` (`file_key` with SHA-256); already-ingested files are skipped with zero metric or row changes.
- **Crash mid-ingest:** Each drop runs inside a single `BEGIN ... COMMIT` transaction; an exception or process kill rolls back all changes for that drop cleanly.
- **Dirty/malformed inputs:** Blank CSV rows skipped without incrementing `row_number`; UTF-8 BOM stripped (`utf-8-sig`); schema-order validation stops at first required error; invalid optional fields record `NULL` + `field_warnings`; malformed JSONL lines and missing `event_id`s counted and skipped.
- **Out-of-order & cross-drop events:** `disposition.override` arriving before `call.initiated` or in a different drop still links via `(workflow_id, call_id)` at report time; unknown disposition codes are ignored without overwriting valid earlier dispositions.
- **Orphan + unknown disposition precedence:** Checked in exact spec order (1. Malformed -> 2. Duplicate -> 3. Orphan -> 4. Unknown disposition), so an unlinked event with an unknown code counts only as `orphan_events`.
- **Known limitation (not handled):** Two separate `ingest` processes writing to the same SQLite DB concurrently would contend on SQLite's single-writer lock (acceptable for single-runner batch CLI; for concurrent workers, switch to Postgres). Also, if `AU_PII_KEY` is lost, encrypted PII in `leads` cannot be decrypted.

## Testing
61 automated tests (`pytest -v`), all passing:
- `solution/tests/test_normalizers.py`: Header normalization, all 6 valid Indian phone formats + invalid cases, phone masking, all 4 date formats + leap years + impossible dates, currency-prefixed decimals + rounding + negative rejection, case-insensitive enums, and UTC/offset/naive-IST timestamps (including UTC->IST midnight date rollovers).
- `solution/tests/test_ingest_and_reports.py`: Schema-order rejection reasons, blank row skipping, numeric `row_number` sorting, field warning exclusion on rejected rows, duplicate merging with permanent `batch_id` and post-merge defaults, `disposition.override` precedence & unknown disposition ignoring, all 4 bucket states, `current_disposition` skipping newer disposition-less calls, `call.initiated` requirement, late-arriving lead resolution across drops, orphan-before-unknown-disposition precedence, empty-DB report generation, raw DB file inspection for zero plaintext PII, append-only SQLite triggers, and idempotent re-ingest.
- `solution/tests/test_e2e_generator.py`: Runs `generate_data.py` on an unseen seed (`--seed 99`), verifies byte-identical outputs across all 5 files between incremental (`drop_1` then `drop_2`), re-ingested, and all-at-once runs, plus cross-report mathematical invariants.

## Results
Verified on `--seed 42 --scale small` (2,057 lead rows, 8,745 events) and `--seed 42 --scale large` (143,990 lead rows, 632,602 events) on Windows 11 / Python 3.13:

**`--scale large` timings (stretch goal < 2 minutes):**
- `ingest data_large/drop_1` (66 files): **12.76s**
- `ingest data_large/drop_2` (67 files): **17.74s**
- `report --out reports_large`: **12.72s**
- **Total end-to-end time:** **43.22s**

**`--scale small` (`seed=42`) summary:**
- Valid leads: `1,892` (`rejected=108`, `merged=57`)
- Events: `8,745` (`malformed=8`, `duplicate_events_dropped=237`, `orphan_events=462`, `unknown_disposition_events=18`)

## If I had another day
1. **Productionize for 100k–1M leads/day:** Move to PostgreSQL (partitioning `interaction_ledger` by `ist_date`, using `COPY` via staging tables for bulk ingestion), store the PII encryption key in AWS KMS / Vault with envelope encryption, and schedule drop ingestion as idempotent DAG tasks (Airflow/Temporal) with dead-letter storage for malformed event lines.
2. **Incremental report materialization:** Maintain incremental summary tables or watermarked ledger checkpoints so `drop_2` doesn't re-scan `drop_1` calls during reporting (trading off simplicity for sub-second report generation at multi-million-row scale).
3. **Key rotation & auditability:** Add a key-version prefix to `phone_enc`/`pii_enc` blobs and an `audit_log` for PII decryption access.
