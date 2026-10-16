# Part 2 — Garment ERP ETL Pipeline

Separate from Part 1. Reads the Part 1 CSV, JSONL, or Parquet outputs and loads PostgreSQL schemas `raw`, `staging`, `integration`, `warehouse`, and `audit`.

## Install (Windows PowerShell)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
Copy-Item .env.example .env
```

Edit `.env` with your PostgreSQL credentials. Create a database and user as a PostgreSQL administrator:

```sql
CREATE USER garment_etl WITH PASSWORD 'use_a_strong_password';
CREATE DATABASE garment_dw OWNER garment_etl;
```

The default config assumes `synthetic_garment_source` and `garment_etl_pipeline` are sibling folders. Change `pipeline.source_dir` in `config/dev.yaml` if needed.

## Run

```powershell
garment-etl --config config/dev.yaml init-db
garment-etl --config config/dev.yaml run
garment-etl --config config/dev.yaml status
pytest -v
```

Run a specific file:

```powershell
garment-etl --config config/dev.yaml run --source "C:\path\to\garment_orders.csv"
garment-etl --config config/dev.yaml run --source "C:\path\to\garment_orders.jsonl"
garment-etl --config config/dev.yaml run --source "C:\path\to\garment_orders.parquet"
```

`run --source` accepts a file or a directory. Directory discovery supports `.csv`, `.jsonl`, and `.parquet`. By default, the configured `source_file` is selected when the configured source directory is used.

## Design

- `raw.source_order`: source payload JSONB plus file and row metadata.
- `staging.garment_order`: validated and typed records.
- `integration.garment_order`: canonical records upserted by `order_id`.
- `warehouse.fact_garment_order`: analytics-ready order fact, upserted by `order_id`.
- `audit.pipeline_run`, `audit.file_load`, `audit.rejected_record`, `audit.error_log`: operational audit.
- Completed files with the same filename and SHA-256 are skipped.
- Changed files are processed as new versions; business keys are upserted.
- Invalid records are retained in raw, recorded in audit, and written to `data/rejected/`.
- The source is never modified or truncated.

This is a local runnable core project. File-level idempotency is implemented; CDC/watermarks, Airflow, dbt, CI/CD, and cloud deployment are later phases. Use a secret manager and least-privilege database grants for production deployments.
