from sqlalchemy import create_engine, text

def make_engine(db_url):
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=1800)

def initialize_database(engine, schemas):
    raw, staging, integration, warehouse, audit = (schemas[k] for k in ("raw","staging","integration","warehouse","audit"))
    statements = [
      f'CREATE SCHEMA IF NOT EXISTS "{raw}"', f'CREATE SCHEMA IF NOT EXISTS "{staging}"',
      f'CREATE SCHEMA IF NOT EXISTS "{integration}"', f'CREATE SCHEMA IF NOT EXISTS "{warehouse}"',
      f'CREATE SCHEMA IF NOT EXISTS "{audit}"',
      f'''CREATE TABLE IF NOT EXISTS "{audit}".pipeline_run (
        run_id BIGSERIAL PRIMARY KEY, pipeline_name TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ, status TEXT NOT NULL, files_seen INT DEFAULT 0, files_skipped INT DEFAULT 0,
        files_succeeded INT DEFAULT 0, files_failed INT DEFAULT 0, rows_read BIGINT DEFAULT 0,
        rows_accepted BIGINT DEFAULT 0, rows_rejected BIGINT DEFAULT 0, error_message TEXT,new_records BIGINT DEFAULT 0,
        changed_records BIGINT DEFAULT 0,unchanged_records BIGINT DEFAULT 0)''',
      f'''CREATE TABLE IF NOT EXISTS "{audit}".file_load (
        file_load_id BIGSERIAL PRIMARY KEY, run_id BIGINT REFERENCES "{audit}".pipeline_run(run_id),
        source_path TEXT NOT NULL, source_name TEXT NOT NULL, source_sha256 TEXT NOT NULL, status TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ, rows_read BIGINT DEFAULT 0,
        rows_accepted BIGINT DEFAULT 0, rows_rejected BIGINT DEFAULT 0, error_message TEXT,
        UNIQUE(source_sha256, source_name))''',
      f'''CREATE TABLE IF NOT EXISTS "{audit}".source_record_state (
        source_name TEXT NOT NULL,
        record_key TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (source_name, record_key))''',
      f'''CREATE TABLE IF NOT EXISTS "{raw}".source_order (
        raw_id BIGSERIAL PRIMARY KEY, file_load_id BIGINT NOT NULL REFERENCES "{audit}".file_load(file_load_id),
        row_number BIGINT NOT NULL, source_payload JSONB NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE(file_load_id,row_number))''',
      f'''CREATE TABLE IF NOT EXISTS "{staging}".garment_order (
        order_id TEXT PRIMARY KEY, style_id TEXT NOT NULL, supplier_id TEXT NOT NULL, order_date DATE NOT NULL,
        receive_date DATE NOT NULL, delivery_date DATE NOT NULL, quantity INT NOT NULL CHECK(quantity>0),
        unit_price NUMERIC(12,2) NOT NULL CHECK(unit_price>0), total_amount NUMERIC(18,2) NOT NULL CHECK(total_amount>0),
        currency CHAR(3) NOT NULL, status TEXT NOT NULL, source_file_load_id BIGINT NOT NULL, staged_at TIMESTAMPTZ DEFAULT now())''',
      f'''CREATE TABLE IF NOT EXISTS "{integration}".garment_order (
        order_id TEXT PRIMARY KEY, style_id TEXT NOT NULL, supplier_id TEXT NOT NULL, order_date DATE NOT NULL,
        receive_date DATE NOT NULL, delivery_date DATE NOT NULL, quantity INT NOT NULL CHECK(quantity>0),
        unit_price NUMERIC(12,2) NOT NULL CHECK(unit_price>0), total_amount NUMERIC(18,2) NOT NULL CHECK(total_amount>0),
        currency CHAR(3) NOT NULL, status TEXT NOT NULL, source_file_load_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now())''',
      f'''CREATE TABLE IF NOT EXISTS "{warehouse}".fact_garment_order (
        order_id TEXT PRIMARY KEY, style_id TEXT NOT NULL, supplier_id TEXT NOT NULL, order_date DATE NOT NULL,
        receive_date DATE NOT NULL, delivery_date DATE NOT NULL, quantity INT NOT NULL CHECK(quantity>0),
        unit_price NUMERIC(12,2) NOT NULL CHECK(unit_price>0), total_amount NUMERIC(18,2) NOT NULL CHECK(total_amount>0),
        currency CHAR(3) NOT NULL, status TEXT NOT NULL, source_file_load_id BIGINT NOT NULL, loaded_at TIMESTAMPTZ DEFAULT now())''',
      f'''CREATE TABLE IF NOT EXISTS "{audit}".rejected_record (
        rejected_id BIGSERIAL PRIMARY KEY, file_load_id BIGINT NOT NULL REFERENCES "{audit}".file_load(file_load_id),
        row_number BIGINT NOT NULL, source_payload JSONB NOT NULL, error_reasons JSONB NOT NULL, rejected_at TIMESTAMPTZ DEFAULT now())''',
      f'''CREATE TABLE IF NOT EXISTS "{audit}".error_log (
        error_id BIGSERIAL PRIMARY KEY, run_id BIGINT, file_load_id BIGINT, error_type TEXT NOT NULL,
        error_message TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now())'''
    ]
    with engine.begin() as conn:
        for statement in statements: conn.execute(text(statement))
