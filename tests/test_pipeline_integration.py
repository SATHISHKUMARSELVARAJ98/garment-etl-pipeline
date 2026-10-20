
import csv
import logging
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text

from garment_etl.config import load_config
from garment_etl.database import initialize_database, make_engine
from garment_etl.pipeline import run_pipeline


@pytest.fixture
def test_setup(tmp_path):
    config = load_config("config/test.yaml")
    engine = make_engine(config.db_url)

    # Create schemas and tables in the test database.
    initialize_database(engine, config.schemas)

    # Use unique IDs and a unique filename for every test run.
    suffix = uuid.uuid4().hex[:12]
    source_file = tmp_path / f"orders_{suffix}.csv"

    rows = [
        {
            "order_id": f"TEST_{suffix}_001",
            "style_id": "ST001",
            "supplier_id": "SUP001",
            "order_date": "2026-01-01",
            "receive_date": "2026-01-05",
            "delivery_date": "2026-01-10",
            "quantity": "100",
            "unit_price": "12.50",
            "total_amount": "1250.00",
            "currency": "INR",
            "status": "CREATED",
        },
        {
            "order_id": f"TEST_{suffix}_002",
            "style_id": "ST002",
            "supplier_id": "SUP002",
            "order_date": "2026-01-02",
            "receive_date": "2026-01-06",
            "delivery_date": "2026-01-11",
            "quantity": "200",
            "unit_price": "10.00",
            "total_amount": "2000.00",
            "currency": "INR",
            "status": "APPROVED",
        },
        {
            "order_id": f"TEST_{suffix}_003",
            "style_id": "ST003",
            "supplier_id": "SUP003",
            "order_date": "2026-01-03",
            "receive_date": "2026-01-07",
            "delivery_date": "2026-01-12",
            "quantity": "50",
            "unit_price": "20.00",
            "total_amount": "1000.00",
            "currency": "INR",
            "status": "CREATED",
        },
    ]

    with source_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    logger = logging.getLogger("integration_test")
    logger.addHandler(logging.NullHandler())

    yield config, engine, source_file, rows, logger

    engine.dispose()


def test_pipeline_loads_three_orders(test_setup):
    config, engine, source_file, rows, logger = test_setup

    result = run_pipeline(
        config=config,
        engine=engine,
        logger=logger,
        source=source_file,
    )

    assert result["status"] == "SUCCEEDED"
    assert result["files_seen"] == 1
    assert result["files_succeeded"] == 1
    assert result["files_failed"] == 0
    assert result["rows_read"] == 3
    assert result["rows_accepted"] == 3
    assert result["rows_rejected"] == 0
    assert result["new_records"] == 3

    warehouse = config.schemas["warehouse"]
    audit = config.schemas["audit"]
    order_ids = [row["order_id"] for row in rows]

    with engine.connect() as conn:
        warehouse_count = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{warehouse}".fact_garment_order '
                "WHERE order_id = ANY(:order_ids)"
            ),
            {"order_ids": order_ids},
        ).scalar_one()

        audit_row = conn.execute(
            text(
                f'SELECT status, rows_read, rows_accepted, rows_rejected, '
                f'new_records FROM "{audit}".pipeline_run WHERE run_id = :run_id'
            ),
            {"run_id": result["run_id"]},
        ).mappings().one()

    assert warehouse_count == 3
    assert audit_row["status"] == "SUCCEEDED"
    assert audit_row["rows_read"] == 3
    assert audit_row["rows_accepted"] == 3
    assert audit_row["rows_rejected"] == 0
    assert audit_row["new_records"] == 3
    

def test_pipeline_detects_unchanged_changed_and_new_records(test_setup):
    config, engine, source_file, rows, logger = test_setup

    # First run: load all three records.
    first = run_pipeline(config, engine, logger, source=source_file)
    assert first["status"] == "SUCCEEDED"
    assert first["new_records"] == 3

    # Second run: same file content, so it should be skipped.
    second = run_pipeline(config, engine, logger, source=source_file)
    assert second["status"] == "SUCCEEDED"
    assert second["files_skipped"] == 1

    # Change the file content: modify one order and add another.
    updated_rows = [dict(row) for row in rows]
    updated_rows[0]["quantity"] = "150"
    updated_rows[0]["total_amount"] = "1875.00"

    new_row = dict(rows[0])
    new_row["order_id"] = rows[0]["order_id"] + "_NEW"
    new_row["quantity"] = "10"
    new_row["unit_price"] = "5.00"
    new_row["total_amount"] = "50.00"
    updated_rows.append(new_row)

    with source_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=updated_rows[0].keys())
        writer.writeheader()
        writer.writerows(updated_rows)

    third = run_pipeline(config, engine, logger, source=source_file)

    assert third["status"] == "SUCCEEDED"
    assert third["new_records"] == 1
    assert third["changed_records"] == 1
    assert third["unchanged_records"] == 2

    warehouse = config.schemas["warehouse"]
    modified_order_id = rows[0]["order_id"]
    new_order_id = new_row["order_id"]

    with engine.connect() as conn:
        modified_quantity = conn.execute(
            text(
                f'SELECT quantity FROM "{warehouse}".fact_garment_order '
                "WHERE order_id = :order_id"
            ),
            {"order_id": modified_order_id},
        ).scalar_one()

        new_order_count = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{warehouse}".fact_garment_order '
                "WHERE order_id = :order_id"
            ),
            {"order_id": new_order_id},
        ).scalar_one()

    assert modified_quantity == 150
    assert new_order_count == 1


def test_pipeline_rejects_invalid_record(test_setup):
    config, engine, source_file, rows, logger = test_setup

    invalid_rows = [dict(row) for row in rows[:2]]
    invalid_rows[0]["quantity"] = "0"

    with source_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=invalid_rows[0].keys())
        writer.writeheader()
        writer.writerows(invalid_rows)

    result = run_pipeline(
        config=config,
        engine=engine,
        logger=logger,
        source=source_file,
    )

    assert result["status"] == "SUCCEEDED"
    assert result["rows_read"] == 2
    assert result["rows_accepted"] == 1
    assert result["rows_rejected"] == 1
    assert result["new_records"] == 1

    audit = config.schemas["audit"]
    warehouse = config.schemas["warehouse"]

    with engine.connect() as conn:
        rejected_count = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{audit}".rejected_record '
                "WHERE file_load_id = ("
                f'SELECT file_load_id FROM "{audit}".file_load '
                "WHERE source_name = :name"
                " ORDER BY file_load_id DESC LIMIT 1)"
            ),
            {"name": source_file.name},
        ).scalar_one()

        invalid_order_count = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{warehouse}".fact_garment_order '
                "WHERE order_id = :order_id"
            ),
            {"order_id": invalid_rows[0]["order_id"]},
        ).scalar_one()

    assert rejected_count == 1
    assert invalid_order_count == 0




def test_pipeline_marks_run_failed_when_error_audit_fails(test_setup):
    config, engine, source_file, rows, logger = test_setup
    audit = config.schemas["audit"]

    original_text = text

    def fail_error_log_sql(sql, *args, **kwargs):
        if "error_log" in str(sql).lower():
            raise RuntimeError("Simulated error audit failure")
        return original_text(sql, *args, **kwargs)

    with patch(
        "garment_etl.pipeline.read_records",
        side_effect=ValueError("Simulated file processing failure"),
    ):
        with patch(
            "garment_etl.pipeline.text",
            side_effect=fail_error_log_sql,
        ):
            with pytest.raises(
                RuntimeError,
                match="Simulated error audit failure",
            ):
                run_pipeline(
                    config=config,
                    engine=engine,
                    logger=logger,
                    source=source_file,
                )

    with engine.connect() as conn:
        run = conn.execute(
            text(
                f'SELECT status, finished_at FROM "{audit}".pipeline_run '
                "ORDER BY run_id DESC LIMIT 1"
            )
        ).mappings().one()

    assert run["status"] == "FAILED"
    assert run["finished_at"] is not None
def test_reprocessing_invalid_file_does_not_duplicate_rejected_rows(test_setup):
    config, engine, source_file, rows, logger = test_setup
    audit = config.schemas["audit"]

    invalid_rows = [dict(rows[0])]
    invalid_rows[0]["quantity"] = "0"

    with source_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=invalid_rows[0].keys())
        writer.writeheader()
        writer.writerows(invalid_rows)

    first = run_pipeline(config, engine, logger, source=source_file)
    second = run_pipeline(
        config,
        engine,
        logger,
        source=source_file,
        force_baseline=True,
    )

    assert first["rows_rejected"] == 1
    assert second["rows_rejected"] == 1

    with engine.connect() as conn:
        rejected_count = conn.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM "{audit}".rejected_record
                WHERE file_load_id = (
                    SELECT file_load_id
                    FROM "{audit}".file_load
                    WHERE source_name = :source_name
                    ORDER BY file_load_id DESC
                    LIMIT 1
                )
                """
            ),
            {"source_name": source_file.name},
        ).scalar_one()

    assert rejected_count == 1
