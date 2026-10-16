from decimal import Decimal
from garment_etl.validation import normalize_order

VALID={"order_id":"ORD1","style_id":"ST1","supplier_id":"SUP1","order_date":"2025-01-01",
"receive_date":"2025-01-05","delivery_date":"2025-01-10","quantity":"100","unit_price":"12.50",
"total_amount":"1250.00","currency":"INR","status":"CREATED"}

def test_valid_record():
    row, errors=normalize_order(VALID,("INR",),("CREATED",))
    assert errors == []
    assert row["quantity"] == 100 and row["unit_price"] == Decimal("12.50")

def test_invalid_quantity():
    row, errors=normalize_order(VALID | {"quantity":"0"},("INR",),("CREATED",))
    assert row is None and any("quantity" in x for x in errors)

def test_total_mismatch():
    row, errors=normalize_order(VALID | {"total_amount":"1.00"},("INR",),("CREATED",))
    assert row is None and any("quantity * unit_price" in x for x in errors)

def test_invalid_dates():
    row, errors=normalize_order(VALID | {"delivery_date":"2024-01-01"},("INR",),("CREATED",))
    assert row is None and errors
