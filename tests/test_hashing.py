from src.garment_etl.pipeline import sha256_record


def test_same_record_produces_same_hash():
    record = {
        "order_id": "ORD000001001",
        "quantity": 100,
        "unit_price": "500.00",
    }

    assert sha256_record(record) == sha256_record(record)


def test_dictionary_key_order_does_not_change_hash():
    first = {"order_id": "ORD001", "quantity": 100}
    second = {"quantity": 100, "order_id": "ORD001"}

    assert sha256_record(first) == sha256_record(second)


def test_changed_value_produces_different_hash():
    first = {"order_id": "ORD001", "quantity": 100}
    second = {"order_id": "ORD001", "quantity": 200}

    assert sha256_record(first) != sha256_record(second)