from src.garment_etl.record_tracking import classify_record


def test_record_without_previous_hash_is_new():
    assert classify_record(None, "hash_a") == "new"


def test_record_with_matching_hash_is_unchanged():
    assert classify_record("hash_a", "hash_a") == "unchanged"


def test_record_with_different_hash_is_changed():
    assert classify_record("hash_a", "hash_b") == "changed"
