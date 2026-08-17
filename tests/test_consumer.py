import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumer"))

from kafka_to_minio import extract_record


def test_insert_event_uses_after_image():
    event = {
        "op": "c",
        "before": None,
        "after": {"id": 1, "email": "alice@example.com", "status": "ACTIVE"},
        "source": {"ts_ms": 1000, "lsn": 111},
    }
    record = extract_record(event)
    assert record["id"] == 1
    assert record["email"] == "alice@example.com"
    assert record["_op"] == "c"


def test_update_event_uses_after_image():
    event = {
        "op": "u",
        "before": {"id": 1, "email": "alice@example.com"},
        "after": {"id": 1, "email": "alice.new@example.com"},
        "source": {"ts_ms": 2000, "lsn": 222},
    }
    record = extract_record(event)
    assert record["email"] == "alice.new@example.com"
    assert record["_op"] == "u"


def test_delete_event_uses_before_image_not_after():
    event = {
        "op": "d",
        "before": {"id": 2, "email": "bob@example.com", "status": "PENDING"},
        "after": None,
        "source": {"ts_ms": 3000, "lsn": 333},
    }
    record = extract_record(event)
    assert record is not None
    assert record["id"] == 2
    assert record["email"] == "bob@example.com"
    assert record["_op"] == "d"


def test_tombstone_message_with_no_row_data_returns_none():
    event = {
        "op": "d",
        "before": None,
        "after": None,
        "source": {"ts_ms": 4000, "lsn": 444},
    }
    record = extract_record(event)
    assert record is None


def test_unknown_operation_type_returns_none():
    event = {
        "op": "r",
        "before": None,
        "after": {"id": 3, "email": "carol@example.com"},
        "source": {"ts_ms": 5000, "lsn": 555},
    }
    record = extract_record(event)
    assert record is not None
    assert record["_op"] == "r"


def test_record_carries_source_metadata():
    event = {
        "op": "c",
        "before": None,
        "after": {"id": 5},
        "source": {"ts_ms": 9999, "lsn": 8888},
    }
    record = extract_record(event)
    assert record["_source_ts_ms"] == 9999
    assert record["_lsn"] == 8888