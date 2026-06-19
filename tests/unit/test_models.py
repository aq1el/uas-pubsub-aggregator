"""Unit test untuk validasi skema Event (Bab 4: Naming).
TIDAK butuh Docker / koneksi network apa pun -> bisa dijalankan kapan saja
dengan: pytest tests/unit -v
"""
import pytest
from pydantic import ValidationError

from app.models import Event


def test_valid_event_passes():
    ev = Event(
        topic="orders",
        event_id="evt-001",
        timestamp="2026-06-19T10:00:00Z",
        source="publisher-1",
        payload={"amount": 100},
    )
    assert ev.topic == "orders"
    assert ev.payload == {"amount": 100}


def test_payload_defaults_to_empty_dict_when_omitted():
    ev = Event(
        topic="auth",
        event_id="evt-002",
        timestamp="2026-06-19T10:00:00Z",
        source="publisher-2",
    )
    assert ev.payload == {}


def test_missing_topic_raises_validation_error():
    with pytest.raises(ValidationError):
        Event(event_id="evt-003", timestamp="2026-06-19T10:00:00Z", source="publisher-1")


def test_empty_event_id_raises_validation_error():
    with pytest.raises(ValidationError):
        Event(topic="orders", event_id="", timestamp="2026-06-19T10:00:00Z", source="publisher-1")


def test_invalid_timestamp_format_raises_validation_error():
    with pytest.raises(ValidationError):
        Event(topic="orders", event_id="evt-004", timestamp="bukan-tanggal", source="publisher-1")


def test_timestamp_with_z_suffix_is_accepted():
    # Format 'Z' (UTC) harus diterima walau Python fromisoformat native tidak
    # mendukungnya secara langsung (lihat field_validator di models.py).
    ev = Event(topic="orders", event_id="evt-005", timestamp="2026-06-19T10:00:00Z", source="p1")
    assert ev.timestamp == "2026-06-19T10:00:00Z"


def test_topic_too_long_raises_validation_error():
    with pytest.raises(ValidationError):
        Event(topic="x" * 300, event_id="evt-006", timestamp="2026-06-19T10:00:00Z", source="p1")
