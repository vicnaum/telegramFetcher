"""Tests for export functionality.

Focus on:
- raw_data exported as JSON object (not double-encoded string)
- Reply sender lookup efficiency
"""

import json
import tempfile
from datetime import datetime, timezone

import pytest

from tgx.db import Database
from tgx.exporter import (
    build_sender_lookup_for_replies,
    collect_reply_ids,
    export_jsonl,
    format_jsonl_line,
)


@pytest.fixture
def db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = f.name

    database = Database(db_path)
    yield database
    database.close()


class TestRawDataExport:
    """Test raw_data export semantics."""

    def test_raw_data_exported_as_object(self, db):
        """raw_data should be exported as parsed JSON object by default."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert message with JSON raw_data
        raw_obj = {"_": "Message", "id": 42, "text": "Hello"}
        db.insert_message(
            msg_id=1,
            peer_id=123,
            date=datetime.now(timezone.utc),
            sender_id=456,
            sender_name="Alice",
            text="Hello",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data=json.dumps(raw_obj),
        )
        db.commit()

        # Get the row and format it
        rows = list(db.get_messages(123))
        line = format_jsonl_line(rows[0], include_raw=True, raw_as_string=False)

        # Parse the output
        output = json.loads(line)

        # raw_data should be an object, not a string
        assert isinstance(output["raw_data"], dict)
        assert output["raw_data"]["_"] == "Message"
        assert output["raw_data"]["id"] == 42

    def test_raw_data_as_string_flag(self, db):
        """raw_data should be exported as string when raw_as_string=True."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        raw_obj = {"_": "Message", "id": 42}
        raw_str = json.dumps(raw_obj)

        db.insert_message(
            msg_id=1,
            peer_id=123,
            date=datetime.now(timezone.utc),
            sender_id=456,
            sender_name="Alice",
            text="Hello",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data=raw_str,
        )
        db.commit()

        rows = list(db.get_messages(123))
        line = format_jsonl_line(rows[0], include_raw=True, raw_as_string=True)

        output = json.loads(line)

        # raw_data should be a string
        assert isinstance(output["raw_data"], str)
        assert output["raw_data"] == raw_str

    def test_raw_data_parse_error_fallback(self, db):
        """Invalid JSON in raw_data should produce error fallback."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert message with invalid JSON
        db.insert_message(
            msg_id=1,
            peer_id=123,
            date=datetime.now(timezone.utc),
            sender_id=456,
            sender_name="Alice",
            text="Hello",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data="not valid json {{{",
        )
        db.commit()

        rows = list(db.get_messages(123))
        line = format_jsonl_line(rows[0], include_raw=True, raw_as_string=False)

        output = json.loads(line)

        # Should have fallback structure
        assert isinstance(output["raw_data"], dict)
        assert "_raw_data_text" in output["raw_data"]
        assert "_raw_data_parse_error" in output["raw_data"]
        assert output["raw_data"]["_raw_data_text"] == "not valid json {{{"

    def test_date_exported_as_iso(self, db):
        """Date should be exported as ISO8601 string."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        dt = datetime(2025, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
        db.insert_message(
            msg_id=1,
            peer_id=123,
            date=dt,
            sender_id=456,
            sender_name="Alice",
            text="Hello",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data="{}",
        )
        db.commit()

        rows = list(db.get_messages(123))
        line = format_jsonl_line(rows[0], include_raw=False)

        output = json.loads(line)

        # Should have both ISO date and epoch_ms
        assert "date" in output
        assert "date_utc_ms" in output
        assert output["date"].startswith("2025-01-15")
        assert isinstance(output["date_utc_ms"], int)


class TestReplySenderLookup:
    """Test optimized reply sender lookup."""

    def test_collect_reply_ids(self, db):
        """Should collect only non-null reply IDs."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert messages with various reply states
        for i, reply_to in enumerate([None, 100, None, 200, 100]):  # 100 appears twice
            db.insert_message(
                msg_id=i + 1,
                peer_id=123,
                date=datetime.now(timezone.utc),
                sender_id=456,
                sender_name="Alice",
                text=f"Message {i}",
                reply_to_msg_id=reply_to,
                has_media=False,
                media_type=None,
                raw_data="{}",
            )
        db.commit()

        rows = list(db.get_messages(123))
        reply_ids = collect_reply_ids(rows)

        # Should be a set with unique non-null values
        assert reply_ids == {100, 200}

    def test_build_sender_lookup_targeted(self, db):
        """Lookup should only query specified message IDs."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert 10 messages
        for i in range(10):
            db.insert_message(
                msg_id=i,
                peer_id=123,
                date=datetime.now(timezone.utc),
                sender_id=456 + i,
                sender_name=f"User{i}",
                text=f"Message {i}",
                reply_to_msg_id=None,
                has_media=False,
                media_type=None,
                raw_data="{}",
            )
        db.commit()

        # Build lookup for only specific IDs
        lookup = build_sender_lookup_for_replies(db, 123, {2, 5, 8})

        # Should only have entries for requested IDs
        assert set(lookup.keys()) == {2, 5, 8}
        assert lookup[2] == "User2"
        assert lookup[5] == "User5"
        assert lookup[8] == "User8"

    def test_build_sender_lookup_empty(self, db):
        """Empty reply set should return empty lookup."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        lookup = build_sender_lookup_for_replies(db, 123, set())
        assert lookup == {}


class TestExportJsonl:
    """Test JSONL export function."""

    def test_export_creates_valid_jsonl(self, db):
        """Export should create valid JSONL file."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        for i in range(5):
            db.insert_message(
                msg_id=i,
                peer_id=123,
                date=datetime.now(timezone.utc),
                sender_id=456,
                sender_name="Alice",
                text=f"Message {i}",
                reply_to_msg_id=None,
                has_media=False,
                media_type=None,
                raw_data=json.dumps({"id": i}),
            )
        db.commit()

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            output_path = f.name

        count = export_jsonl(db, 123, output_path, include_raw=True)
        assert count == 5

        # Verify each line is valid JSON
        with open(output_path) as f:
            lines = f.readlines()

        assert len(lines) == 5
        for line in lines:
            obj = json.loads(line)
            assert "id" in obj
            assert "raw_data" in obj
            assert isinstance(obj["raw_data"], dict)  # Parsed, not string

