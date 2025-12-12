"""Tests for export functionality.

Focus on:
- raw_data exported as JSON object (not double-encoded string)
- Export output format validation
- Filter parameter validation
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tgx.db import Database
from tgx.exporter import (
    export_jsonl,
    export_txt,
    format_jsonl_line,
)


@pytest.fixture
def db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.sqlite"
    database = Database(str(db_path))
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


class TestExportJsonl:
    """Test JSONL export function."""

    def test_export_creates_valid_jsonl(self, db, tmp_path):
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

        output_path = tmp_path / "export.jsonl"
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


class TestExportTxt:
    """Test TXT export function."""

    def test_export_creates_valid_txt(self, db, tmp_path):
        """Export should create valid TXT file."""
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
                raw_data="{}",
            )
        db.commit()

        output_path = tmp_path / "export.txt"
        count = export_txt(db, 123, output_path)
        assert count == 5

        # Verify file format
        with open(output_path) as f:
            lines = f.readlines()

        assert len(lines) == 5
        for i, line in enumerate(lines):
            # Each line should have format: [msg_id] timestamp | sender | text
            assert f"[{i}]" in line
            assert "Alice" in line
            assert f"Message {i}" in line

    def test_export_with_replies(self, db, tmp_path):
        """Export should include reply sender names."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Original message
        db.insert_message(
            msg_id=1,
            peer_id=123,
            date=datetime.now(timezone.utc),
            sender_id=456,
            sender_name="Alice",
            text="Original message",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data="{}",
        )
        # Reply to the original
        db.insert_message(
            msg_id=2,
            peer_id=123,
            date=datetime.now(timezone.utc),
            sender_id=789,
            sender_name="Bob",
            text="Reply message",
            reply_to_msg_id=1,
            has_media=False,
            media_type=None,
            raw_data="{}",
        )
        db.commit()

        output_path = tmp_path / "export.txt"
        count = export_txt(db, 123, output_path)
        assert count == 2

        with open(output_path) as f:
            lines = f.readlines()

        # Second line should have reply info with sender name
        assert "[reply to #1 @Alice]" in lines[1]


class TestExportFilterValidation:
    """Test that export functions validate filter parameters."""

    def test_last_n_with_other_filters_raises(self, db, tmp_path):
        """Using last_n with other filters should raise ValueError."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        output_path = tmp_path / "export.jsonl"

        with pytest.raises(ValueError, match="last_n cannot be combined"):
            export_jsonl(db, 123, output_path, last_n=10, since_id=5)

        with pytest.raises(ValueError, match="last_n cannot be combined"):
            export_jsonl(
                db, 123, output_path,
                last_n=10,
                start_date=datetime.now(timezone.utc)
            )

    def test_last_n_alone_works(self, db, tmp_path):
        """Using last_n alone should work."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        for i in range(10):
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
                raw_data="{}",
            )
        db.commit()

        output_path = tmp_path / "export.jsonl"
        count = export_jsonl(db, 123, output_path, last_n=5)
        assert count == 5

    def test_other_filters_work(self, db, tmp_path):
        """Using filters without last_n should work."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        for i in range(10):
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
                raw_data="{}",
            )
        db.commit()

        output_path = tmp_path / "export.jsonl"
        count = export_jsonl(db, 123, output_path, since_id=5)
        assert count == 4  # Messages 6, 7, 8, 9
