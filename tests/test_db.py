"""Tests for database operations.

These tests focus on the tricky correctness issues:
- Date normalization and range queries
- insert_message() return semantics
- Constraint handling (duplicates vs other errors)
"""

from datetime import datetime, timedelta, timezone

import pytest

from tgx.db import (
    Database,
    datetime_to_epoch_ms,
    epoch_ms_to_datetime,
)


@pytest.fixture
def db(tmp_path):
    """Create a temporary database for testing.

    Uses pytest's tmp_path fixture for automatic cleanup.
    """
    db_path = tmp_path / "test.sqlite"
    database = Database(str(db_path))
    yield database
    database.close()


class TestDateConversion:
    """Test date conversion functions."""

    def test_datetime_to_epoch_ms_naive(self):
        """Naive datetime should be treated as UTC."""
        dt = datetime(2025, 1, 15, 10, 30, 45)
        ms = datetime_to_epoch_ms(dt)

        # Convert back and verify
        result = epoch_ms_to_datetime(ms)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 45
        assert result.tzinfo == timezone.utc

    def test_datetime_to_epoch_ms_aware_utc(self):
        """UTC-aware datetime should work correctly."""
        dt = datetime(2025, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
        ms = datetime_to_epoch_ms(dt)

        result = epoch_ms_to_datetime(ms)
        assert result == dt

    def test_datetime_to_epoch_ms_aware_offset(self):
        """Aware datetime with offset should be converted to UTC."""
        # 10:30 in UTC+5 = 05:30 UTC
        offset = timezone(timedelta(hours=5))
        dt = datetime(2025, 1, 15, 10, 30, 45, tzinfo=offset)
        ms = datetime_to_epoch_ms(dt)

        result = epoch_ms_to_datetime(ms)
        assert result.hour == 5  # Converted to UTC
        assert result.minute == 30
        assert result.tzinfo == timezone.utc

    def test_epoch_ms_preserves_milliseconds(self):
        """Millisecond precision should be preserved."""
        dt = datetime(2025, 1, 15, 10, 30, 45, 123000, tzinfo=timezone.utc)
        ms = datetime_to_epoch_ms(dt)
        result = epoch_ms_to_datetime(ms)

        # Should preserve up to millisecond precision
        assert result.microsecond // 1000 == 123


class TestInsertMessage:
    """Test insert_message return semantics."""

    def test_insert_new_returns_true(self, db):
        """First insert should return True."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        result = db.insert_message(
            msg_id=1,
            peer_id=123,
            date=datetime.now(timezone.utc),
            sender_id=456,
            sender_name="Alice",
            text="Hello",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data="{}",
        )

        assert result is True

    def test_insert_duplicate_returns_false(self, db):
        """Duplicate insert should return False."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        now = datetime.now(timezone.utc)

        # First insert
        db.insert_message(
            msg_id=1,
            peer_id=123,
            date=now,
            sender_id=456,
            sender_name="Alice",
            text="Hello",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data="{}",
        )
        db.commit()

        # Duplicate insert
        result = db.insert_message(
            msg_id=1,  # Same ID
            peer_id=123,  # Same peer
            date=now,
            sender_id=789,  # Different data
            sender_name="Bob",
            text="Different text",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data="{}",
        )

        assert result is False

    def test_insert_consecutive_returns_correct(self, db):
        """Multiple consecutive inserts should return correct values."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        now = datetime.now(timezone.utc)

        # This tests the fix for issue #2 (total_changes was cumulative)
        results = []
        for i in range(5):
            result = db.insert_message(
                msg_id=i,
                peer_id=123,
                date=now,
                sender_id=456,
                sender_name="Alice",
                text=f"Message {i}",
                reply_to_msg_id=None,
                has_media=False,
                media_type=None,
                raw_data="{}",
            )
            results.append(result)

        assert results == [True, True, True, True, True]

        # Now try inserting duplicates
        for i in range(5):
            result = db.insert_message(
                msg_id=i,
                peer_id=123,
                date=now,
                sender_id=456,
                sender_name="Alice",
                text=f"Message {i}",
                reply_to_msg_id=None,
                has_media=False,
                media_type=None,
                raw_data="{}",
            )
            results.append(result)

        # All duplicates should return False
        assert results[5:] == [False, False, False, False, False]

    def test_insert_null_date_raises(self, db):
        """Insert with None date should raise ValueError."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        with pytest.raises(ValueError, match="has no date"):
            db.insert_message(
                msg_id=1,
                peer_id=123,
                date=None,  # type: ignore
                sender_id=456,
                sender_name="Alice",
                text="Hello",
                reply_to_msg_id=None,
                has_media=False,
                media_type=None,
                raw_data="{}",
            )


class TestDateRangeQueries:
    """Test date filtering and ordering."""

    def test_date_range_query_ordering(self, db):
        """Messages should be ordered correctly by date."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert messages with different dates
        base = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        for i in range(5):
            dt = base + timedelta(hours=i)
            db.insert_message(
                msg_id=100 - i,  # IDs in reverse order to test sorting
                peer_id=123,
                date=dt,
                sender_id=456,
                sender_name="Alice",
                text=f"Message at {i} hours",
                reply_to_msg_id=None,
                has_media=False,
                media_type=None,
                raw_data="{}",
            )
        db.commit()

        # Query ascending (default)
        rows = list(db.get_messages(123))
        dates = [epoch_ms_to_datetime(r["date_utc_ms"]) for r in rows]
        assert dates == sorted(dates)

        # Query descending
        rows = list(db.get_messages(123, order_desc=True))
        dates = [epoch_ms_to_datetime(r["date_utc_ms"]) for r in rows]
        assert dates == sorted(dates, reverse=True)

    def test_date_range_filter(self, db):
        """Date range filters should work correctly."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert messages across January 2025
        for day in [1, 5, 10, 15, 20, 25, 30]:
            dt = datetime(2025, 1, day, 12, 0, 0, tzinfo=timezone.utc)
            db.insert_message(
                msg_id=day,
                peer_id=123,
                date=dt,
                sender_id=456,
                sender_name="Alice",
                text=f"Message on day {day}",
                reply_to_msg_id=None,
                has_media=False,
                media_type=None,
                raw_data="{}",
            )
        db.commit()

        # Query for days 10-20 inclusive
        start = datetime(2025, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 20, 23, 59, 59, tzinfo=timezone.utc)

        rows = list(db.get_messages(123, start_date=start, end_date=end))
        msg_ids = [r["id"] for r in rows]

        assert 10 in msg_ids
        assert 15 in msg_ids
        assert 20 in msg_ids
        assert 1 not in msg_ids
        assert 5 not in msg_ids
        assert 25 not in msg_ids
        assert 30 not in msg_ids

    def test_date_filter_with_timezone_offset(self, db):
        """Date filter should work correctly with timezone offsets."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert message at exactly midnight UTC
        midnight_utc = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        db.insert_message(
            msg_id=1,
            peer_id=123,
            date=midnight_utc,
            sender_id=456,
            sender_name="Alice",
            text="Midnight message",
            reply_to_msg_id=None,
            has_media=False,
            media_type=None,
            raw_data="{}",
        )
        db.commit()

        # Filter using UTC+5 timezone (should still find the message)
        offset = timezone(timedelta(hours=5))
        # 00:00 UTC = 05:00 UTC+5
        start = datetime(2025, 1, 15, 4, 0, 0, tzinfo=offset)  # 04:00 UTC+5 = 23:00 Jan 14 UTC
        end = datetime(2025, 1, 15, 6, 0, 0, tzinfo=offset)    # 06:00 UTC+5 = 01:00 Jan 15 UTC

        rows = list(db.get_messages(123, start_date=start, end_date=end))
        assert len(rows) == 1


class TestBatchInsert:
    """Test batch insert operations."""

    def test_batch_insert_returns_count(self, db):
        """Batch insert should return inserted count."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        messages = [
            {
                "msg_id": i,
                "peer_id": 123,
                "date": datetime.now(timezone.utc),
                "sender_id": 456,
                "sender_name": "Alice",
                "text": f"Message {i}",
                "reply_to_msg_id": None,
                "has_media": False,
                "media_type": None,
                "raw_data": "{}",
            }
            for i in range(10)
        ]

        inserted = db.insert_messages_batch(messages)

        assert inserted == 10

    def test_batch_insert_handles_duplicates(self, db):
        """Batch insert should skip duplicates silently."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        now = datetime.now(timezone.utc)
        messages = [
            {
                "msg_id": 1,  # Same ID
                "peer_id": 123,
                "date": now,
                "sender_id": 456,
                "sender_name": "Alice",
                "text": f"Message attempt {i}",
                "reply_to_msg_id": None,
                "has_media": False,
                "media_type": None,
                "raw_data": "{}",
            }
            for i in range(5)
        ]

        inserted = db.insert_messages_batch(messages)

        assert inserted == 1  # Only first one, duplicates silently skipped


class TestLimitHandling:
    """Test LIMIT clause handling."""

    def test_limit_zero_returns_empty(self, db):
        """LIMIT 0 should return zero results (not unlimited)."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert some messages
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

        # LIMIT 0 should return empty
        rows = list(db.get_messages(123, limit=0))
        assert len(rows) == 0

    def test_limit_none_returns_all(self, db):
        """LIMIT None should return all results."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Insert some messages
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

        # LIMIT None should return all
        rows = list(db.get_messages(123, limit=None))
        assert len(rows) == 5


class TestContextManager:
    """Test database context manager."""

    def test_context_manager_commits_on_success(self, tmp_path):
        """Context manager should commit on successful exit."""
        db_path = str(tmp_path / "context_test.sqlite")

        # Insert with context manager
        with Database(db_path) as db:
            db.update_peer(123, "test", "Test Peer", "channel")
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
                raw_data="{}",
            )

        # Verify data was committed
        with Database(db_path) as db:
            rows = list(db.get_messages(123))
            assert len(rows) == 1

