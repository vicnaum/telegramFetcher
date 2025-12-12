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


class TestExportFilterValidation:
    """Test export filter validation."""

    def test_last_n_positive_required(self, db):
        """last_n must be positive."""
        with pytest.raises(ValueError, match="must be positive"):
            db._validate_export_filters(
                last_n=0,
                since_id=None,
                until_id=None,
                start_date=None,
                end_date=None,
            )

        with pytest.raises(ValueError, match="must be positive"):
            db._validate_export_filters(
                last_n=-5,
                since_id=None,
                until_id=None,
                start_date=None,
                end_date=None,
            )

    def test_last_n_cannot_combine_with_other_filters(self, db):
        """last_n cannot be combined with other filters."""
        with pytest.raises(ValueError, match="cannot be combined"):
            db._validate_export_filters(
                last_n=10,
                since_id=5,
                until_id=None,
                start_date=None,
                end_date=None,
            )

    def test_since_id_must_be_less_than_until_id(self, db):
        """since_id must be less than until_id."""
        with pytest.raises(ValueError, match="must be less than until_id"):
            db._validate_export_filters(
                last_n=None,
                since_id=100,
                until_id=50,
                start_date=None,
                end_date=None,
            )

        with pytest.raises(ValueError, match="must be less than until_id"):
            db._validate_export_filters(
                last_n=None,
                since_id=100,
                until_id=100,  # Equal is also invalid
                start_date=None,
                end_date=None,
            )

    def test_start_date_must_not_be_after_end_date(self, db):
        """start_date must not be after end_date."""
        start = datetime(2025, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 10, 0, 0, 0, tzinfo=timezone.utc)

        with pytest.raises(ValueError, match="must not be after end_date"):
            db._validate_export_filters(
                last_n=None,
                since_id=None,
                until_id=None,
                start_date=start,
                end_date=end,
            )

    def test_valid_filters_pass(self, db):
        """Valid filter combinations should not raise."""
        # last_n alone
        db._validate_export_filters(
            last_n=10,
            since_id=None,
            until_id=None,
            start_date=None,
            end_date=None,
        )

        # Valid ID range
        db._validate_export_filters(
            last_n=None,
            since_id=10,
            until_id=100,
            start_date=None,
            end_date=None,
        )

        # Valid date range
        start = datetime(2025, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
        db._validate_export_filters(
            last_n=None,
            since_id=None,
            until_id=None,
            start_date=start,
            end_date=end,
        )

        # No filters at all
        db._validate_export_filters(
            last_n=None,
            since_id=None,
            until_id=None,
            start_date=None,
            end_date=None,
        )


class TestSyncRanges:
    """Test sync range tracking and gap detection."""

    def test_add_sync_range(self, db):
        """Adding a sync range should work."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        db.add_sync_range(
            peer_id=123,
            min_msg_id=100,
            max_msg_id=200,
            min_date_utc_ms=1700000000000,
            max_date_utc_ms=1700100000000,
            message_count=50,
        )
        db.commit()

        ranges = db.get_sync_ranges(123)
        assert len(ranges) == 1
        assert ranges[0].min_msg_id == 100
        assert ranges[0].max_msg_id == 200
        assert ranges[0].message_count == 50

    def test_adjacent_ranges_are_merged(self, db):
        """Adjacent ranges should be merged automatically."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Add first range: 100-200
        db.add_sync_range(
            peer_id=123,
            min_msg_id=100,
            max_msg_id=200,
            min_date_utc_ms=1700000000000,
            max_date_utc_ms=1700100000000,
            message_count=50,
        )
        db.commit()

        # Add adjacent range: 201-300 (should merge)
        db.add_sync_range(
            peer_id=123,
            min_msg_id=201,
            max_msg_id=300,
            min_date_utc_ms=1700100000001,
            max_date_utc_ms=1700200000000,
            message_count=50,
        )
        db.commit()

        ranges = db.get_sync_ranges(123)
        assert len(ranges) == 1
        assert ranges[0].min_msg_id == 100
        assert ranges[0].max_msg_id == 300
        assert ranges[0].message_count == 100

    def test_overlapping_ranges_are_merged(self, db):
        """Overlapping ranges should be merged."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Add first range: 100-200
        db.add_sync_range(
            peer_id=123,
            min_msg_id=100,
            max_msg_id=200,
            min_date_utc_ms=1700000000000,
            max_date_utc_ms=1700100000000,
            message_count=50,
        )
        db.commit()

        # Add overlapping range: 150-250 (should merge)
        db.add_sync_range(
            peer_id=123,
            min_msg_id=150,
            max_msg_id=250,
            min_date_utc_ms=1700050000000,
            max_date_utc_ms=1700150000000,
            message_count=50,
        )
        db.commit()

        ranges = db.get_sync_ranges(123)
        assert len(ranges) == 1
        assert ranges[0].min_msg_id == 100
        assert ranges[0].max_msg_id == 250

    def test_gap_detection(self, db):
        """Gaps between ranges should be detected."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Add first range: 100-200
        db.add_sync_range(
            peer_id=123,
            min_msg_id=100,
            max_msg_id=200,
            min_date_utc_ms=1700000000000,
            max_date_utc_ms=1700100000000,
            message_count=50,
        )
        db.commit()

        # Add non-adjacent range: 500-600 (creates a gap)
        db.add_sync_range(
            peer_id=123,
            min_msg_id=500,
            max_msg_id=600,
            min_date_utc_ms=1700200000000,
            max_date_utc_ms=1700300000000,
            message_count=50,
        )
        db.commit()

        ranges = db.get_sync_ranges(123)
        assert len(ranges) == 2

        gaps = db.find_gaps_in_ranges(123)
        assert len(gaps) == 1
        assert gaps[0] == (201, 499)  # Gap between 200 and 500

    def test_coverage_summary(self, db):
        """Coverage summary should report correct info."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        # Add two ranges with a gap
        db.add_sync_range(
            peer_id=123,
            min_msg_id=100,
            max_msg_id=200,
            min_date_utc_ms=1700000000000,
            max_date_utc_ms=1700100000000,
            message_count=50,
        )
        db.add_sync_range(
            peer_id=123,
            min_msg_id=500,
            max_msg_id=600,
            min_date_utc_ms=1700200000000,
            max_date_utc_ms=1700300000000,
            message_count=60,
        )
        db.commit()

        summary = db.get_coverage_summary(123)
        assert summary["total_messages"] == 110
        assert summary["total_ranges"] == 2
        assert summary["has_gaps"] is True
        assert len(summary["gaps"]) == 1

    def test_no_ranges_returns_empty(self, db):
        """Peer with no ranges should return empty list."""
        db.update_peer(123, "test", "Test Peer", "channel")
        db.commit()

        ranges = db.get_sync_ranges(123)
        assert len(ranges) == 0

        summary = db.get_coverage_summary(123)
        assert summary["total_messages"] == 0
        assert summary["has_gaps"] is False

