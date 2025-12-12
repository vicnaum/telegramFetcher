"""SQLite database wrapper for tgx."""

import os
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def get_db_path() -> str:
    """Get database file path from env or default."""
    path = os.environ.get("TGX_DB", "./tgx.sqlite")
    return str(Path(path).expanduser().resolve())


def datetime_to_epoch_ms(dt: datetime) -> int:
    """Convert datetime to UTC epoch milliseconds.

    Args:
        dt: datetime object (naive assumed UTC, aware converted to UTC)

    Returns:
        Integer milliseconds since Unix epoch
    """
    if dt.tzinfo is None:
        # Assume naive datetime is UTC
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        # Convert to UTC
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_datetime(epoch_ms: int) -> datetime:
    """Convert UTC epoch milliseconds to aware datetime.

    Args:
        epoch_ms: Milliseconds since Unix epoch

    Returns:
        UTC-aware datetime object
    """
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


class Database:
    """SQLite database wrapper for message storage."""

    # Current schema version - equals the highest implemented migration number
    SCHEMA_VERSION = 1

    def __init__(self, db_path: str | None = None):
        """Initialize database connection.

        Args:
            db_path: Path to database file, or None for default
        """
        if db_path is None:
            db_path = get_db_path()

        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")

        self._init_schema()

    def _get_schema_version(self) -> int:
        """Get the current schema version from the database.

        Returns:
            Schema version number, 0 if not set
        """
        row = self.conn.execute("PRAGMA user_version").fetchone()
        return row[0] if row else 0

    def _set_schema_version(self, version: int) -> None:
        """Set the schema version in the database.

        Args:
            version: Version number to set
        """
        # PRAGMA user_version doesn't support parameters, must use string formatting
        # This is safe as version is always an int
        self.conn.execute(f"PRAGMA user_version = {int(version)}")
        self.conn.commit()

    def _init_schema(self) -> None:
        """Initialize database schema.

        Note: All timestamps are stored as INTEGER epoch milliseconds (UTC).
        """
        # Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON")

        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS peers (
                id INTEGER PRIMARY KEY,
                username TEXT,
                title TEXT,
                type TEXT,
                min_msg_id INTEGER DEFAULT 0,
                max_msg_id INTEGER DEFAULT 0,
                last_sync_ts INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_peers_username ON peers(username);

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER,
                peer_id INTEGER NOT NULL,
                date_utc_ms INTEGER NOT NULL,
                sender_id INTEGER,
                sender_name TEXT,
                text TEXT,
                reply_to_msg_id INTEGER,
                has_media INTEGER DEFAULT 0,
                media_type TEXT,
                raw_data TEXT,
                PRIMARY KEY (id, peer_id),
                FOREIGN KEY (peer_id) REFERENCES peers(id)
            );
            CREATE INDEX IF NOT EXISTS idx_msg_date ON messages(peer_id, date_utc_ms);
            CREATE INDEX IF NOT EXISTS idx_msg_peer_id ON messages(peer_id, id);
            CREATE INDEX IF NOT EXISTS idx_msg_reply ON messages(peer_id, reply_to_msg_id);

            -- Tracks which ranges of messages have been synced for each peer
            -- Allows detecting gaps and avoiding redundant fetches
            CREATE TABLE IF NOT EXISTS sync_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id INTEGER NOT NULL,
                -- ID boundaries (precise, monotonic)
                min_msg_id INTEGER NOT NULL,
                max_msg_id INTEGER NOT NULL,
                -- Date boundaries (for user-facing queries)
                min_date_utc_ms INTEGER NOT NULL,
                max_date_utc_ms INTEGER NOT NULL,
                -- Metadata
                message_count INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY (peer_id) REFERENCES peers(id)
            );
            CREATE INDEX IF NOT EXISTS idx_sync_ranges_peer ON sync_ranges(peer_id, min_msg_id);
        """)
        self.conn.commit()

        # Run migrations for existing databases
        self._run_migrations()

    def _run_migrations(self) -> None:
        """Run database migrations for schema updates.

        Uses PRAGMA user_version to track schema version and apply
        migrations incrementally.
        """
        current_version = self._get_schema_version()

        # Migration 1: Drop unused raw_data column from peers (if exists)
        if current_version < 1:
            cursor = self.conn.execute("PRAGMA table_info(peers)")
            columns = [row[1] for row in cursor.fetchall()]

            if "raw_data" in columns:
                # SQLite 3.35+ supports ALTER TABLE DROP COLUMN
                try:
                    self.conn.execute("ALTER TABLE peers DROP COLUMN raw_data")
                    self.conn.commit()
                except sqlite3.OperationalError:
                    # Older SQLite - just ignore, the column will be unused
                    pass

            self._set_schema_version(1)
            current_version = 1

        # Future migrations go here:
        # if current_version < 2:
        #     self.conn.execute("ALTER TABLE messages ADD COLUMN new_field TEXT")
        #     self.conn.commit()
        #     self._set_schema_version(2)
        #     current_version = 2
        #
        # Note: Only bump SCHEMA_VERSION when adding a new migration.
        # The version should equal the highest implemented migration number.

    def __enter__(self) -> "Database":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Context manager exit - commit if no exception, then close."""
        if exc_type is None:
            self.conn.commit()
        self.close()

    def close(self) -> None:
        """Close database connection."""
        self.conn.close()

    def commit(self) -> None:
        """Commit current transaction."""
        self.conn.commit()

    def update_peer(
        self,
        peer_id: int,
        username: str | None,
        title: str,
        peer_type: str,
    ) -> None:
        """Upsert peer metadata.

        Args:
            peer_id: Telegram peer ID
            username: Optional username
            title: Display title
            peer_type: Type ('user', 'group', 'channel')
        """
        self.conn.execute("""
            INSERT INTO peers (id, username, title, type)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username = excluded.username,
                title = excluded.title,
                type = excluded.type
        """, (peer_id, username, title, peer_type))

    def update_peer_sync_boundaries(
        self,
        peer_id: int,
        min_msg_id: int | None = None,
        max_msg_id: int | None = None,
    ) -> None:
        """Update peer sync boundaries.

        Args:
            peer_id: Telegram peer ID
            min_msg_id: New minimum message ID (if lower than current)
            max_msg_id: New maximum message ID (if higher than current)
        """
        if min_msg_id is not None:
            self.conn.execute("""
                UPDATE peers
                SET min_msg_id = ?
                WHERE id = ? AND (min_msg_id = 0 OR min_msg_id > ?)
            """, (min_msg_id, peer_id, min_msg_id))

        if max_msg_id is not None:
            self.conn.execute("""
                UPDATE peers
                SET max_msg_id = ?
                WHERE id = ? AND max_msg_id < ?
            """, (max_msg_id, peer_id, max_msg_id))

        # Store last_sync_ts as epoch milliseconds (UTC)
        now_ms = datetime_to_epoch_ms(datetime.now(timezone.utc))
        self.conn.execute("""
            UPDATE peers SET last_sync_ts = ? WHERE id = ?
        """, (now_ms, peer_id))

    def get_sync_boundaries(self, peer_id: int) -> tuple[int, int]:
        """Get sync boundaries for a peer.

        Args:
            peer_id: Telegram peer ID

        Returns:
            Tuple of (min_msg_id, max_msg_id), (0, 0) if not synced
        """
        row = self.conn.execute("""
            SELECT min_msg_id, max_msg_id FROM peers WHERE id = ?
        """, (peer_id,)).fetchone()

        if row is None:
            return (0, 0)

        return (row["min_msg_id"] or 0, row["max_msg_id"] or 0)

    def get_actual_message_boundaries(self, peer_id: int) -> tuple[int, int]:
        """Get actual min/max message IDs from messages table.

        This queries the messages table directly to get accurate boundaries,
        useful for deriving boundaries after commits rather than tracking
        in-memory which can drift on retry paths.

        Args:
            peer_id: Telegram peer ID

        Returns:
            Tuple of (min_msg_id, max_msg_id), (0, 0) if no messages
        """
        row = self.conn.execute("""
            SELECT MIN(id) as min_id, MAX(id) as max_id
            FROM messages WHERE peer_id = ?
        """, (peer_id,)).fetchone()

        if row is None or row["min_id"] is None:
            return (0, 0)

        return (row["min_id"], row["max_id"])

    def count_messages(self, peer_id: int) -> int:
        """Count messages for a peer.

        Args:
            peer_id: Telegram peer ID

        Returns:
            Number of messages stored
        """
        row = self.conn.execute("""
            SELECT COUNT(*) as cnt FROM messages WHERE peer_id = ?
        """, (peer_id,)).fetchone()

        return row["cnt"] if row else 0

    def get_oldest_message_date(self, peer_id: int) -> datetime | None:
        """Get the date of the oldest message for a peer.

        Args:
            peer_id: Telegram peer ID

        Returns:
            Datetime of oldest message, or None if no messages
        """
        row = self.conn.execute("""
            SELECT MIN(date_utc_ms) as min_date FROM messages WHERE peer_id = ?
        """, (peer_id,)).fetchone()

        if row and row["min_date"] is not None:
            return epoch_ms_to_datetime(row["min_date"])
        return None

    def has_message_at_or_before_date(self, peer_id: int, target_date: datetime) -> bool:
        """Check if we have a message at or before the target date.

        Args:
            peer_id: Telegram peer ID
            target_date: Target datetime (UTC)

        Returns:
            True if we have a message at or before target_date
        """
        target_ms = datetime_to_epoch_ms(target_date)
        row = self.conn.execute("""
            SELECT 1 FROM messages WHERE peer_id = ? AND date_utc_ms <= ? LIMIT 1
        """, (peer_id, target_ms)).fetchone()

        return row is not None

    def has_message_at_or_before_id(self, peer_id: int, target_id: int) -> bool:
        """Check if we have a message at or before the target ID.

        Args:
            peer_id: Telegram peer ID
            target_id: Target message ID

        Returns:
            True if we have a message with ID <= target_id
        """
        row = self.conn.execute("""
            SELECT 1 FROM messages WHERE peer_id = ? AND id <= ? LIMIT 1
        """, (peer_id, target_id)).fetchone()

        return row is not None

    def insert_message(
        self,
        msg_id: int,
        peer_id: int,
        date: datetime,
        sender_id: int | None,
        sender_name: str | None,
        text: str | None,
        reply_to_msg_id: int | None,
        has_media: bool,
        media_type: str | None,
        raw_data: str | None,
    ) -> bool:
        """Insert a message (ignores duplicates only).

        Uses INSERT OR IGNORE for robust duplicate handling that doesn't
        depend on exception text parsing.

        Args:
            msg_id: Message ID
            peer_id: Peer ID
            date: Message datetime (UTC) - REQUIRED
            sender_id: Sender user ID
            sender_name: Sender display name
            text: Message text
            reply_to_msg_id: Reply-to message ID
            has_media: Whether message has media
            media_type: Type of media
            raw_data: JSON dump of message

        Returns:
            True if inserted, False if duplicate (primary key conflict)

        Raises:
            ValueError: If date is None
            sqlite3.IntegrityError: For FK violations (peer_id not in peers table)
        """
        if date is None:
            raise ValueError(f"Message {msg_id} has no date - cannot insert")

        date_ms = datetime_to_epoch_ms(date)

        cursor = self.conn.execute("""
            INSERT OR IGNORE INTO messages
            (id, peer_id, date_utc_ms, sender_id, sender_name, text,
             reply_to_msg_id, has_media, media_type, raw_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            msg_id, peer_id, date_ms,
            sender_id, sender_name, text, reply_to_msg_id,
            1 if has_media else 0, media_type, raw_data
        ))
        return cursor.rowcount == 1

    def insert_messages_batch(self, messages: list[dict]) -> int:
        """Insert multiple messages in a batch using executemany.

        Uses INSERT OR IGNORE to efficiently handle duplicates without per-row
        exception handling. Duplicate messages are silently skipped.

        Args:
            messages: List of message dicts with keys:
                - msg_id: Message ID
                - peer_id: Peer ID
                - date: datetime object (required)
                - sender_id: Sender user ID
                - sender_name: Sender display name
                - text: Message text
                - reply_to_msg_id: Reply-to message ID
                - has_media: Whether message has media
                - media_type: Type of media
                - raw_data: JSON dump of message

        Returns:
            Number of messages inserted (duplicates are not counted)

        Raises:
            ValueError: If any message has no date
        """
        if not messages:
            return 0

        # Validate and convert dates
        rows = []
        for msg in messages:
            if msg["date"] is None:
                raise ValueError(f"Message {msg['msg_id']} has no date - cannot insert")

            date_ms = datetime_to_epoch_ms(msg["date"])
            rows.append((
                msg["msg_id"], msg["peer_id"], date_ms,
                msg["sender_id"], msg["sender_name"], msg["text"],
                msg["reply_to_msg_id"], 1 if msg["has_media"] else 0,
                msg["media_type"], msg["raw_data"]
            ))

        # Use INSERT OR IGNORE with executemany for efficiency
        # Track inserted count via total_changes delta
        changes_before = self.conn.total_changes

        self.conn.executemany("""
            INSERT OR IGNORE INTO messages
            (id, peer_id, date_utc_ms, sender_id, sender_name, text,
             reply_to_msg_id, has_media, media_type, raw_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        return self.conn.total_changes - changes_before

    def get_messages(
        self,
        peer_id: int,
        limit: int | None = None,
        since_id: int | None = None,
        until_id: int | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        order_desc: bool = False,
    ) -> Iterator[sqlite3.Row]:
        """Query messages from database.

        Args:
            peer_id: Peer ID to query
            limit: Maximum number of messages (None = unlimited, 0 = zero results)
            since_id: Minimum message ID (exclusive)
            until_id: Maximum message ID (exclusive)
            start_date: Start datetime (inclusive, UTC)
            end_date: End datetime (inclusive, UTC)
            order_desc: If True, order by date DESC (newest first)

        Yields:
            Database rows (with date_utc_ms as integer epoch milliseconds)
        """
        conditions = ["peer_id = ?"]
        params: list = [peer_id]

        if since_id is not None:
            conditions.append("id > ?")
            params.append(since_id)

        if until_id is not None:
            conditions.append("id < ?")
            params.append(until_id)

        if start_date is not None:
            start_ms = datetime_to_epoch_ms(start_date)
            conditions.append("date_utc_ms >= ?")
            params.append(start_ms)

        if end_date is not None:
            end_ms = datetime_to_epoch_ms(end_date)
            conditions.append("date_utc_ms <= ?")
            params.append(end_ms)

        order = "DESC" if order_desc else "ASC"

        # Parameterize LIMIT clause
        if limit is not None:
            if limit < 0:
                raise ValueError(f"Limit must be non-negative, got {limit}")
            limit_clause = "LIMIT ?"
            params.append(limit)
        else:
            limit_clause = ""

        query = f"""
            SELECT * FROM messages
            WHERE {' AND '.join(conditions)}
            ORDER BY date_utc_ms {order}, id {order}
            {limit_clause}
        """

        cursor = self.conn.execute(query, params)
        yield from cursor

    def _validate_export_filters(
        self,
        last_n: int | None,
        since_id: int | None,
        until_id: int | None,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> None:
        """Validate that export filter parameters are valid and not conflicting.

        Args:
            last_n: Get last N messages
            since_id: Minimum message ID (exclusive)
            until_id: Maximum message ID (exclusive)
            start_date: Start datetime (inclusive, UTC)
            end_date: End datetime (inclusive, UTC)

        Raises:
            ValueError: If parameters are invalid or conflicting
        """
        if last_n is not None:
            if last_n <= 0:
                raise ValueError("last_n must be positive")

            other_filters = [since_id, until_id, start_date, end_date]
            if any(f is not None for f in other_filters):
                raise ValueError(
                    "last_n cannot be combined with other filters "
                    "(since_id, until_id, start_date, end_date). "
                    "Use either last_n OR the other filters, not both."
                )

        if since_id is not None and until_id is not None and since_id >= until_id:
            raise ValueError(
                f"since_id ({since_id}) must be less than until_id ({until_id})"
            )

        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must not be after end_date ({end_date})"
            )

    def get_messages_for_export(
        self,
        peer_id: int,
        last_n: int | None = None,
        since_id: int | None = None,
        until_id: int | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Iterator[sqlite3.Row]:
        """Get messages for export (always chronological order).

        If last_n is specified, gets the N most recent messages.
        Otherwise applies the other filters.

        Args:
            peer_id: Peer ID to query
            last_n: Get last N messages (mutually exclusive with other filters)
            since_id: Minimum message ID (exclusive)
            until_id: Maximum message ID (exclusive)
            start_date: Start datetime (inclusive, UTC)
            end_date: End datetime (inclusive, UTC)

        Yields:
            Database rows in chronological order

        Raises:
            ValueError: If last_n is combined with other filters
        """
        self._validate_export_filters(last_n, since_id, until_id, start_date, end_date)

        if last_n is not None:
            # Use SQL subquery to get last N in chronological order
            # This avoids materializing and reversing in Python
            query = """
                SELECT * FROM (
                    SELECT * FROM messages WHERE peer_id = ?
                    ORDER BY date_utc_ms DESC, id DESC LIMIT ?
                ) ORDER BY date_utc_ms ASC, id ASC
            """
            cursor = self.conn.execute(query, [peer_id, last_n])
            yield from cursor
        else:
            # Apply filters and return in chronological order
            yield from self.get_messages(
                peer_id,
                since_id=since_id,
                until_id=until_id,
                start_date=start_date,
                end_date=end_date,
                order_desc=False,
            )

    def get_messages_for_export_with_reply_sender(
        self,
        peer_id: int,
        last_n: int | None = None,
        since_id: int | None = None,
        until_id: int | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Iterator[sqlite3.Row]:
        """Get messages for export with reply sender info via LEFT JOIN.

        This is a streaming-friendly method that includes reply_sender_name
        for each message without needing a second pass.

        Args:
            peer_id: Peer ID to query
            last_n: Get last N messages (mutually exclusive with other filters)
            since_id: Minimum message ID (exclusive)
            until_id: Maximum message ID (exclusive)
            start_date: Start datetime (inclusive, UTC)
            end_date: End datetime (inclusive, UTC)

        Yields:
            Database rows in chronological order with additional reply_sender_name column

        Raises:
            ValueError: If last_n is combined with other filters
        """
        self._validate_export_filters(last_n, since_id, until_id, start_date, end_date)

        if last_n is not None:
            # Use SQL subquery to get last N with reply sender
            query = """
                SELECT m.*, r.sender_name as reply_sender_name
                FROM (
                    SELECT * FROM messages WHERE peer_id = ?
                    ORDER BY date_utc_ms DESC, id DESC LIMIT ?
                ) m
                LEFT JOIN messages r ON m.peer_id = r.peer_id AND m.reply_to_msg_id = r.id
                ORDER BY m.date_utc_ms ASC, m.id ASC
            """
            cursor = self.conn.execute(query, [peer_id, last_n])
            yield from cursor
        else:
            # Build filtered query with LEFT JOIN
            conditions = ["m.peer_id = ?"]
            params: list = [peer_id]

            if since_id is not None:
                conditions.append("m.id > ?")
                params.append(since_id)

            if until_id is not None:
                conditions.append("m.id < ?")
                params.append(until_id)

            if start_date is not None:
                start_ms = datetime_to_epoch_ms(start_date)
                conditions.append("m.date_utc_ms >= ?")
                params.append(start_ms)

            if end_date is not None:
                end_ms = datetime_to_epoch_ms(end_date)
                conditions.append("m.date_utc_ms <= ?")
                params.append(end_ms)

            query = f"""
                SELECT m.*, r.sender_name as reply_sender_name
                FROM messages m
                LEFT JOIN messages r ON m.peer_id = r.peer_id AND m.reply_to_msg_id = r.id
                WHERE {' AND '.join(conditions)}
                ORDER BY m.date_utc_ms ASC, m.id ASC
            """
            cursor = self.conn.execute(query, params)
            yield from cursor

    # -------------------------------------------------------------------------
    # Sync Range Management
    # -------------------------------------------------------------------------

    def get_sync_ranges(self, peer_id: int) -> list["SyncRange"]:
        """Get all sync ranges for a peer, ordered by min_msg_id.

        Args:
            peer_id: Telegram peer ID

        Returns:
            List of SyncRange objects, sorted by min_msg_id ascending
        """
        rows = self.conn.execute("""
            SELECT id, peer_id, min_msg_id, max_msg_id,
                   min_date_utc_ms, max_date_utc_ms, message_count
            FROM sync_ranges
            WHERE peer_id = ?
            ORDER BY min_msg_id ASC
        """, (peer_id,)).fetchall()

        return [
            SyncRange(
                id=row["id"],
                peer_id=row["peer_id"],
                min_msg_id=row["min_msg_id"],
                max_msg_id=row["max_msg_id"],
                min_date_utc_ms=row["min_date_utc_ms"],
                max_date_utc_ms=row["max_date_utc_ms"],
                message_count=row["message_count"],
            )
            for row in rows
        ]

    def add_sync_range(
        self,
        peer_id: int,
        min_msg_id: int,
        max_msg_id: int,
        min_date_utc_ms: int,
        max_date_utc_ms: int,
        message_count: int,
    ) -> int:
        """Add a new sync range and merge with overlapping/adjacent ranges.

        Args:
            peer_id: Telegram peer ID
            min_msg_id: Minimum message ID in range
            max_msg_id: Maximum message ID in range
            min_date_utc_ms: Earliest message date in range (epoch ms)
            max_date_utc_ms: Latest message date in range (epoch ms)
            message_count: Number of messages in range

        Returns:
            ID of the inserted/merged range
        """
        now_ms = datetime_to_epoch_ms(datetime.now(timezone.utc))

        # Insert the new range
        cursor = self.conn.execute("""
            INSERT INTO sync_ranges
            (peer_id, min_msg_id, max_msg_id, min_date_utc_ms, max_date_utc_ms,
             message_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            peer_id, min_msg_id, max_msg_id,
            min_date_utc_ms, max_date_utc_ms,
            message_count, now_ms, now_ms
        ))
        new_id = cursor.lastrowid

        # Merge overlapping/adjacent ranges
        self._merge_sync_ranges(peer_id)

        return new_id

    def _merge_sync_ranges(self, peer_id: int) -> None:
        """Merge overlapping or adjacent sync ranges for a peer.

        Ranges are considered adjacent if their IDs differ by 1 or less
        (accounting for potential deleted messages in between).

        Args:
            peer_id: Telegram peer ID
        """
        ranges = self.get_sync_ranges(peer_id)
        if len(ranges) < 2:
            return

        merged: list[SyncRange] = [ranges[0]]

        for r in ranges[1:]:
            last = merged[-1]
            # Adjacent or overlapping? (IDs within 1 means adjacent)
            # We use a small tolerance (10) to account for deleted messages
            if r.min_msg_id <= last.max_msg_id + 10:
                # Merge: combine into one range
                merged[-1] = SyncRange(
                    id=last.id,  # Keep the first range's ID
                    peer_id=peer_id,
                    min_msg_id=min(last.min_msg_id, r.min_msg_id),
                    max_msg_id=max(last.max_msg_id, r.max_msg_id),
                    min_date_utc_ms=min(last.min_date_utc_ms, r.min_date_utc_ms),
                    max_date_utc_ms=max(last.max_date_utc_ms, r.max_date_utc_ms),
                    message_count=last.message_count + r.message_count,
                )
            else:
                # Gap detected, keep as separate range
                merged.append(r)

        # If we merged anything, update the database
        if len(merged) < len(ranges):
            now_ms = datetime_to_epoch_ms(datetime.now(timezone.utc))

            # Delete all existing ranges for this peer
            self.conn.execute(
                "DELETE FROM sync_ranges WHERE peer_id = ?",
                (peer_id,)
            )

            # Re-insert the merged ranges
            for r in merged:
                self.conn.execute("""
                    INSERT INTO sync_ranges
                    (peer_id, min_msg_id, max_msg_id, min_date_utc_ms, max_date_utc_ms,
                     message_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    peer_id, r.min_msg_id, r.max_msg_id,
                    r.min_date_utc_ms, r.max_date_utc_ms,
                    r.message_count, now_ms, now_ms
                ))

    def find_gaps_in_ranges(
        self,
        peer_id: int,
        target_min_id: int | None = None,
        target_max_id: int | None = None,
    ) -> list[tuple[int, int]]:
        """Find gaps between sync ranges, optionally within a target range.

        Args:
            peer_id: Telegram peer ID
            target_min_id: If set, only return gaps >= this ID
            target_max_id: If set, only return gaps <= this ID

        Returns:
            List of (gap_start_id, gap_end_id) tuples
        """
        ranges = self.get_sync_ranges(peer_id)
        if not ranges:
            # No ranges = everything is a gap
            if target_min_id is not None and target_max_id is not None:
                return [(target_min_id, target_max_id)]
            return []

        gaps: list[tuple[int, int]] = []

        # Gap before first range (if target specifies earlier start)
        if target_min_id is not None and target_min_id < ranges[0].min_msg_id:
            gaps.append((target_min_id, ranges[0].min_msg_id - 1))

        # Gaps between ranges
        for i in range(len(ranges) - 1):
            gap_start = ranges[i].max_msg_id + 1
            gap_end = ranges[i + 1].min_msg_id - 1

            if gap_end >= gap_start:
                # Apply target filters
                if target_min_id is not None:
                    gap_start = max(gap_start, target_min_id)
                if target_max_id is not None:
                    gap_end = min(gap_end, target_max_id)

                if gap_end >= gap_start:
                    gaps.append((gap_start, gap_end))

        # Gap after last range (if target specifies later end)
        if target_max_id is not None and target_max_id > ranges[-1].max_msg_id:
            gaps.append((ranges[-1].max_msg_id + 1, target_max_id))

        return gaps

    def find_gaps_in_date_range(
        self,
        peer_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> list[tuple[datetime, datetime, int | None, int | None]]:
        """Find gaps in sync coverage for a date range.

        Args:
            peer_id: Telegram peer ID
            start_date: Start of requested range (UTC)
            end_date: End of requested range (UTC)

        Returns:
            List of (gap_start_date, gap_end_date, approx_start_id, approx_end_id) tuples.
            IDs are approximate based on adjacent ranges, or None if unknown.
        """
        start_ms = datetime_to_epoch_ms(start_date)
        end_ms = datetime_to_epoch_ms(end_date)

        ranges = self.get_sync_ranges(peer_id)
        if not ranges:
            # No ranges = entire requested range is a gap
            return [(start_date, end_date, None, None)]

        # Filter ranges that overlap with requested date range
        relevant_ranges = [
            r for r in ranges
            if r.max_date_utc_ms >= start_ms and r.min_date_utc_ms <= end_ms
        ]

        if not relevant_ranges:
            # No overlap = entire range is a gap
            # Try to estimate IDs from nearest ranges
            before = [r for r in ranges if r.max_date_utc_ms < start_ms]
            after = [r for r in ranges if r.min_date_utc_ms > end_ms]

            approx_start_id = before[-1].max_msg_id + 1 if before else None
            approx_end_id = after[0].min_msg_id - 1 if after else None

            return [(start_date, end_date, approx_start_id, approx_end_id)]

        gaps: list[tuple[datetime, datetime, int | None, int | None]] = []

        # Gap before first relevant range
        first = relevant_ranges[0]
        if first.min_date_utc_ms > start_ms:
            gap_end_date = epoch_ms_to_datetime(first.min_date_utc_ms)
            gaps.append((start_date, gap_end_date, None, first.min_msg_id - 1))

        # Gaps between relevant ranges
        for i in range(len(relevant_ranges) - 1):
            curr = relevant_ranges[i]
            next_r = relevant_ranges[i + 1]

            # Only report gap if there's actually a time gap
            if next_r.min_date_utc_ms > curr.max_date_utc_ms:
                gap_start = epoch_ms_to_datetime(curr.max_date_utc_ms)
                gap_end = epoch_ms_to_datetime(next_r.min_date_utc_ms)
                gaps.append((
                    gap_start, gap_end,
                    curr.max_msg_id + 1, next_r.min_msg_id - 1
                ))

        # Gap after last relevant range
        last = relevant_ranges[-1]
        if last.max_date_utc_ms < end_ms:
            gap_start_date = epoch_ms_to_datetime(last.max_date_utc_ms)
            gaps.append((gap_start_date, end_date, last.max_msg_id + 1, None))

        return gaps

    def get_coverage_summary(self, peer_id: int) -> dict:
        """Get a summary of sync coverage for a peer.

        Args:
            peer_id: Telegram peer ID

        Returns:
            Dict with coverage info: ranges, total_messages, gaps, etc.
        """
        ranges = self.get_sync_ranges(peer_id)

        if not ranges:
            return {
                "ranges": [],
                "total_messages": 0,
                "total_ranges": 0,
                "has_gaps": False,
                "gaps": [],
            }

        gaps = self.find_gaps_in_ranges(peer_id)

        return {
            "ranges": [
                {
                    "min_msg_id": r.min_msg_id,
                    "max_msg_id": r.max_msg_id,
                    "min_date": epoch_ms_to_datetime(r.min_date_utc_ms),
                    "max_date": epoch_ms_to_datetime(r.max_date_utc_ms),
                    "message_count": r.message_count,
                }
                for r in ranges
            ],
            "total_messages": sum(r.message_count for r in ranges),
            "total_ranges": len(ranges),
            "has_gaps": len(gaps) > 0,
            "gaps": gaps,
            "oldest_date": epoch_ms_to_datetime(ranges[0].min_date_utc_ms),
            "newest_date": epoch_ms_to_datetime(ranges[-1].max_date_utc_ms),
        }


@dataclass
class SyncRange:
    """Represents a contiguous range of synced messages."""

    id: int | None
    peer_id: int
    min_msg_id: int
    max_msg_id: int
    min_date_utc_ms: int
    max_date_utc_ms: int
    message_count: int

