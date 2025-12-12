"""SQLite database wrapper for tgx."""

import os
import sqlite3
from collections.abc import Iterator
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
                last_sync_ts INTEGER,
                raw_data TEXT
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
        """)
        self.conn.commit()

    def __enter__(self) -> "Database":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
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
        raw_data: str | None = None,
    ) -> None:
        """Upsert peer metadata.

        Args:
            peer_id: Telegram peer ID
            username: Optional username
            title: Display title
            peer_type: Type ('user', 'group', 'channel')
            raw_data: Optional JSON dump of entity
        """
        self.conn.execute("""
            INSERT INTO peers (id, username, title, type, raw_data)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username = excluded.username,
                title = excluded.title,
                type = excluded.type,
                raw_data = COALESCE(excluded.raw_data, peers.raw_data)
        """, (peer_id, username, title, peer_type, raw_data))

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
            sqlite3.IntegrityError: For non-duplicate constraint violations
        """
        if date is None:
            raise ValueError(f"Message {msg_id} has no date - cannot insert")

        date_ms = datetime_to_epoch_ms(date)

        try:
            cursor = self.conn.execute("""
                INSERT INTO messages
                (id, peer_id, date_utc_ms, sender_id, sender_name, text,
                 reply_to_msg_id, has_media, media_type, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                msg_id, peer_id, date_ms,
                sender_id, sender_name, text, reply_to_msg_id,
                1 if has_media else 0, media_type, raw_data
            ))
            return cursor.rowcount == 1
        except sqlite3.IntegrityError as e:
            # Check if it's a primary key (duplicate) error
            if "UNIQUE constraint failed" in str(e) or "PRIMARY KEY" in str(e):
                return False
            # Re-raise other integrity errors (FK violations, etc.)
            raise

    def insert_messages_batch(self, messages: list[dict]) -> tuple[int, list[str]]:
        """Insert multiple messages in a batch using executemany.

        Args:
            messages: List of message dicts with keys matching insert_message args

        Returns:
            Tuple of (inserted_count, list of error messages for non-duplicate failures)

        Raises:
            ValueError: If any message has no date
        """
        if not messages:
            return (0, [])

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

        # Use INSERT OR IGNORE for duplicates, but we track what was actually inserted
        # by comparing row counts before and after
        errors: list[str] = []
        inserted = 0

        # Process in a transaction for efficiency
        for row in rows:
            try:
                cursor = self.conn.execute("""
                    INSERT INTO messages
                    (id, peer_id, date_utc_ms, sender_id, sender_name, text,
                     reply_to_msg_id, has_media, media_type, raw_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
                if cursor.rowcount == 1:
                    inserted += 1
            except sqlite3.IntegrityError as e:
                err_str = str(e)
                if "UNIQUE constraint failed" in err_str or "PRIMARY KEY" in err_str:
                    # Duplicate - expected, skip silently
                    pass
                else:
                    # Other constraint error - log it
                    errors.append(f"Message {row[0]}: {err_str}")

        return (inserted, errors)

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
        # Fix: explicitly check for None vs 0
        limit_clause = f"LIMIT {limit}" if limit is not None else ""

        query = f"""
            SELECT * FROM messages
            WHERE {' AND '.join(conditions)}
            ORDER BY date_utc_ms {order}, id {order}
            {limit_clause}
        """

        cursor = self.conn.execute(query, params)
        yield from cursor

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
            last_n: Get last N messages
            since_id: Minimum message ID (exclusive)
            until_id: Maximum message ID (exclusive)
            start_date: Start datetime (inclusive, UTC)
            end_date: End datetime (inclusive, UTC)

        Yields:
            Database rows in chronological order
        """
        if last_n is not None:
            # Get the last N messages, then return in chronological order
            rows = list(self.get_messages(
                peer_id, limit=last_n, order_desc=True
            ))
            # Reverse to get chronological order
            yield from reversed(rows)
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

