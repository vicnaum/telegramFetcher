"""Export messages from database to TXT and JSONL formats."""

import json
import logging
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from tgx.db import Database, epoch_ms_to_datetime
from tgx.utils import flatten_text

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_local_timezone():
    """Get the local timezone (cached).

    Uses lru_cache to avoid repeated system calls.
    The cache persists for the lifetime of the process.
    """
    return datetime.now().astimezone().tzinfo


def utc_to_local(utc_dt: datetime) -> datetime:
    """Convert UTC datetime to local timezone.

    Args:
        utc_dt: UTC datetime (may be naive or aware)

    Returns:
        Local datetime
    """
    if utc_dt.tzinfo is None:
        # Assume UTC if naive
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)

    # Use cached timezone
    return utc_dt.astimezone(get_local_timezone())


def get_datetime_from_row(row) -> datetime | None:
    """Get datetime from a database row (handles epoch_ms format).

    Args:
        row: Database row with date_utc_ms field

    Returns:
        UTC-aware datetime or None
    """
    date_ms = row["date_utc_ms"]
    if date_ms is None:
        return None
    return epoch_ms_to_datetime(date_ms)


def format_txt_line(row) -> str:
    """Format a database row as a TXT line.

    Uses reply_sender_name from the row (via LEFT JOIN in the query).

    Format: "[msg_id] YYYY-MM-DD HH:MM:SS | sender | text"
    With reply: "[msg_id] YYYY-MM-DD HH:MM:SS | sender | [reply to #123 @name] text"

    Args:
        row: Database row with date_utc_ms and reply_sender_name fields

    Returns:
        Formatted line
    """
    msg_id = row["id"]

    # Get datetime from epoch_ms and convert to local
    dt = get_datetime_from_row(row)

    if dt:
        local_dt = utc_to_local(dt)
        time_str = local_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        time_str = "unknown"

    # Get sender
    sender = row["sender_name"]
    if not sender:
        sender_id = row["sender_id"]
        sender = f"user_{sender_id}" if sender_id else "channel"

    # Get text (flatten newlines)
    text = flatten_text(row["text"])
    if not text:
        if row["has_media"]:
            media_type = row["media_type"] or "media"
            text = f"[{media_type}]"
        else:
            text = "[empty]"

    # Handle reply (use reply_sender_name from JOIN)
    reply_to_msg_id = row["reply_to_msg_id"]
    if reply_to_msg_id:
        reply_sender = ""
        # reply_sender_name comes from the LEFT JOIN
        reply_sender_name = row["reply_sender_name"]
        if reply_sender_name:
            reply_sender = f" @{reply_sender_name}"
        text = f"[reply to #{reply_to_msg_id}{reply_sender}] {text}"

    return f"[{msg_id}] {time_str} | {sender} | {text}"


def format_jsonl_line(
    row,
    include_raw: bool = False,
    raw_as_string: bool = False
) -> str:
    """Format a database row as a JSONL line.

    Args:
        row: Database row (sqlite3.Row) with date_utc_ms field
        include_raw: Whether to include raw_data field
        raw_as_string: If True, emit raw_data as string; otherwise parse to object

    Returns:
        JSON string
    """
    # Convert epoch_ms to ISO8601 for export (human-readable)
    dt = get_datetime_from_row(row)
    date_iso = dt.isoformat() if dt else None

    data: dict = {
        "id": row["id"],
        "peer_id": row["peer_id"],
        "date": date_iso,
        "date_utc_ms": row["date_utc_ms"],
        "sender_id": row["sender_id"],
        "sender_name": row["sender_name"],
        "text": row["text"],
        "reply_to_msg_id": row["reply_to_msg_id"],
        "has_media": bool(row["has_media"]),
        "media_type": row["media_type"],
    }

    if include_raw:
        raw_text = row["raw_data"]
        if raw_as_string:
            # Emit as string for debugging/round-trip
            data["raw_data"] = raw_text
        else:
            # Parse to object (default) for downstream tooling
            if raw_text:
                try:
                    data["raw_data"] = json.loads(raw_text)
                except json.JSONDecodeError as e:
                    # Fallback: emit with error info
                    data["raw_data"] = {
                        "_raw_data_text": raw_text,
                        "_raw_data_parse_error": str(e),
                    }
                    logger.warning(f"Failed to parse raw_data for msg {row['id']}: {e}")
            else:
                data["raw_data"] = None

    return json.dumps(data, ensure_ascii=False)


def export_txt(
    db: Database,
    peer_id: int,
    output_path: str | Path,
    last_n: int | None = None,
    since_id: int | None = None,
    until_id: int | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> int:
    """Export messages to TXT format.

    Uses streaming export with SQL JOIN to get reply sender info efficiently.
    Does not materialize all rows in memory.

    Args:
        db: Database instance
        peer_id: Peer ID to export
        output_path: Output file path
        last_n: Export last N messages
        since_id: Minimum message ID (exclusive)
        until_id: Maximum message ID (exclusive)
        start_date: Start datetime (UTC)
        end_date: End datetime (UTC)

    Returns:
        Number of messages exported
    """
    output_path = Path(output_path)
    count = 0

    # Stream export with LEFT JOIN for reply sender
    with open(output_path, "w", encoding="utf-8") as f:
        for row in db.get_messages_for_export_with_reply_sender(
            peer_id,
            last_n=last_n,
            since_id=since_id,
            until_id=until_id,
            start_date=start_date,
            end_date=end_date,
        ):
            line = format_txt_line(row)
            f.write(line + "\n")
            count += 1

    return count


def export_jsonl(
    db: Database,
    peer_id: int,
    output_path: str | Path,
    last_n: int | None = None,
    since_id: int | None = None,
    until_id: int | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    include_raw: bool = False,
    raw_as_string: bool = False,
) -> int:
    """Export messages to JSONL format.

    Args:
        db: Database instance
        peer_id: Peer ID to export
        output_path: Output file path
        last_n: Export last N messages
        since_id: Minimum message ID (exclusive)
        until_id: Maximum message ID (exclusive)
        start_date: Start datetime (UTC)
        end_date: End datetime (UTC)
        include_raw: Whether to include raw_data field
        raw_as_string: If True, emit raw_data as string; otherwise parse to object

    Returns:
        Number of messages exported
    """
    output_path = Path(output_path)
    count = 0

    with open(output_path, "w", encoding="utf-8") as f:
        for row in db.get_messages_for_export(
            peer_id,
            last_n=last_n,
            since_id=since_id,
            until_id=until_id,
            start_date=start_date,
            end_date=end_date,
        ):
            line = format_jsonl_line(row, include_raw=include_raw, raw_as_string=raw_as_string)
            f.write(line + "\n")
            count += 1

    return count


def export_messages(
    db: Database,
    peer_id: int,
    txt_path: str | Path | None = None,
    jsonl_path: str | Path | None = None,
    last_n: int | None = None,
    since_id: int | None = None,
    until_id: int | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    include_raw: bool = False,
    raw_as_string: bool = False,
) -> dict[str, int]:
    """Export messages to one or more formats.

    Args:
        db: Database instance
        peer_id: Peer ID to export
        txt_path: Output path for TXT (optional)
        jsonl_path: Output path for JSONL (optional)
        last_n: Export last N messages
        since_id: Minimum message ID (exclusive)
        until_id: Maximum message ID (exclusive)
        start_date: Start datetime (UTC)
        end_date: End datetime (UTC)
        include_raw: Whether to include raw_data in JSONL
        raw_as_string: If True, emit raw_data as string (for debugging)

    Returns:
        Dict with counts per format: {"txt": N, "jsonl": M}
    """
    results = {}

    if txt_path:
        count = export_txt(
            db, peer_id, txt_path,
            last_n=last_n,
            since_id=since_id,
            until_id=until_id,
            start_date=start_date,
            end_date=end_date,
        )
        results["txt"] = count
        logger.info(f"Exported {count} messages to {txt_path}")

    if jsonl_path:
        count = export_jsonl(
            db, peer_id, jsonl_path,
            last_n=last_n,
            since_id=since_id,
            until_id=until_id,
            start_date=start_date,
            end_date=end_date,
            include_raw=include_raw,
            raw_as_string=raw_as_string,
        )
        results["jsonl"] = count
        logger.info(f"Exported {count} messages to {jsonl_path}")

    return results
