"""Export messages from database to TXT and JSONL formats."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from tgx.db import Database
from tgx.utils import flatten_text


def get_local_timezone():
    """Get the local timezone."""
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
    
    return utc_dt.astimezone(get_local_timezone())


def parse_iso_datetime(iso_str: str | None) -> datetime | None:
    """Parse ISO datetime string.
    
    Args:
        iso_str: ISO format datetime string
    
    Returns:
        datetime object or None
    """
    if not iso_str:
        return None
    
    try:
        # Handle various ISO formats
        if iso_str.endswith("Z"):
            iso_str = iso_str[:-1] + "+00:00"
        
        return datetime.fromisoformat(iso_str)
    except ValueError:
        return None


def format_txt_line(row, sender_lookup: dict[int, str] | None = None) -> str:
    """Format a database row as a TXT line.
    
    Format: "[msg_id] YYYY-MM-DD HH:MM:SS | sender | text"
    With reply: "[msg_id] YYYY-MM-DD HH:MM:SS | sender | [reply to #123 @name] text"
    
    Args:
        row: Database row (sqlite3.Row)
        sender_lookup: Optional dict mapping msg_id -> sender_name for reply lookups
    
    Returns:
        Formatted line
    """
    msg_id = row["id"]
    
    # Parse date and convert to local
    date_str = row["date"]
    dt = parse_iso_datetime(date_str)
    
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
    
    # Handle reply
    reply_to_msg_id = row["reply_to_msg_id"]
    if reply_to_msg_id:
        # Try to get the sender name of the replied-to message
        reply_sender = ""
        if sender_lookup and reply_to_msg_id in sender_lookup:
            reply_sender = f" @{sender_lookup[reply_to_msg_id]}"
        text = f"[reply to #{reply_to_msg_id}{reply_sender}] {text}"
    
    return f"[{msg_id}] {time_str} | {sender} | {text}"


def format_jsonl_line(row, include_raw: bool = False) -> str:
    """Format a database row as a JSONL line.
    
    Args:
        row: Database row (sqlite3.Row)
        include_raw: Whether to include raw_data field
    
    Returns:
        JSON string
    """
    data = {
        "id": row["id"],
        "peer_id": row["peer_id"],
        "date": row["date"],
        "sender_id": row["sender_id"],
        "sender_name": row["sender_name"],
        "text": row["text"],
        "reply_to_msg_id": row["reply_to_msg_id"],
        "has_media": bool(row["has_media"]),
        "media_type": row["media_type"],
    }
    
    if include_raw:
        data["raw_data"] = row["raw_data"]
    
    return json.dumps(data, ensure_ascii=False)


def build_sender_lookup(db: Database, peer_id: int) -> dict[int, str]:
    """Build a lookup dict from message_id -> sender_name for reply resolution.
    
    Args:
        db: Database instance
        peer_id: Peer ID
    
    Returns:
        Dict mapping message_id to sender_name
    """
    lookup = {}
    for row in db.get_messages(peer_id):
        sender_name = row["sender_name"]
        if sender_name:
            lookup[row["id"]] = sender_name
    return lookup


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
    
    # Build sender lookup for reply resolution
    sender_lookup = build_sender_lookup(db, peer_id)
    
    with open(output_path, "w", encoding="utf-8") as f:
        for row in db.get_messages_for_export(
            peer_id,
            last_n=last_n,
            since_id=since_id,
            until_id=until_id,
            start_date=start_date,
            end_date=end_date,
        ):
            line = format_txt_line(row, sender_lookup=sender_lookup)
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
            line = format_jsonl_line(row, include_raw=include_raw)
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
        print(f"Exported {count} messages to {txt_path}")
    
    if jsonl_path:
        count = export_jsonl(
            db, peer_id, jsonl_path,
            last_n=last_n,
            since_id=since_id,
            until_id=until_id,
            start_date=start_date,
            end_date=end_date,
            include_raw=include_raw,
        )
        results["jsonl"] = count
        print(f"Exported {count} messages to {jsonl_path}")
    
    return results

