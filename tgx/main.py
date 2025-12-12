"""CLI entry point for tgx."""

import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from telethon import TelegramClient

from tgx.client import auth_test, create_client, ensure_authorized, fetch_test, list_dialogs
from tgx.db import Database
from tgx.exporter import export_messages
from tgx.sync import sync_peer
from tgx.utils import get_display_name, get_peer_id

logger = logging.getLogger(__name__)

# Global references for graceful shutdown
_current_db: Database | None = None
_current_client: TelegramClient | None = None


def _signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    print("\n\nInterrupted! Cleaning up...")

    # Disconnect Telethon client (best-effort)
    if _current_client is not None:
        try:
            # Note: We can't await in a signal handler, so we do sync disconnect
            # This is best-effort - the client may not cleanly disconnect
            if _current_client.is_connected():
                _current_client.disconnect()
                print("Client disconnected.")
        except Exception:
            pass

    # Save and close database
    if _current_db is not None:
        try:
            _current_db.commit()
            _current_db.close()
            print("Database saved and closed.")
        except Exception:
            pass

    sys.exit(130)


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="tgx",
        description="Personal Telegram archiver/exporter CLI",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # auth-test command
    auth_parser = subparsers.add_parser(
        "auth-test",
        help="Test authentication and print user info",
    )
    auth_parser.add_argument(
        "--phone",
        action="store_true",
        help="Use phone number login instead of QR code",
    )

    # dialogs command
    dialogs_parser = subparsers.add_parser(
        "dialogs",
        help="List dialogs (chats/channels)",
    )
    dialogs_parser.add_argument(
        "--search",
        type=str,
        default=None,
        help="Filter dialogs by title or username",
    )
    dialogs_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of dialogs to show (default: 20)",
    )

    # fetch-test command
    fetch_parser = subparsers.add_parser(
        "fetch-test",
        help="Fetch a few messages from a peer for testing",
    )
    fetch_parser.add_argument(
        "--peer",
        type=str,
        required=True,
        help="Peer identifier (@username, t.me link, or peer ID)",
    )
    fetch_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of messages to fetch (default: 5)",
    )

    # sync command
    sync_parser = subparsers.add_parser(
        "sync",
        help="Sync messages from a peer to local database",
    )
    sync_parser.add_argument(
        "--peer",
        type=str,
        required=True,
        help="Peer identifier (@username, t.me link, or peer ID)",
    )
    sync_parser.add_argument(
        "--last",
        type=int,
        default=100,
        help="Target number of messages to have in DB (default: 100)",
    )

    # export command
    export_parser = subparsers.add_parser(
        "export",
        help="Sync and export messages from a peer",
    )
    export_parser.add_argument(
        "--peer",
        type=str,
        required=True,
        help="Peer identifier (@username, t.me link, or peer ID)",
    )
    export_parser.add_argument(
        "--last",
        type=int,
        default=None,
        help="Export last N messages",
    )
    export_parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS, local timezone)",
    )
    export_parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS, local timezone)",
    )
    export_parser.add_argument(
        "--since-id",
        type=int,
        default=None,
        help="Export messages with ID > this value",
    )
    export_parser.add_argument(
        "--until-id",
        type=int,
        default=None,
        help="Export messages with ID < this value",
    )
    export_parser.add_argument(
        "--txt",
        type=str,
        default=None,
        help="Output path for TXT format",
    )
    export_parser.add_argument(
        "--jsonl",
        type=str,
        default=None,
        help="Output path for JSONL format",
    )
    export_parser.add_argument(
        "--include-raw",
        action="store_true",
        help="Include raw_data in JSONL output",
    )
    export_parser.add_argument(
        "--raw-as-string",
        action="store_true",
        help="Emit raw_data as JSON string instead of parsed object (for debugging)",
    )

    return parser


def main() -> int:
    """Main entry point."""
    # Load .env file if present (before accessing any config)
    load_dotenv()

    # Set up basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)

    parser = create_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "auth-test":
        return asyncio.run(auth_test(use_phone=args.phone))

    if args.command == "dialogs":
        return asyncio.run(list_dialogs(search=args.search, limit=args.limit))

    if args.command == "fetch-test":
        return asyncio.run(fetch_test(peer_input=args.peer, limit=args.limit))

    if args.command == "sync":
        return asyncio.run(run_sync(peer_input=args.peer, target_count=args.last))

    if args.command == "export":
        return asyncio.run(run_export(
            peer_input=args.peer,
            last_n=args.last,
            start_date=args.start,
            end_date=args.end,
            since_id=args.since_id,
            until_id=args.until_id,
            txt_path=args.txt,
            jsonl_path=args.jsonl,
            include_raw=args.include_raw,
            raw_as_string=args.raw_as_string,
        ))

    return 0


def parse_local_datetime(date_str: str | None) -> datetime | None:
    """Parse a local datetime string to UTC.

    Args:
        date_str: Date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)

    Returns:
        UTC datetime or None
    """
    if not date_str:
        return None


    # Try parsing with time
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Assume local timezone, convert to UTC
            local_dt = dt.astimezone()  # Add local TZ info
            utc_dt = local_dt.astimezone(timezone.utc)
            return utc_dt
        except ValueError:
            continue

    raise ValueError(f"Could not parse date: {date_str}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")


async def run_export(
    peer_input: str,
    last_n: int | None,
    start_date: str | None,
    end_date: str | None,
    since_id: int | None,
    until_id: int | None,
    txt_path: str | None,
    jsonl_path: str | None,
    include_raw: bool,
    raw_as_string: bool = False,
) -> int:
    """Run export command (sync first, then export).

    Returns:
        Exit code
    """
    global _current_db, _current_client

    if not txt_path and not jsonl_path:
        print("Error: At least one output format required (--txt or --jsonl)")
        return 1

    # Parse dates
    try:
        start_dt = parse_local_datetime(start_date)
        end_dt = parse_local_datetime(end_date)
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    # Determine target count for sync
    # If --last is specified, sync at least that many
    # Otherwise sync a reasonable default
    target_count = last_n if last_n else 1000

    client = create_client()
    db = Database()
    _current_db = db
    _current_client = client

    try:
        await ensure_authorized(client)

        # Step 1: Resolve peer to get peer_id
        print(f"Resolving peer: {peer_input}...")
        try:
            input_entity = await client.get_input_entity(peer_input)
            entity = await client.get_entity(input_entity)
        except ValueError:
            print(f"Error: Could not find entity '{peer_input}'")
            return 1

        peer_id = get_peer_id(entity)
        title = get_display_name(entity)
        print(f"Resolved: {title} (peer_id: {peer_id})")

        # Step 2: Sync
        print("\n--- Syncing ---")
        await sync_peer(
            client=client,
            db=db,
            peer_input=peer_input,
            target_count=target_count,
        )

        # Step 3: Export from DB
        print("\n--- Exporting ---")
        export_messages(
            db=db,
            peer_id=peer_id,
            txt_path=txt_path,
            jsonl_path=jsonl_path,
            last_n=last_n,
            since_id=since_id,
            until_id=until_id,
            start_date=start_dt,
            end_date=end_dt,
            include_raw=include_raw,
            raw_as_string=raw_as_string,
        )

        print("\nExport complete!")
        return 0

    except ValueError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await client.disconnect()
        db.close()


async def run_sync(peer_input: str, target_count: int) -> int:
    """Run sync command.

    Args:
        peer_input: Peer identifier
        target_count: Target number of messages

    Returns:
        Exit code
    """
    global _current_db, _current_client

    client = create_client()
    db = Database()
    _current_db = db
    _current_client = client

    try:
        await ensure_authorized(client)

        await sync_peer(
            client=client,
            db=db,
            peer_input=peer_input,
            target_count=target_count,
        )

        return 0
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await client.disconnect()
        db.close()


if __name__ == "__main__":
    sys.exit(main())

