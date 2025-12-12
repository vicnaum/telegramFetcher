"""CLI entry point for tgx."""

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient

from tgx.client import (
    ConfigurationError,
    auth_test,
    create_client,
    ensure_authorized,
    fetch_test,
    list_dialogs,
)
from tgx.db import Database
from tgx.exporter import export_messages
from tgx.sync import sync_peer
from tgx.utils import get_display_name, get_peer_id, normalize_peer_input

logger = logging.getLogger(__name__)

# Shutdown timeout for task cancellation
SHUTDOWN_TIMEOUT = 10.0

# Global references for graceful shutdown
_current_db: Database | None = None
_current_client: TelegramClient | None = None
_shutdown_event: asyncio.Event | None = None


def _setup_signal_handlers() -> None:
    """Set up signal handlers for graceful shutdown in async context.

    On Unix: uses loop.add_signal_handler for proper async signal handling.
    On Windows: signal handlers are not supported in event loops, so we rely
    on KeyboardInterrupt handling in the outer wrapper.
    """
    global _shutdown_event

    def handle_signal() -> None:
        """Signal handler that sets the shutdown event."""
        print("\n\nInterrupted! Requesting graceful shutdown...")
        if _shutdown_event is not None:
            _shutdown_event.set()

    try:
        loop = asyncio.get_running_loop()
        # Use asyncio signal handling for proper async cleanup
        loop.add_signal_handler(signal.SIGINT, handle_signal)
        loop.add_signal_handler(signal.SIGTERM, handle_signal)
    except (RuntimeError, NotImplementedError):
        # Windows doesn't support loop.add_signal_handler
        # We handle KeyboardInterrupt in run_async_with_shutdown instead
        logger.debug("Signal handlers not supported on this platform")


async def _cleanup_resources() -> None:
    """Clean up database and client resources.

    Uses asyncio.shield for the final commit to ensure data is saved
    even during cancellation.
    """
    global _current_db, _current_client

    # Disconnect client first (network resource)
    if _current_client is not None:
        try:
            if _current_client.is_connected():
                await _current_client.disconnect()
                print("Client disconnected.")
        except Exception as e:
            logger.debug(f"Error disconnecting client: {e}")
        finally:
            _current_client = None

    # Commit and close database (must be on same thread as connection was created)
    if _current_db is not None:
        try:
            _current_db.commit()
            _current_db.close()
            print("Database saved and closed.")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
        finally:
            _current_db = None


async def run_with_graceful_shutdown(
    coro: Coroutine[Any, Any, int],
) -> int:
    """Run a coroutine with proper two-phase graceful shutdown.

    Phase 1: On signal, set shutdown_event and let the task check it
    Phase 2: If task doesn't exit within timeout, cancel it

    The shutdown_event is passed to sync_peer so it can break loops cleanly.

    Args:
        coro: Coroutine to run (should return int exit code)

    Returns:
        Exit code from the coroutine, or 130 if interrupted
    """
    global _shutdown_event
    _shutdown_event = asyncio.Event()
    _setup_signal_handlers()

    # Create task for main work
    main_task = asyncio.create_task(coro)

    try:
        # Wait for either: task completion OR shutdown signal
        shutdown_waiter = asyncio.create_task(_shutdown_event.wait())

        done, pending = await asyncio.wait(
            [main_task, shutdown_waiter],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # If shutdown was signaled (not task completion)
        if shutdown_waiter in done and main_task in pending:
            print("Shutdown requested, waiting for task to finish...")

            # Give the task time to finish gracefully
            try:
                result = await asyncio.wait_for(main_task, timeout=SHUTDOWN_TIMEOUT)
                return result
            except asyncio.TimeoutError:
                print(f"Task did not finish within {SHUTDOWN_TIMEOUT}s, cancelling...")
                main_task.cancel()
                try:
                    await main_task
                except asyncio.CancelledError:
                    pass
                return 130  # Standard exit code for SIGINT

        # Clean up the shutdown waiter if task finished first
        if shutdown_waiter in pending:
            shutdown_waiter.cancel()
            try:
                await shutdown_waiter
            except asyncio.CancelledError:
                pass

        # Task completed normally
        if main_task in done:
            return main_task.result()

        return 0

    except asyncio.CancelledError:
        print("Operation cancelled.")
        return 130
    finally:
        await _cleanup_resources()


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="tgx",
        description="Personal Telegram archiver/exporter CLI",
    )

    # Global verbosity options
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output (debug logging)",
    )
    verbosity.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Quiet mode (only errors and essential output)",
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
    sync_parser.add_argument(
        "--no-store-raw",
        action="store_true",
        help="Don't store raw JSON for each message (reduces DB size)",
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
        help="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
    )
    export_parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
    )
    export_parser.add_argument(
        "--tz",
        type=str,
        default=None,
        help="Timezone for date parsing (e.g., 'America/New_York', 'UTC'). "
             "Defaults to system local timezone.",
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
    export_parser.add_argument(
        "--no-store-raw",
        action="store_true",
        help="Don't store raw JSON for new messages during sync (reduces DB size)",
    )

    return parser


def setup_logging(verbose: bool = False, quiet: bool = False) -> None:
    """Configure logging based on verbosity flags.

    Args:
        verbose: Enable debug-level logging
        quiet: Only show errors and essential output
    """
    if verbose:
        level = logging.DEBUG
        fmt = "%(levelname)s [%(name)s] %(message)s"
    elif quiet:
        level = logging.ERROR
        fmt = "%(message)s"
    else:
        # User-friendly format: just the message for INFO, level prefix for warnings
        level = logging.INFO
        fmt = "%(message)s"

    logging.basicConfig(level=level, format=fmt, force=True)

    # Suppress noisy third-party loggers in non-verbose mode
    if not verbose:
        logging.getLogger("telethon").setLevel(logging.WARNING)


def run_async_with_shutdown(coro: Coroutine[Any, Any, int]) -> int:
    """Run async coroutine with graceful shutdown, handling Windows compatibility.

    This wrapper handles KeyboardInterrupt for Windows where loop.add_signal_handler
    is not supported. On all platforms, it ensures proper cleanup.

    Args:
        coro: Coroutine to run

    Returns:
        Exit code
    """
    try:
        return asyncio.run(run_with_graceful_shutdown(coro))
    except KeyboardInterrupt:
        # Windows fallback: KeyboardInterrupt is raised instead of signal handler
        print("\n\nInterrupted! Cleaning up...")
        # The cleanup already happened in run_with_graceful_shutdown's finally block
        return 130


def main() -> int:
    """Main entry point."""
    # Load .env file if present (before accessing any config)
    load_dotenv()

    parser = create_parser()
    args = parser.parse_args()

    # Set up logging based on verbosity flags
    setup_logging(
        verbose=getattr(args, 'verbose', False),
        quiet=getattr(args, 'quiet', False)
    )

    if args.command is None:
        parser.print_help()
        return 1

    try:
        # Commands that don't need graceful shutdown
        if args.command == "auth-test":
            try:
                return asyncio.run(auth_test(use_phone=args.phone))
            except KeyboardInterrupt:
                print("\n\nInterrupted!")
                return 130

        if args.command == "dialogs":
            try:
                return asyncio.run(list_dialogs(search=args.search, limit=args.limit))
            except KeyboardInterrupt:
                print("\n\nInterrupted!")
                return 130

        if args.command == "fetch-test":
            try:
                return asyncio.run(fetch_test(peer_input=args.peer, limit=args.limit))
            except KeyboardInterrupt:
                print("\n\nInterrupted!")
                return 130

        # Commands that need graceful shutdown (database operations)
        if args.command == "sync":
            return run_async_with_shutdown(
                run_sync(
                    peer_input=args.peer,
                    target_count=args.last,
                    store_raw=not args.no_store_raw,
                )
            )

        if args.command == "export":
            return run_async_with_shutdown(run_export(
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
                tz_name=args.tz,
                store_raw=not args.no_store_raw,
            ))

    except ConfigurationError as e:
        print(f"Error: {e}")
        return 1

    return 0


def parse_local_datetime(
    date_str: str | None,
    is_end: bool = False,
    tz_name: str | None = None,
) -> datetime | None:
    """Parse a datetime string to UTC.

    Args:
        date_str: Date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        is_end: If True and date-only format, set time to end of day (23:59:59.999999)
        tz_name: Explicit timezone name (e.g., 'America/New_York', 'UTC').
                 If None, uses system local timezone.

    Returns:
        UTC datetime or None

    Raises:
        ValueError: If date_str cannot be parsed or tz_name is invalid
    """
    if not date_str:
        return None

    # Get the timezone to use
    if tz_name is not None:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name)
        except (ImportError, KeyError) as e:
            raise ValueError(
                f"Invalid timezone: {tz_name}. "
                "Use IANA timezone names like 'America/New_York' or 'UTC'."
            ) from e
    else:
        # Use system local timezone
        tz = None

    # Try parsing with time
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(date_str, fmt)
            # For date-only format with is_end=True, set to end of day
            if fmt == "%Y-%m-%d" and is_end:
                dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)

            if tz is not None:
                # Use explicit timezone
                dt = dt.replace(tzinfo=tz)
            else:
                # Use local timezone (astimezone on naive datetime adds local TZ)
                dt = dt.astimezone()

            # Convert to UTC
            utc_dt = dt.astimezone(timezone.utc)
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
    tz_name: str | None = None,
    store_raw: bool = True,
) -> int:
    """Run export command (sync first, then export).

    Returns:
        Exit code
    """
    global _current_db, _current_client, _shutdown_event

    if not txt_path and not jsonl_path:
        logger.error("At least one output format required (--txt or --jsonl)")
        return 1

    # Validate mutual exclusivity of --last with date/ID filters
    if last_n is not None and any([start_date, end_date, since_id, until_id]):
        logger.error("--last cannot be combined with date/ID filters (--start, --end, --since-id, --until-id)")
        return 1

    # Parse dates (end date uses end-of-day semantics for date-only input)
    try:
        start_dt = parse_local_datetime(start_date, is_end=False, tz_name=tz_name)
        end_dt = parse_local_datetime(end_date, is_end=True, tz_name=tz_name)
    except ValueError as e:
        logger.error(f"{e}")
        return 1

    # Determine target count for sync
    # - With --last: sync exactly that many
    # - With date/ID filters (start_date, since_id): sync only until boundary (no minimum)
    # - No filters: sync a reasonable default (1000)
    if last_n is not None:
        target_count = last_n
    elif start_date or since_id:
        # Date/ID filters specified - sync until boundary, no minimum count
        target_count = 0
    else:
        target_count = 1000

    client = create_client()
    db = Database()
    _current_db = db
    _current_client = client

    try:
        await ensure_authorized(client)

        # Check for early shutdown
        if _shutdown_event and _shutdown_event.is_set():
            logger.info("Shutdown requested before sync started")
            return 130

        # Normalize peer input (handle t.me links, etc.)
        normalized_peer = normalize_peer_input(peer_input)

        # Step 1: Resolve peer to get peer_id
        logger.info(f"Resolving peer: {peer_input}...")
        try:
            input_entity = await client.get_input_entity(normalized_peer)
            entity = await client.get_entity(input_entity)
        except ValueError:
            logger.error(f"Could not find entity '{peer_input}'")
            return 1

        peer_id = get_peer_id(entity)
        title = get_display_name(entity)
        logger.info(f"Resolved: {title} (peer_id: {peer_id})")

        # Step 2: Sync (with boundary-aware backfill for date/ID filters)
        # Pass resolved entity and shutdown_event for graceful interruption
        logger.info("--- Syncing ---")
        await sync_peer(
            client=client,
            db=db,
            target_count=target_count,
            min_date=start_dt,   # Backfill until we have messages at this date
            min_id=since_id,     # Or until we have messages at this ID
            entity=entity,
            peer_id=peer_id,
            shutdown_event=_shutdown_event,
            store_raw=store_raw,
        )

        # Check for shutdown before export
        if _shutdown_event and _shutdown_event.is_set():
            logger.info("Shutdown requested, skipping export")
            return 130

        # Step 3: Export from DB
        logger.info("--- Exporting ---")
        results = export_messages(
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

        # Report results
        for fmt, count in results.items():
            logger.info(f"Exported {count} messages to {fmt.upper()}")

        logger.info("Export complete!")
        return 0

    except asyncio.CancelledError:
        # Re-raise to let the shutdown handler deal with it
        raise
    except ValueError as e:
        logger.error(f"{e}")
        return 1
    # Note: Cleanup is handled by run_with_graceful_shutdown


async def run_sync(peer_input: str, target_count: int, store_raw: bool = True) -> int:
    """Run sync command.

    Args:
        peer_input: Peer identifier
        target_count: Target number of messages
        store_raw: Whether to store raw JSON for each message

    Returns:
        Exit code
    """
    global _current_db, _current_client, _shutdown_event

    client = create_client()
    db = Database()
    _current_db = db
    _current_client = client

    try:
        await ensure_authorized(client)

        # Check for early shutdown
        if _shutdown_event and _shutdown_event.is_set():
            logger.info("Shutdown requested before sync started")
            return 130

        # Normalize peer input (handle t.me links, etc.)
        normalized_peer = normalize_peer_input(peer_input)

        await sync_peer(
            client=client,
            db=db,
            peer_input=normalized_peer,
            target_count=target_count,
            shutdown_event=_shutdown_event,
            store_raw=store_raw,
        )

        return 0
    except asyncio.CancelledError:
        # Re-raise to let the shutdown handler deal with it
        raise
    except ValueError as e:
        logger.error(f"{e}")
        return 1
    # Note: Cleanup is handled by run_with_graceful_shutdown


if __name__ == "__main__":
    sys.exit(main())

