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

# Telegram API constants
MSGS_PER_REQUEST = 100  # Telegram's limit per GetHistory request
REQUESTS_PER_MINUTE = 30  # Conservative estimate to avoid FloodWait


def _format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string like "2h 30m" or "45m" or "30s"
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes}m"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        if minutes > 0:
            return f"{hours}h {minutes}m"
        return f"{hours}h"


def _estimate_sync_time(message_count: int) -> tuple[int, float]:
    """Estimate sync time for a given message count.

    Args:
        message_count: Number of messages to fetch

    Returns:
        Tuple of (requests_needed, estimated_seconds)
    """
    requests_needed = (message_count + MSGS_PER_REQUEST - 1) // MSGS_PER_REQUEST
    # Add 20% buffer for FloodWaits
    minutes_needed = (requests_needed / REQUESTS_PER_MINUTE) * 1.2
    return requests_needed, minutes_needed * 60

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
        # Use print here as logger may not be fully configured in signal context
        # and we want immediate user feedback
        logger.warning("\nInterrupted! Requesting graceful shutdown...")
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

    Commits and closes DB first (synchronous, cannot be interrupted by
    CancelledError), then disconnects client.
    """
    global _current_db, _current_client

    # Commit and close database first (synchronous - cannot be interrupted)
    # This ensures data is saved even if client disconnect gets cancelled
    if _current_db is not None:
        try:
            _current_db.commit()
            _current_db.close()
            logger.info("Database saved and closed.")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
        finally:
            _current_db = None

    # Disconnect client (may involve network I/O)
    if _current_client is not None:
        try:
            if _current_client.is_connected():
                await _current_client.disconnect()
                logger.info("Client disconnected.")
        except BaseException as e:
            # Catch BaseException to handle CancelledError too
            logger.debug(f"Error disconnecting client: {e}")
        finally:
            _current_client = None


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
            logger.info("Shutdown requested, waiting for task to finish...")

            # Give the task time to finish gracefully
            try:
                result = await asyncio.wait_for(main_task, timeout=SHUTDOWN_TIMEOUT)
                return result
            except asyncio.TimeoutError:
                logger.warning(f"Task did not finish within {SHUTDOWN_TIMEOUT}s, cancelling...")
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
        logger.warning("Operation cancelled.")
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

    # stats command
    stats_parser = subparsers.add_parser(
        "stats",
        help="Show chat statistics (total messages, date range, etc.)",
    )
    stats_parser.add_argument(
        "--peer",
        type=str,
        required=True,
        help="Peer identifier (@username, t.me link, or peer ID)",
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
        logger.warning("\nInterrupted! Cleaning up...")
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
                logger.warning("\nInterrupted!")
                return 130

        if args.command == "dialogs":
            try:
                return asyncio.run(list_dialogs(search=args.search, limit=args.limit))
            except KeyboardInterrupt:
                logger.warning("\nInterrupted!")
                return 130

        if args.command == "fetch-test":
            try:
                return asyncio.run(fetch_test(peer_input=args.peer, limit=args.limit))
            except KeyboardInterrupt:
                logger.warning("\nInterrupted!")
                return 130

        if args.command == "stats":
            try:
                return asyncio.run(show_stats(peer_input=args.peer))
            except KeyboardInterrupt:
                logger.warning("\nInterrupted!")
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
        logger.error(f"Error: {e}")
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


async def show_stats(peer_input: str) -> int:
    """Show statistics for a chat.

    Displays:
    - Total message count in the chat (from Telegram)
    - First and last message dates
    - Messages per day estimate
    - Local coverage (if any data synced)

    Args:
        peer_input: Peer identifier

    Returns:
        Exit code
    """
    from tgx.db import Database, epoch_ms_to_datetime

    client = create_client()

    try:
        await ensure_authorized(client)

        # Normalize and resolve peer
        normalized_peer = normalize_peer_input(peer_input)

        print(f"Fetching stats for {peer_input}...")
        print()

        try:
            input_entity = await client.get_input_entity(normalized_peer)
            entity = await client.get_entity(input_entity)
        except ValueError:
            print(f"Error: Could not find entity '{peer_input}'")
            return 1

        peer_id = get_peer_id(entity)
        title = get_display_name(entity)

        print(f"Chat: {title}")
        print(f"Peer ID: {peer_id}")
        print()

        # Get total count and latest message
        # iter_messages with limit=0 returns total count
        async for msg in client.iter_messages(entity, limit=1):
            latest_msg = msg
            break
        else:
            print("No messages in this chat.")
            return 0

        # Get message count (Telethon provides this)
        # We need to make a request that returns the total
        messages = await client.get_messages(entity, limit=0)
        total_count = messages.total if hasattr(messages, 'total') else None

        # Get the first message (oldest)
        first_msg = None
        async for msg in client.iter_messages(entity, limit=1, reverse=True):
            first_msg = msg
            break

        print("=== Telegram Stats ===")
        if total_count:
            print(f"Total messages: ~{total_count:,}")
        print(f"Latest message ID: {latest_msg.id}")
        print(f"Latest message date: {latest_msg.date.strftime('%Y-%m-%d %H:%M:%S')}")

        if first_msg:
            print(f"First message ID: {first_msg.id}")
            print(f"First message date: {first_msg.date.strftime('%Y-%m-%d %H:%M:%S')}")

            # Calculate chat age and message density
            from datetime import datetime, timezone
            chat_age = datetime.now(timezone.utc) - first_msg.date.replace(tzinfo=timezone.utc)
            days = chat_age.days or 1

            if total_count:
                msgs_per_day = total_count / days
                print()
                print(f"Chat age: {days:,} days ({days // 365} years, {(days % 365) // 30} months)")
                print(f"Avg messages/day: {msgs_per_day:.1f}")

        # Check local coverage first (needed for remaining estimate)
        local_count = 0
        try:
            db = Database()
            local_count = db.count_messages(peer_id)
            summary = db.get_coverage_summary(peer_id) if local_count > 0 else None
            db.close()
        except Exception:
            summary = None

        # Sync time estimation
        if total_count:
            print()
            print("=== Sync Estimate ===")

            remaining = max(0, total_count - local_count)
            if local_count > 0 and remaining > 0:
                # Show remaining estimate
                requests_needed, est_seconds = _estimate_sync_time(remaining)
                print(f"Remaining to sync: ~{remaining:,} messages")
                print(f"Requests needed: ~{requests_needed:,} ({MSGS_PER_REQUEST} msgs/request)")
                print(f"Estimated time: {_format_duration(est_seconds)}")
            elif local_count > 0:
                print("Chat fully synced!")
            else:
                # Full sync estimate
                requests_needed, est_seconds = _estimate_sync_time(total_count)
                print(f"Full sync: ~{total_count:,} messages")
                print(f"Requests needed: ~{requests_needed:,} ({MSGS_PER_REQUEST} msgs/request)")
                print(f"Estimated time: {_format_duration(est_seconds)}")

            print(f"(Based on ~{REQUESTS_PER_MINUTE} req/min with 20% FloodWait buffer)")

        # Show local coverage
        print()
        print("=== Local Coverage ===")
        if local_count > 0 and summary:
            print(f"Messages in DB: {local_count:,}")

            if total_count:
                coverage_pct = (local_count / total_count) * 100
                print(f"Coverage: {coverage_pct:.1f}%")

            print(f"Synced ranges: {summary['total_ranges']}")
            if summary['has_gaps']:
                print(f"Has gaps: Yes ({len(summary['gaps'])} gaps)")
            else:
                print("Has gaps: No")

            if summary.get('oldest_date') and summary.get('newest_date'):
                oldest = summary['oldest_date'].strftime('%Y-%m-%d %H:%M')
                newest = summary['newest_date'].strftime('%Y-%m-%d %H:%M')
                print(f"Date range: {oldest} → {newest}")
        else:
            print("No local data synced yet.")

        return 0

    except ValueError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await client.disconnect()


async def _sync_date_range_with_gaps(
    client,
    db: Database,
    entity,
    peer_id: int,
    start_dt: datetime,
    end_dt: datetime,
    shutdown_event: asyncio.Event | None,
    store_raw: bool,
) -> None:
    """Sync a specific date range, fetching ONLY what's needed.

    This is the gap-aware sync strategy:
    1. Check existing coverage for the requested date range
    2. Identify gaps (parts of the range we don't have)
    3. Fetch ONLY the gaps - NOT everything from now to start

    This allows fetching an old date range without fetching all
    intermediate messages.

    Args:
        client: Authenticated TelegramClient
        db: Database instance
        entity: Resolved Telethon entity
        peer_id: Peer ID
        start_dt: Start of date range (UTC)
        end_dt: End of date range (UTC)
        shutdown_event: Optional shutdown event
        store_raw: Whether to store raw JSON
    """

    def _should_shutdown() -> bool:
        return shutdown_event is not None and shutdown_event.is_set()

    # Check what we already have for this date range
    gaps = db.find_gaps_in_date_range(peer_id, start_dt, end_dt)

    if not gaps:
        logger.info("Date range fully covered, no sync needed")
        return

    # Report what we need to fetch
    if len(gaps) == 1 and gaps[0][0] == start_dt and gaps[0][1] == end_dt:
        # Entire range is missing
        logger.info(f"Fetching: {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')}")
    else:
        logger.info(f"Found {len(gaps)} gap(s) to fill:")
        for gap_start, gap_end, approx_start_id, approx_end_id in gaps:
            gap_start_str = gap_start.strftime("%Y-%m-%d %H:%M")
            gap_end_str = gap_end.strftime("%Y-%m-%d %H:%M")
            logger.info(f"  {gap_start_str} → {gap_end_str}")

    # Fetch each gap (or the whole range if no prior coverage)
    for i, (gap_start, gap_end, approx_start_id, approx_end_id) in enumerate(gaps, 1):
        if _should_shutdown():
            logger.info("Shutdown requested, stopping sync")
            return

        if len(gaps) > 1:
            logger.info(f"Fetching gap {i}/{len(gaps)}...")

        # Fetch ONLY this date range using offset_date
        await _fetch_date_range(
            client=client,
            db=db,
            entity=entity,
            peer_id=peer_id,
            start_date=gap_start,
            end_date=gap_end,
            shutdown_event=shutdown_event,
            store_raw=store_raw,
        )

    if len(gaps) > 1:
        logger.info("All gaps filled!")


async def _fetch_date_range(
    client,
    db: Database,
    entity,
    peer_id: int,
    start_date: datetime,
    end_date: datetime,
    shutdown_event: asyncio.Event | None,
    store_raw: bool,
    batch_size: int = 100,
) -> int:
    """Fetch messages within a specific date range.

    Uses offset_date to start from end_date and works backwards until start_date.

    Args:
        client: TelegramClient
        db: Database
        entity: Telethon entity
        peer_id: Peer ID
        start_date: Start of range (UTC)
        end_date: End of range (UTC)
        shutdown_event: Shutdown event
        store_raw: Whether to store raw JSON
        batch_size: Commit batch size

    Returns:
        Number of messages fetched
    """
    from tgx.db import datetime_to_epoch_ms
    from tgx.sync import (
        _flush_batch,
        classify_peer_type,
        message_to_dict,
    )

    def _should_shutdown() -> bool:
        return shutdown_event is not None and shutdown_event.is_set()

    title = get_display_name(entity)
    username = getattr(entity, "username", None)
    peer_type = classify_peer_type(entity)

    # Ensure peer record exists
    db.update_peer(peer_id, username, title, peer_type)
    db.commit()

    sender_cache: dict[int, str | None] = {}
    stats = {"total_inserted": 0}
    batch: list[dict] = []
    total_fetched = 0

    # Track progress
    import time
    start_time = time.time()
    last_progress_time = start_time

    # Fetch messages using offset_date (starts from end_date, goes backwards)
    async for msg in client.iter_messages(
        entity,
        offset_date=end_date,
        reverse=False,  # Newest first, going backwards
        wait_time=1,
    ):
        # Stop if we've gone past start_date
        if msg.date.replace(tzinfo=timezone.utc) < start_date:
            break

        if _should_shutdown():
            break

        batch.append(await message_to_dict(msg, peer_id, sender_cache, title, store_raw))
        total_fetched += 1

        if len(batch) >= batch_size:
            _flush_batch(db, batch, stats)

            # Show progress with timing info
            elapsed = time.time() - start_time
            rate = total_fetched / elapsed if elapsed > 0 else 0

            batch_dates = [m["date"] for m in batch if m["date"]]
            if batch_dates:
                oldest = min(batch_dates).strftime("%Y-%m-%d %H:%M")
                newest = max(batch_dates).strftime("%Y-%m-%d %H:%M")
                target = start_date.strftime("%Y-%m-%d %H:%M")
                logger.info(
                    f"  +{len(batch)} msgs [{oldest} → {newest}] "
                    f"→ target: {target} ({total_fetched} total, {rate:.0f} msg/s)"
                )
            batch = []

    # Flush remaining
    if batch:
        _flush_batch(db, batch, stats)
        batch_dates = [m["date"] for m in batch if m["date"]]
        if batch_dates:
            oldest = min(batch_dates).strftime("%Y-%m-%d %H:%M")
            newest = max(batch_dates).strftime("%Y-%m-%d %H:%M")
            logger.info(f"  +{len(batch)} msgs [{oldest} → {newest}]")

    # Register this fetch as a sync range (if we inserted anything)
    if stats["total_inserted"] > 0 and stats.get("session_min_id") is not None:
        db.add_sync_range(
            peer_id=peer_id,
            min_msg_id=stats["session_min_id"],
            max_msg_id=stats["session_max_id"],
            min_date_utc_ms=stats["session_min_date_ms"],
            max_date_utc_ms=stats["session_max_date_ms"],
            message_count=stats["total_inserted"],
        )
        db.commit()

    # Summary with timing
    elapsed = time.time() - start_time
    rate = total_fetched / elapsed if elapsed > 0 else 0
    logger.info(f"Fetched: {total_fetched} msgs, {stats['total_inserted']} new in {_format_duration(elapsed)} ({rate:.0f} msg/s)")
    return stats["total_inserted"]


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
    # - With date/ID filters: sync until boundary (no minimum count)
    # - No filters: sync a reasonable default (1000)
    if last_n is not None:
        target_count = last_n
    elif start_date or end_date or since_id or until_id:
        # Date/ID filters specified - sync until boundary, no minimum count
        target_count = 0
    else:
        target_count = 1000

    # Derive sync boundaries from whichever bound is provided
    # If only --end is given, we still need to backfill to have messages up to that point
    # If only --until-id is given, we need messages up to that ID
    sync_min_date = start_dt or end_dt
    sync_min_id = since_id or until_id

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

        # Step 2: Sync - strategy depends on whether date range is specified
        logger.info("--- Syncing ---")

        if start_dt and end_dt:
            # Date range mode: check coverage and fill gaps
            await _sync_date_range_with_gaps(
                client=client,
                db=db,
                entity=entity,
                peer_id=peer_id,
                start_dt=start_dt,
                end_dt=end_dt,
                shutdown_event=_shutdown_event,
                store_raw=store_raw,
            )
        else:
            # Standard mode: tail sync + backfill to boundary
            await sync_peer(
                client=client,
                db=db,
                target_count=target_count,
                min_date=sync_min_date,   # Backfill until we have messages at this date
                min_id=sync_min_id,       # Or until we have messages at this ID
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

