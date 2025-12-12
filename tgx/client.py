"""Telethon client wrapper with auth flow."""

import logging
import os
import sys
from getpass import getpass
from io import StringIO
from pathlib import Path

import qrcode
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from tgx.utils import get_display_name, get_peer_id, normalize_peer_input, truncate_text

logger = logging.getLogger(__name__)


def get_session_path() -> str:
    """Get session file path from env or default.

    Expands ~ and resolves to absolute path.
    """
    path = os.environ.get("TGX_SESSION", "./tgx.session")
    return str(Path(path).expanduser().resolve())


def get_api_credentials() -> tuple[int, str]:
    """Get API credentials from environment variables.

    Returns:
        Tuple of (api_id, api_hash)

    Raises:
        SystemExit: If credentials are not set
    """
    api_id = os.environ.get("TGX_API_ID")
    api_hash = os.environ.get("TGX_API_HASH")

    if not api_id or not api_hash:
        print("Error: TGX_API_ID and TGX_API_HASH environment variables must be set.")
        print("Get them from https://my.telegram.org/apps")
        sys.exit(1)

    return int(api_id), api_hash


def create_client(session_path: str | None = None, receive_updates: bool = True) -> TelegramClient:
    """Create a TelegramClient with recommended settings for archiving.

    Args:
        session_path: Path to session file, or None for default
        receive_updates: Whether to receive updates (needed for QR login)

    Returns:
        Configured TelegramClient instance
    """
    if session_path is None:
        session_path = get_session_path()

    api_id, api_hash = get_api_credentials()

    # Note: receive_updates=True is needed for QR code login to work.
    # For archiving/scraping, it's fine to leave it True - we just won't
    # register any handlers, so updates will be ignored anyway.
    client = TelegramClient(
        session_path,
        api_id,
        api_hash,
        flood_sleep_threshold=60,  # Auto-sleep if wait time < 60s
        request_retries=5,         # Retry internal errors 5 times
        receive_updates=receive_updates,
    )

    return client


def display_qr_code(url: str) -> None:
    """Display QR code as ASCII art in terminal.

    Args:
        url: URL to encode in QR code
    """
    qr = qrcode.QRCode(box_size=1, border=1)
    qr.add_data(url)
    qr.make()

    f = StringIO()
    qr.print_ascii(out=f)
    f.seek(0)
    print(f.read())


async def qr_login(client: TelegramClient, timeout: int = 120) -> bool:
    """Attempt QR code login with auto-refresh.

    QR codes expire periodically. This function automatically refreshes them
    until the user scans one or the total timeout is reached.

    Args:
        client: TelegramClient instance
        timeout: Total timeout in seconds for the entire login process

    Returns:
        True if successful, False otherwise
    """
    import asyncio
    from datetime import datetime, timezone

    print("\n📱 QR Code Login")
    print("=" * 40)
    print("1. Open Telegram on your phone")
    print("2. Go to Settings → Devices → Link Desktop Device")
    print("3. Scan the QR code below")
    print("=" * 40 + "\n")

    start_time = asyncio.get_event_loop().time()

    try:
        qr_login_obj = await client.qr_login()

        while True:
            # Check total timeout
            elapsed = asyncio.get_event_loop().time() - start_time
            remaining = timeout - elapsed
            if remaining <= 0:
                print(f"\n⏰ QR code login timed out after {timeout}s")
                return False

            display_qr_code(qr_login_obj.url)

            # Calculate time until QR expires
            now = datetime.now(timezone.utc)
            expires_at = qr_login_obj.expires
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            qr_remaining = (expires_at - now).total_seconds()

            # Wait time is minimum of QR expiry and total remaining timeout
            wait_time = min(max(qr_remaining, 5), remaining)  # At least 5 seconds

            print(f"QR expires in ~{int(qr_remaining)}s (total timeout: {int(remaining)}s remaining)")
            print("Waiting for you to scan...")
            print("(Press Ctrl+C to cancel and use phone login instead)\n")

            try:
                # wait() returns the logged-in user on success
                user = await asyncio.wait_for(qr_login_obj.wait(), timeout=wait_time)
                print(f"\n✅ Successfully logged in via QR code as {user.first_name}!")
                return True
            except asyncio.TimeoutError:
                # Check if total timeout exceeded
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    print(f"\n⏰ QR code login timed out after {timeout}s")
                    return False

                # QR expired, refresh it
                print("\n🔄 QR code expired, generating new one...\n")
                try:
                    await qr_login_obj.recreate()
                except Exception as e:
                    logger.warning(f"Failed to recreate QR: {e}")
                    # Try creating a new QR login object
                    qr_login_obj = await client.qr_login()

    except SessionPasswordNeededError:
        print("\n🔐 Two-factor authentication is enabled.")
        password = getpass("Enter your 2FA password: ")
        await client.sign_in(password=password)
        print("✅ Successfully logged in with 2FA!")
        return True

    except KeyboardInterrupt:
        print("\n\n⚠️ QR login cancelled by user")
        return False

    except Exception as e:
        # Log the full exception for debugging
        logger.exception("QR code login failed")
        print("\n❌ QR code login failed:")
        print(f"   Exception type: {type(e).__name__}")
        print(f"   Message: {e}")
        if hasattr(e, '__cause__') and e.__cause__:
            print(f"   Cause: {e.__cause__}")
        return False


async def phone_login(client: TelegramClient) -> bool:
    """Attempt phone number login.

    Args:
        client: TelegramClient instance

    Returns:
        True if successful, False otherwise
    """
    from telethon.errors import FloodWaitError, PhoneNumberInvalidError, PhoneCodeInvalidError

    print("\n📞 Phone Login")
    print("=" * 40)

    try:
        phone = input("Enter your phone number (with country code, e.g., +1234567890): ").strip()

        if not phone:
            print("❌ Phone number cannot be empty")
            return False

        try:
            await client.send_code_request(phone)
        except PhoneNumberInvalidError:
            print("❌ Invalid phone number format. Use international format (e.g., +1234567890)")
            return False
        except FloodWaitError as e:
            print(f"❌ Too many attempts. Please wait {e.seconds} seconds before trying again.")
            return False

        code = input("Enter the code you received: ").strip()

        if not code:
            print("❌ Code cannot be empty")
            return False

        try:
            await client.sign_in(phone, code)
            print("\n✅ Successfully logged in!")
            return True

        except PhoneCodeInvalidError:
            print("❌ Invalid code. Please try again.")
            return False
        except SessionPasswordNeededError:
            print("\n🔐 Two-factor authentication is enabled.")
            password = getpass("Enter your 2FA password: ")
            try:
                await client.sign_in(password=password)
                print("✅ Successfully logged in with 2FA!")
                return True
            except Exception as e:
                print(f"❌ 2FA authentication failed: {e}")
                return False

    except KeyboardInterrupt:
        print("\n⚠️ Login cancelled by user")
        return False
    except Exception as e:
        print(f"\n❌ Phone login failed: {e}")
        return False


async def ensure_authorized(client: TelegramClient, use_phone: bool = False) -> None:
    """Ensure the client is connected and authorized.

    Uses QR code login by default, with phone login as fallback.

    Args:
        client: TelegramClient instance
        use_phone: If True, use phone login instead of QR code
    """
    await client.connect()

    if await client.is_user_authorized():
        return

    # Need to login
    print("Not authorized. Starting login flow...")

    if use_phone:
        # Direct phone login
        success = await phone_login(client)
    else:
        # Try QR code first, fallback to phone
        success = await qr_login(client)

        if not success:
            print("\nFalling back to phone login...")
            success = await phone_login(client)

    if not success:
        raise RuntimeError("Authentication failed")


async def auth_test(use_phone: bool = False) -> int:
    """Test authentication and print user info.

    Args:
        use_phone: If True, use phone login instead of QR code

    Returns:
        Exit code (0 for success)
    """
    client = create_client()

    try:
        await ensure_authorized(client, use_phone=use_phone)

        # Get user info
        me = await client.get_me()

        print("\n" + "=" * 40)
        print("AUTHORIZED")
        print("=" * 40)
        print(f"User ID: {me.id}")
        print(f"Username: @{me.username}" if me.username else "Username: (none)")
        print(f"Name: {me.first_name} {me.last_name or ''}".strip())
        print(f"Session: {get_session_path()}")
        print("=" * 40)

        return 0
    except RuntimeError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await client.disconnect()


async def list_dialogs(search: str | None = None, limit: int = 20) -> int:
    """List dialogs (chats/channels) with optional search filter.

    Args:
        search: Optional search text to filter dialogs
        limit: Maximum number of dialogs to show

    Returns:
        Exit code (0 for success)
    """
    client = create_client()

    try:
        await ensure_authorized(client)

        search_msg = f' matching "{search}"' if search else ''
        print(f"Listing dialogs{search_msg}...\n")

        count = 0
        async for dialog in client.iter_dialogs():
            # Filter by search if provided
            title = dialog.title or ""
            username = getattr(dialog.entity, "username", None) or ""

            if search:
                search_lower = search.lower()
                if search_lower not in title.lower() and search_lower not in username.lower():
                    continue

            peer_id = get_peer_id(dialog.entity)

            # Format output
            username_str = f"@{username}" if username else "(no username)"
            print(f"  {title}")
            print(f"    peer_id: {peer_id}")
            print(f"    username: {username_str}")
            print()

            count += 1
            if count >= limit:
                print(f"(showing first {limit} results)")
                break

        if count == 0:
            print("No dialogs found.")

        return 0
    finally:
        await client.disconnect()


async def fetch_test(peer_input: str, limit: int = 5) -> int:
    """Fetch a few messages from a peer for testing.

    Args:
        peer_input: Peer identifier (@username, link, or ID)
        limit: Number of messages to fetch

    Returns:
        Exit code (0 for success)
    """
    client = create_client()

    try:
        await ensure_authorized(client)

        # Normalize peer input (handle t.me links, etc.)
        normalized_peer = normalize_peer_input(peer_input)
        print(f"Resolving peer: {peer_input}...")

        # Resolve the peer
        try:
            input_entity = await client.get_input_entity(normalized_peer)
            entity = await client.get_entity(input_entity)
        except ValueError:
            print(f"Error: Could not find entity '{peer_input}'")
            print("Make sure you have joined the group/channel first.")
            return 1

        peer_id = get_peer_id(entity)
        title = get_display_name(entity)
        username = getattr(entity, "username", None)

        print("\nResolved:")
        print(f"  Title: {title}")
        print(f"  Peer ID: {peer_id}")
        if username:
            print(f"  Username: @{username}")

        print(f"\nFetching last {limit} messages...\n")

        count = 0
        async for msg in client.iter_messages(entity, limit=limit):
            msg_id = msg.id
            date = msg.date.strftime("%Y-%m-%d %H:%M:%S") if msg.date else "unknown"
            sender_id = msg.sender_id or "channel"
            text_preview = truncate_text(msg.raw_text, 60)

            # Get sender name if available
            sender_name = ""
            if msg.sender:
                sender_name = f" ({get_display_name(msg.sender)})"

            print(f"  [{msg_id}] {date} | sender: {sender_id}{sender_name}")
            if text_preview:
                print(f"         {text_preview}")
            elif msg.media:
                print("         [Media]")
            print()

            count += 1

        print(f"Fetched {count} messages.")
        return 0
    finally:
        await client.disconnect()

