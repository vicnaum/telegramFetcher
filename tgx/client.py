"""Telethon client wrapper with auth flow."""

import os
import sys
from getpass import getpass
from io import StringIO
from pathlib import Path

import qrcode
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, ChannelPrivateError

from tgx.utils import get_display_name, get_peer_id, truncate_text

# Load .env file if present
load_dotenv()


def get_session_path() -> str:
    """Get session file path from env or default."""
    return os.environ.get("TGX_SESSION", "./tgx.session")


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
    
    Note: QR login requires receiving updates, so we temporarily enable them.
    
    Args:
        client: TelegramClient instance
        timeout: Total timeout in seconds
    
    Returns:
        True if successful, False otherwise
    """
    import asyncio
    
    print("\n📱 QR Code Login")
    print("=" * 40)
    print("1. Open Telegram on your phone")
    print("2. Go to Settings → Devices → Link Desktop Device")
    print("3. Scan the QR code below")
    print("=" * 40 + "\n")
    
    try:
        qr_login_obj = await client.qr_login()
        
        display_qr_code(qr_login_obj.url)
        
        print(f"QR expires at: {qr_login_obj.expires}")
        print("Waiting for you to scan...")
        print("(Press Ctrl+C to cancel and use phone login instead)\n")
        
        try:
            # wait() returns the logged-in user on success
            user = await asyncio.wait_for(qr_login_obj.wait(timeout=timeout), timeout=timeout)
            print(f"\n✅ Successfully logged in via QR code as {user.first_name}!")
            return True
        except asyncio.TimeoutError:
            print(f"\n⏰ QR code login timed out after {timeout}s")
            return False
        
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
        print(f"\n❌ QR code login failed:")
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
    print("\n📞 Phone Login")
    print("=" * 40)
    
    phone = input("Enter your phone number (with country code, e.g., +1234567890): ").strip()
    
    await client.send_code_request(phone)
    
    code = input("Enter the code you received: ").strip()
    
    try:
        await client.sign_in(phone, code)
        print("\n✅ Successfully logged in!")
        return True
        
    except SessionPasswordNeededError:
        print("\n🔐 Two-factor authentication is enabled.")
        password = getpass("Enter your 2FA password: ")
        await client.sign_in(password=password)
        print("✅ Successfully logged in with 2FA!")
        return True
        
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
        
        print(f"Listing dialogs{f' matching \"{search}\"' if search else ''}...\n")
        
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
        
        print(f"Resolving peer: {peer_input}...")
        
        # Resolve the peer
        try:
            input_entity = await client.get_input_entity(peer_input)
            entity = await client.get_entity(input_entity)
        except ValueError as e:
            print(f"Error: Could not find entity '{peer_input}'")
            print("Make sure you have joined the group/channel first.")
            return 1
        
        peer_id = get_peer_id(entity)
        title = get_display_name(entity)
        username = getattr(entity, "username", None)
        
        print(f"\nResolved:")
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
                print(f"         [Media]")
            print()
            
            count += 1
        
        print(f"Fetched {count} messages.")
        return 0
    finally:
        await client.disconnect()

