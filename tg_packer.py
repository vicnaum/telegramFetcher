#!/usr/bin/env python3
"""
Telegram Chats Packer - Compress Telegram chat logs for efficient AI processing.

Features:
- Removes message IDs
- Concatenates consecutive messages from the same user
- Groups multiple photos/videos/stickers into counts
- Simplifies timestamps to just date headers
- Interactive user name shortening
- Link compression (full/short/remove modes)
- Token counting for AI usage estimation
"""

import re
import sys
from pathlib import Path
from collections import Counter
from typing import Optional
from urllib.parse import urlparse
from enum import Enum

import typer
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

app = typer.Typer(help="Telegram Chats Packer - Compress chat logs for AI processing")
console = Console()

# Link handling modes
class LinkMode(str, Enum):
    full = "full"      # Keep links as-is
    short = "short"    # Shorten to [domain.com link]
    remove = "remove"  # Just [link]

# Pattern to match log lines: [id] YYYY-MM-DD HH:MM:SS | User Name | message
LOG_PATTERN = re.compile(
    r'^\s*\[(\d+)\]\s+(\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}\s+\|\s+(.+?)\s+\|\s+(.*)$'
)

# URL pattern - matches http/https URLs
URL_PATTERN = re.compile(r'https?://[^\s]+')

# Bespoke URL patterns for special handling
TWITTER_STATUS_PATTERN = re.compile(r'https?://(?:x\.com|twitter\.com)/([^/]+)/status/\d+')
GITHUB_REPO_PATTERN = re.compile(r'https?://github\.com/([^/]+)/([^/]+)(?:/(.*))?')

# Sensitive data patterns for redaction (order matters - more specific patterns first)
SENSITIVE_PATTERNS = [
    # Mnemonic seed phrases (12-24 words in quotes)
    (re.compile(r'["\'](?:[a-z]+\s+){11,23}[a-z]+["\']'), '[REDACTED_MNEMONIC]'),
    
    # JWT tokens (Supabase, Auth tokens, etc.)
    (re.compile(r'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+'), '[REDACTED_JWT]'),
    
    # Telegram bot tokens
    (re.compile(r'\b\d{9,10}:[A-Za-z0-9_-]{35}\b'), '[REDACTED_TG_TOKEN]'),
    
    # Ethereum private keys (64 hex chars after 0x)
    (re.compile(r'0x[a-fA-F0-9]{64}\b'), '[REDACTED_PRIVATE_KEY]'),
    
    # Long hex strings (likely secrets, but not addresses which are 40 chars)
    (re.compile(r'\b[a-fA-F0-9]{48,}\b'), '[REDACTED_HEX]'),
    
    # Google API keys
    (re.compile(r'AIza[A-Za-z0-9_-]{35}'), '[REDACTED_GOOGLE_KEY]'),
    
    # OpenAI/Anthropic keys
    (re.compile(r'sk-[A-Za-z0-9]{32,}'), '[REDACTED_API_KEY]'),
    
    # RevenueCat keys
    (re.compile(r'appl_[A-Za-z0-9]{16,}'), '[REDACTED_RC_KEY]'),
    
    # Keys with prefixes like key_, secret_, etc.
    (re.compile(r'\b(key|secret|token)_[A-Za-z0-9_-]{16,}\b', re.IGNORECASE), '[REDACTED_KEY]'),
    
    # UUID-style keys (commonly used as API keys)
    (re.compile(r'\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b', re.IGNORECASE), '[REDACTED_UUID]'),
    
    # Values after = sign that are 16+ alphanumeric chars (env vars, configs)
    (re.compile(r'(?<==)[A-Za-z0-9_-]{16,}\b'), '[REDACTED_VALUE]'),
    
    # Generic long alphanumeric strings (16+ chars, no spaces/dots/dashes inside)
    (re.compile(r'\b[A-Za-z0-9]{16,}\b'), '[REDACTED_KEY]'),
    
    # Supabase project URLs with keys in them
    (re.compile(r'https?://[a-z0-9]+\.supabase\.co[^\s]*'), '[REDACTED_SUPABASE_URL]'),
    
    # ngrok URLs (often contain session tokens)
    (re.compile(r'https?://[a-z0-9-]+\.ngrok[a-z-]*\.(app|io)[^\s]*'), '[REDACTED_NGROK_URL]'),
    
    # Database connection strings
    (re.compile(r'(postgres|mysql|mongodb)://[^\s]+'), '[REDACTED_DB_URL]'),
    
    # Common secret/key/token/password environment variables
    (re.compile(r'(?i)(API_KEY|SECRET|TOKEN|PASSWORD|PRIVATE_KEY|AUTH|SALT|MNEMONIC)\s*[=:]\s*\S+'), '[REDACTED_SECRET]'),
    
    # Password patterns (word "password" followed by something)
    (re.compile(r'(?i)(password|passwd|pwd)\s*[-=:]\s*\S+'), '[REDACTED_PASSWORD]'),
]

# Media patterns
MEDIA_PATTERNS = {
    'photo': re.compile(r'^\[photo\]$'),
    'video': re.compile(r'^\[video\]$'),
    'sticker': re.compile(r'^\[sticker\]$'),
    'voice': re.compile(r'^\[voice\]$'),
    'audio': re.compile(r'^\[audio\]$'),
    'document': re.compile(r'^\[document\]$'),
    'animation': re.compile(r'^\[animation\]$'),
}

# Reply pattern to simplify
REPLY_PATTERN = re.compile(r'^\[reply to #\d+ @.+?\]\s*')


def count_tokens(text: str) -> int:
    """Count approximate tokens using tiktoken if available, else estimate."""
    try:
        import tiktoken
        enc = tiktoken.encoding_for_model("gpt-4")
        return len(enc.encode(text))
    except ImportError:
        # Rough estimation: ~4 chars per token for English/mixed text
        return len(text) // 4


def parse_line(line: str) -> Optional[dict]:
    """Parse a single log line into components."""
    match = LOG_PATTERN.match(line)
    if not match:
        return None
    
    msg_id, date, user, content = match.groups()
    return {
        'id': msg_id,
        'date': date,
        'user': user.strip(),
        'content': content.strip(),
    }


def detect_media_type(content: str) -> Optional[str]:
    """Check if content is a single media item."""
    content = content.strip()
    for media_type, pattern in MEDIA_PATTERNS.items():
        if pattern.match(content):
            return media_type
    return None


def simplify_reply(content: str) -> str:
    """Remove reply metadata entirely."""
    match = REPLY_PATTERN.match(content)
    if match:
        return content[match.end():]
    return content


def redact_sensitive_data(content: str) -> str:
    """Redact sensitive data like API keys, tokens, passwords, etc."""
    for pattern, replacement in SENSITIVE_PATTERNS:
        content = pattern.sub(replacement, content)
    return content


def extract_domain(url: str) -> str:
    """Extract domain from URL, simplifying common ones."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return "link"


def get_bespoke_link_key(url: str) -> Optional[str]:
    """Get a bespoke short key for known URL patterns.
    
    Returns None if no bespoke handling, otherwise returns the key like '@levelsio tweet'.
    """
    # Twitter/X tweets
    match = TWITTER_STATUS_PATTERN.match(url)
    if match:
        username = match.group(1)
        return f"@{username} tweet"
    
    # GitHub repos
    match = GITHUB_REPO_PATTERN.match(url)
    if match:
        org = match.group(1)
        repo = match.group(2)
        # Clean up repo name (remove .git suffix, query params)
        repo = repo.split('?')[0].rstrip('.git')
        rest = match.group(3) or ""
        
        # Determine what type of GitHub link
        if not rest or rest in ('', 'invitations'):
            return f"{org}/{repo} repo"
        elif rest.startswith('pull/') or rest.startswith('pulls'):
            return f"{org}/{repo} PR"
        elif rest.startswith('blob/') or rest.startswith('tree/'):
            return f"{org}/{repo} file"
        elif rest.startswith('issues'):
            return f"{org}/{repo} issue"
        else:
            return f"{org}/{repo} repo"
    
    return None


def is_only_url(content: str) -> Optional[str]:
    """Check if content is just a single URL (possibly with trailing punctuation)."""
    content = content.strip()
    # Remove common trailing punctuation that might be attached
    clean = content.rstrip('.,!?;:')
    if URL_PATTERN.fullmatch(clean):
        return clean
    return None


def get_link_short_key(url: str) -> str:
    """Get the short key for a URL (bespoke if available, otherwise domain)."""
    bespoke = get_bespoke_link_key(url)
    if bespoke:
        return bespoke
    return f"{extract_domain(url)} link"


def process_links(content: str, mode: LinkMode) -> str:
    """Process links in content based on mode."""
    if mode == LinkMode.full:
        return content
    
    # Check if content is just a single URL
    single_url = is_only_url(content)
    if single_url:
        if mode == LinkMode.remove:
            return "[link]"
        else:  # short mode
            return f"[{get_link_short_key(single_url)}]"
    
    # Replace URLs inline for mixed content
    def replace_url(match):
        url = match.group(0)
        if mode == LinkMode.remove:
            return "[link]"
        else:  # short mode
            return f"[{get_link_short_key(url)}]"
    
    return URL_PATTERN.sub(replace_url, content)


def get_user_renames(users: list[str], interactive: bool = True) -> dict[str, str]:
    """Get user name mappings, optionally interactively."""
    if not interactive:
        return {u: u for u in users}
    
    console.print("\n[bold cyan]User names found in the chat:[/bold cyan]")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim")
    table.add_column("Original Name")
    table.add_column("Message Count", justify="right")
    
    # Count messages per user
    user_counts = Counter(users)
    sorted_users = sorted(set(users), key=lambda u: -user_counts[u])
    
    for i, user in enumerate(sorted_users, 1):
        table.add_row(str(i), user, str(user_counts[user]))
    
    console.print(table)
    console.print("\n[dim]Enter short names for each user (press Enter to keep original):[/dim]\n")
    
    renames = {}
    for user in sorted_users:
        short = Prompt.ask(f"  [cyan]{user}[/cyan]", default=user)
        renames[user] = short.strip() or user
    
    return renames


def pack_messages(
    messages: list[dict], 
    user_renames: dict[str, str], 
    simplify_replies: bool = True,
    link_mode: LinkMode = LinkMode.short,
    redact_secrets: bool = False
) -> str:
    """Pack parsed messages into compressed format."""
    if not messages:
        return ""
    
    lines = []
    current_date = None
    current_user = None
    current_contents = []
    pending_media = {}  # media_type -> count
    pending_links = {}  # domain -> count (for short mode) or "link" -> count (for remove mode)
    
    def flush_pending():
        """Flush accumulated messages/media/links for current user."""
        nonlocal current_contents, pending_media, pending_links
        
        result_parts = []
        
        # Add accumulated media counts
        for media_type, count in pending_media.items():
            if count == 1:
                result_parts.append(f"[{media_type}]")
            else:
                result_parts.append(f"[{count} {media_type}s]")
        
        # Add accumulated link counts
        for link_key, count in pending_links.items():
            if link_key:  # short mode with bespoke key (already includes type like "link", "tweet", "repo")
                if count == 1:
                    result_parts.append(f"[{link_key}]")
                else:
                    # Pluralize: "tweet" -> "tweets", "link" -> "links", "repo" -> "repos"
                    result_parts.append(f"[{count} {link_key}s]")
            else:  # remove mode, no domain
                if count == 1:
                    result_parts.append("[link]")
                else:
                    result_parts.append(f"[{count} links]")
        
        # Add text messages
        result_parts.extend(current_contents)
        
        if result_parts and current_user:
            user_display = user_renames.get(current_user, current_user)
            # Join parts, avoiding double periods
            joined = '. '.join(result_parts)
            # Clean up double periods (when original message ends with period)
            joined = joined.replace('.. ', '. ').replace(' .', '.')
            lines.append(f"{user_display}: {joined}")
        
        current_contents = []
        pending_media = {}
        pending_links = {}
    
    def flush_media_and_links_to_content():
        """Flush pending media and links into current_contents (when text arrives)."""
        nonlocal pending_media, pending_links
        
        for mt, count in pending_media.items():
            if count == 1:
                current_contents.append(f"[{mt}]")
            else:
                current_contents.append(f"[{count} {mt}s]")
        pending_media = {}
        
        for link_key, count in pending_links.items():
            if link_key:  # short mode with bespoke key
                if count == 1:
                    current_contents.append(f"[{link_key}]")
                else:
                    current_contents.append(f"[{count} {link_key}s]")
            else:  # remove mode, no domain
                if count == 1:
                    current_contents.append("[link]")
                else:
                    current_contents.append(f"[{count} links]")
        pending_links = {}
    
    for msg in messages:
        date = msg['date']
        user = msg['user']
        content = msg['content']
        
        # Handle date change
        if date != current_date:
            flush_pending()
            current_user = None
            if current_date is not None:
                lines.append("")  # Empty line between days
            lines.append(f"# {date}")
            current_date = date
        
        # Handle user change
        if user != current_user:
            flush_pending()
            current_user = user
        
        # Process content
        if simplify_replies:
            content = simplify_reply(content)
        
        # Redact sensitive data
        if redact_secrets:
            content = redact_sensitive_data(content)
        
        # Check for media
        media_type = detect_media_type(content)
        if media_type:
            pending_media[media_type] = pending_media.get(media_type, 0) + 1
            continue
        
        # Check for standalone link (for grouping)
        if link_mode != LinkMode.full:
            single_url = is_only_url(content)
            if single_url:
                if link_mode == LinkMode.remove:
                    pending_links[""] = pending_links.get("", 0) + 1
                else:  # short mode - use bespoke key or domain
                    link_key = get_link_short_key(single_url)
                    pending_links[link_key] = pending_links.get(link_key, 0) + 1
                continue
        
        # Regular text content
        if content:
            # Flush any pending media/links before adding text
            if pending_media or pending_links:
                flush_media_and_links_to_content()
            
            # Process inline links in content
            if link_mode != LinkMode.full:
                content = process_links(content, link_mode)
            
            current_contents.append(content)
    
    # Final flush
    flush_pending()
    
    return '\n'.join(lines)


def process_file(
    input_path: Path,
    interactive: bool = True,
    simplify_replies: bool = True,
    link_mode: LinkMode = LinkMode.short,
    redact_secrets: bool = False,
) -> tuple[str, dict[str, str], int, int]:
    """Process a chat log file and return packed content."""
    
    content = input_path.read_text(encoding='utf-8')
    lines = content.splitlines()
    
    # Parse all lines
    messages = []
    users = []
    
    for line in lines:
        parsed = parse_line(line)
        if parsed:
            messages.append(parsed)
            users.append(parsed['user'])
    
    if not messages:
        console.print("[red]No valid messages found in the file![/red]")
        raise typer.Exit(1)
    
    # Get user renames
    user_renames = get_user_renames(users, interactive=interactive)
    
    # Pack messages
    packed = pack_messages(
        messages, 
        user_renames, 
        simplify_replies=simplify_replies, 
        link_mode=link_mode,
        redact_secrets=redact_secrets
    )
    
    # Count tokens
    original_tokens = count_tokens(content)
    packed_tokens = count_tokens(packed)
    
    return packed, user_renames, original_tokens, packed_tokens


@app.command()
def pack(
    input_file: Path = typer.Argument(..., help="Input chat log file (.txt)"),
    output_file: Optional[Path] = typer.Option(None, "-o", "--output", help="Output file path (default: input_packed.txt)"),
    no_interactive: bool = typer.Option(False, "--no-interactive", "-n", help="Skip interactive user renaming"),
    keep_replies: bool = typer.Option(False, "--keep-replies", "-r", help="Keep full reply metadata"),
    links: LinkMode = typer.Option(LinkMode.short, "-l", "--links", help="Link handling: full (keep), short (domain only, default), remove"),
    redact: bool = typer.Option(False, "--redact", "-s", help="Redact sensitive data (API keys, tokens, passwords, etc.)"),
):
    """
    Pack a Telegram chat log file into a compressed format.
    
    Removes message IDs, concatenates consecutive messages, groups media,
    and simplifies timestamps to date headers.
    """
    
    if not input_file.exists():
        console.print(f"[red]File not found: {input_file}[/red]")
        raise typer.Exit(1)
    
    console.print(f"\n[bold]📦 Telegram Chats Packer[/bold]")
    console.print(f"Processing: [cyan]{input_file}[/cyan]\n")
    
    # Process the file
    packed, user_renames, original_tokens, packed_tokens = process_file(
        input_file,
        interactive=not no_interactive,
        simplify_replies=not keep_replies,
        link_mode=links,
        redact_secrets=redact,
    )
    
    # Determine output path
    if output_file is None:
        output_file = input_file.with_stem(f"{input_file.stem}_packed")
    
    # Write output
    output_file.write_text(packed, encoding='utf-8')
    
    # Statistics
    original_size = input_file.stat().st_size
    packed_size = len(packed.encode('utf-8'))
    compression_ratio = (1 - packed_size / original_size) * 100 if original_size > 0 else 0
    token_reduction = (1 - packed_tokens / original_tokens) * 100 if original_tokens > 0 else 0
    
    console.print("\n[bold green]✅ Packing complete![/bold green]\n")
    
    # Results table
    table = Table(title="Compression Results", show_header=True, header_style="bold")
    table.add_column("Metric", style="cyan")
    table.add_column("Original", justify="right")
    table.add_column("Packed", justify="right")
    table.add_column("Reduction", justify="right", style="green")
    
    table.add_row(
        "File Size",
        f"{original_size:,} bytes",
        f"{packed_size:,} bytes",
        f"{compression_ratio:.1f}%"
    )
    table.add_row(
        "AI Tokens (est.)",
        f"{original_tokens:,}",
        f"{packed_tokens:,}",
        f"{token_reduction:.1f}%"
    )
    
    console.print(table)
    
    # User renames used
    if any(k != v for k, v in user_renames.items()):
        console.print("\n[bold]User name mappings:[/bold]")
        for original, short in user_renames.items():
            if original != short:
                console.print(f"  {original} → [cyan]{short}[/cyan]")
    
    console.print(f"\n[dim]Output saved to: {output_file}[/dim]")


@app.command()
def analyze(
    input_file: Path = typer.Argument(..., help="Input chat log file (.txt)"),
):
    """
    Analyze a chat log file without packing (shows statistics only).
    """
    
    if not input_file.exists():
        console.print(f"[red]File not found: {input_file}[/red]")
        raise typer.Exit(1)
    
    content = input_file.read_text(encoding='utf-8')
    lines = content.splitlines()
    
    messages = []
    users = []
    dates = set()
    media_counts = Counter()
    
    for line in lines:
        parsed = parse_line(line)
        if parsed:
            messages.append(parsed)
            users.append(parsed['user'])
            dates.add(parsed['date'])
            media_type = detect_media_type(parsed['content'])
            if media_type:
                media_counts[media_type] += 1
    
    console.print(f"\n[bold]📊 Chat Analysis: {input_file.name}[/bold]\n")
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")
    
    table.add_row("Total Messages", f"{len(messages):,}")
    table.add_row("Unique Users", f"{len(set(users)):,}")
    table.add_row("Date Range", f"{min(dates)} to {max(dates)}" if dates else "N/A")
    table.add_row("Days", f"{len(dates):,}")
    table.add_row("File Size", f"{input_file.stat().st_size:,} bytes")
    table.add_row("Est. AI Tokens", f"{count_tokens(content):,}")
    
    console.print(table)
    
    if media_counts:
        console.print("\n[bold]Media breakdown:[/bold]")
        for media_type, count in media_counts.most_common():
            console.print(f"  [{media_type}]: {count:,}")
    
    # User stats
    user_counts = Counter(users)
    console.print("\n[bold]Top users:[/bold]")
    for user, count in user_counts.most_common(10):
        console.print(f"  {user}: {count:,} messages")


if __name__ == "__main__":
    app()
