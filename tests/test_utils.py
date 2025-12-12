"""Tests for utility functions.

Focus on:
- normalize_peer_input URL parsing edge cases
"""

import pytest

from tgx.utils import normalize_peer_input


class TestNormalizePeerInput:
    """Test peer input normalization."""

    # Basic username handling
    def test_username_with_at(self):
        """Username with @ should be unchanged."""
        assert normalize_peer_input("@username") == "@username"

    def test_username_without_at(self):
        """Username without @ should get @ prepended."""
        assert normalize_peer_input("username") == "@username"

    def test_username_with_whitespace(self):
        """Whitespace should be stripped."""
        assert normalize_peer_input("  @username  ") == "@username"
        assert normalize_peer_input("  username  ") == "@username"

    # Numeric peer IDs
    def test_positive_peer_id(self):
        """Positive peer ID should be parsed as int."""
        assert normalize_peer_input("1234567890") == 1234567890

    def test_negative_peer_id(self):
        """Negative peer ID (channel/group) should be parsed as int."""
        assert normalize_peer_input("-1001234567890") == -1001234567890

    # Public t.me links
    def test_tme_username_https(self):
        """https://t.me/username should become @username."""
        assert normalize_peer_input("https://t.me/durov") == "@durov"

    def test_tme_username_http(self):
        """http://t.me/username should become @username."""
        assert normalize_peer_input("http://t.me/durov") == "@durov"

    def test_tme_username_no_protocol(self):
        """t.me/username without protocol should become @username."""
        assert normalize_peer_input("t.me/durov") == "@durov"

    def test_tme_message_link(self):
        """t.me/username/123 (message link) should become @username."""
        assert normalize_peer_input("https://t.me/durov/123") == "@durov"
        assert normalize_peer_input("t.me/telegram/999") == "@telegram"

    def test_telegram_me_link(self):
        """telegram.me links should work the same as t.me."""
        assert normalize_peer_input("https://telegram.me/durov") == "@durov"
        assert normalize_peer_input("telegram.me/telegram") == "@telegram"

    # Private channel links (t.me/c/...)
    def test_tme_private_channel_to_peer_id(self):
        """t.me/c/123456789/123 should be converted to peer ID."""
        result = normalize_peer_input("https://t.me/c/1234567890/456")
        # Should convert to peer_id format: -100 prefix for channels
        assert result == -1001234567890

    def test_tme_private_channel_no_protocol(self):
        """t.me/c/... without protocol should be converted to peer ID."""
        result = normalize_peer_input("t.me/c/1234567890/456")
        # Should convert to peer_id
        assert result == -1001234567890

    def test_tme_private_channel_no_message_id(self):
        """t.me/c/123456789 without message ID should still work."""
        result = normalize_peer_input("t.me/c/1234567890")
        assert result == -1001234567890

    # Invite links
    def test_tme_invite_plus(self):
        """t.me/+xxx invite links should be passed through."""
        result = normalize_peer_input("https://t.me/+abcdef123")
        assert result == "https://t.me/+abcdef123"

    def test_tme_invite_joinchat(self):
        """t.me/joinchat/xxx links should be passed through."""
        result = normalize_peer_input("https://t.me/joinchat/abcdef123")
        assert result == "https://t.me/joinchat/abcdef123"

    def test_tme_invite_no_protocol(self):
        """Invite links without protocol should be passed through."""
        result = normalize_peer_input("t.me/+abcdef123")
        assert "+abcdef" in result

    # Edge cases
    def test_short_username(self):
        """Very short usernames might not match the pattern."""
        # Telegram usernames must be 5+ chars, but we're lenient
        result = normalize_peer_input("t.me/abc")
        # Too short to match username pattern, should pass through
        assert result == "t.me/abc"

    def test_underscore_in_username(self):
        """Usernames with underscores should work."""
        assert normalize_peer_input("t.me/my_channel_name") == "@my_channel_name"

    def test_numbers_in_username(self):
        """Usernames with numbers should work."""
        assert normalize_peer_input("t.me/channel123") == "@channel123"

    def test_empty_path(self):
        """Just t.me with no path should pass through."""
        result = normalize_peer_input("https://t.me/")
        # Should pass through as-is
        assert "t.me" in result

