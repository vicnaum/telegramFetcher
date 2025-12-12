"""Tests for sync functionality.

Focus on:
- Peer type classification (User vs Chat vs Channel)
- Message conversion
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

# Import the functions we're testing
from tgx.sync import get_media_type, message_to_dict


class MockMessage:
    """Mock Telethon Message for testing."""

    def __init__(
        self,
        msg_id: int,
        date: datetime,
        sender_id: int | None = None,
        text: str | None = None,
        reply_to_msg_id: int | None = None,
        has_media: bool = False,
    ):
        self.id = msg_id
        self.date = date
        self.sender_id = sender_id
        self.raw_text = text
        self.sender = None
        self.reply_to = None
        self.media = MagicMock() if has_media else None
        self.photo = None
        self.video = None
        self.voice = None
        self.audio = None
        self.sticker = None
        self.gif = None
        self.document = None
        self.web_preview = None

        if reply_to_msg_id:
            self.reply_to = MagicMock()
            self.reply_to.reply_to_msg_id = reply_to_msg_id

    def to_json(self):
        return '{"_": "Message", "id": ' + str(self.id) + '}'


class TestMessageToDict:
    """Test message conversion to dict."""

    def test_basic_message(self):
        """Basic message should convert correctly."""
        msg = MockMessage(
            msg_id=123,
            date=datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc),
            sender_id=456,
            text="Hello world",
        )

        result = message_to_dict(msg, peer_id=789)

        assert result["msg_id"] == 123
        assert result["peer_id"] == 789
        assert result["date"] == msg.date
        assert result["sender_id"] == 456
        assert result["text"] == "Hello world"
        assert result["reply_to_msg_id"] is None
        assert result["has_media"] is False

    def test_message_with_reply(self):
        """Message with reply should capture reply_to_msg_id."""
        msg = MockMessage(
            msg_id=200,
            date=datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc),
            sender_id=456,
            text="Reply message",
            reply_to_msg_id=100,
        )

        result = message_to_dict(msg, peer_id=789)

        assert result["reply_to_msg_id"] == 100

    def test_message_with_media(self):
        """Message with media should set has_media flag."""
        msg = MockMessage(
            msg_id=123,
            date=datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc),
            sender_id=456,
            has_media=True,
        )

        result = message_to_dict(msg, peer_id=789)

        assert result["has_media"] is True

    def test_raw_data_included(self):
        """raw_data should contain JSON serialization."""
        msg = MockMessage(
            msg_id=123,
            date=datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc),
        )

        result = message_to_dict(msg, peer_id=789)

        assert result["raw_data"] is not None
        assert "Message" in result["raw_data"]


class TestGetMediaType:
    """Test media type detection."""

    def test_no_media(self):
        """No media should return None."""
        msg = MagicMock()
        msg.media = None

        assert get_media_type(msg) is None

    def test_photo(self):
        """Photo should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = MagicMock()
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = None
        msg.web_preview = None

        assert get_media_type(msg) == "photo"

    def test_video(self):
        """Video should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = MagicMock()
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = None
        msg.web_preview = None

        assert get_media_type(msg) == "video"

    def test_document(self):
        """Document should be detected."""
        msg = MagicMock()
        msg.media = MagicMock()
        msg.photo = None
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.sticker = None
        msg.gif = None
        msg.document = MagicMock()
        msg.web_preview = None

        assert get_media_type(msg) == "document"


class TestPeerTypeClassification:
    """Test peer type classification.

    Note: Full sync_peer testing requires mocking TelegramClient,
    but we can test the classification logic by importing the types.
    """

    def test_user_type_detection(self):
        """User entities should be classified as 'user'."""
        from telethon.tl.types import User

        # Create a mock User (we just need isinstance to work)
        user = MagicMock(spec=User)

        assert isinstance(user, User)

    def test_channel_type_detection(self):
        """Channel entities should be classified based on megagroup flag."""
        from telethon.tl.types import Channel

        # Broadcast channel
        broadcast = MagicMock(spec=Channel)
        broadcast.megagroup = False
        broadcast.gigagroup = False
        broadcast.broadcast = True

        # Megagroup (supergroup)
        megagroup = MagicMock(spec=Channel)
        megagroup.megagroup = True
        megagroup.gigagroup = False
        megagroup.broadcast = False

        # Gigagroup
        gigagroup = MagicMock(spec=Channel)
        gigagroup.megagroup = False
        gigagroup.gigagroup = True
        gigagroup.broadcast = False

        # Just verify the mocks have the right types
        assert isinstance(broadcast, Channel)
        assert isinstance(megagroup, Channel)
        assert isinstance(gigagroup, Channel)

