#!/usr/bin/env python3
"""
Test suite for duplicate meeting prevention.

The issue: When users click "join now" multiple times quickly, the Firebase
function creates multiple meeting documents with the same join_url. This
results in multiple bots trying to join the same meeting.

The fix: The controller should detect duplicate meetings (same org + URL) and
consolidate them before launching a bot.
"""

import hashlib
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class MockMeeting:
    """Mock Firestore meeting document."""

    id: str
    data: Dict

    @property
    def exists(self) -> bool:
        return True

    def to_dict(self) -> Dict:
        return self.data.copy()


class DuplicateMeetingPrevention:
    """
    Logic for preventing duplicate meetings with the same URL.

    This class implements the deduplication logic that should be added to
    the controller to prevent multiple bots from joining when the Firebase
    function creates duplicate meeting documents.
    """

    @staticmethod
    def normalize_meeting_url(url: str) -> str:
        """
        Normalize meeting URL for consistent comparison.
        Removes URL encoding differences, trailing slashes, etc.
        """
        if not url:
            return ""

        import urllib.parse

        # Handle URL encoding (%3a → :, etc.)
        decoded = urllib.parse.unquote(url)

        # Lowercase for consistent matching
        normalized = decoded.lower().strip()

        # Remove trailing slashes
        normalized = normalized.rstrip("/")

        # Remove common URL fragments
        if "#" in normalized:
            normalized = normalized.split("#")[0]

        return normalized

    @staticmethod
    def extract_meeting_key(url: str) -> Optional[str]:
        """
        Extract a unique meeting key from the URL.

        For Teams: The meeting ID from the URL (e.g., 4393898968980)
        For Meet: The meeting code (e.g., abc-defg-hij)
        For Zoom: The meeting ID
        """
        if not url:
            return None

        import re

        url_lower = url.lower()

        # Teams Meet URLs: /meet/4393898968980?p=...
        if "teams.microsoft.com/meet/" in url_lower:
            match = re.search(r"/meet/(\d+)", url)
            if match:
                return f"teams:{match.group(1)}"

        # Teams meetup-join URLs: /meetup-join/19%3ameeting_xxx/...
        if "teams.microsoft.com/l/meetup-join" in url_lower:
            # Extract the meeting GUID from encoded URL
            match = re.search(r"meeting_([A-Za-z0-9]+)", url)
            if match:
                return f"teams:meeting_{match.group(1)}"

        # Google Meet: /abc-defg-hij
        if "meet.google.com/" in url_lower:
            match = re.search(r"meet\.google\.com/([a-z]+-[a-z]+-[a-z]+)", url_lower)
            if match:
                return f"meet:{match.group(1)}"

        # Zoom: /j/12345678901
        if "zoom.us/j/" in url_lower or "zoom.com/j/" in url_lower:
            match = re.search(r"/j/(\d+)", url)
            if match:
                return f"zoom:{match.group(1)}"

        return None

    @staticmethod
    def find_duplicate_meetings(
        meetings: List[MockMeeting],
        org_id: str,
    ) -> Dict[str, List[MockMeeting]]:
        """
        Group meetings by their normalized URL to find duplicates.

        Returns:
            Dict mapping meeting_key to list of meetings with that key
        """
        by_key: Dict[str, List[MockMeeting]] = {}

        for meeting in meetings:
            data = meeting.to_dict()

            # Skip if different org
            if data.get("organization_id") != org_id:
                continue

            url = data.get("join_url") or data.get("meeting_url") or ""
            key = DuplicateMeetingPrevention.extract_meeting_key(url)

            if key:
                if key not in by_key:
                    by_key[key] = []
                by_key[key].append(meeting)

        return by_key

    @staticmethod
    def choose_canonical_meeting(
        duplicates: List[MockMeeting],
    ) -> Tuple[MockMeeting, List[MockMeeting]]:
        """
        Choose which meeting should be the canonical one.

        Strategy:
        1. Prefer meetings that already have a bot_instance_id
        2. If multiple, prefer the oldest one
        3. If same age, prefer by document ID (deterministic)

        Returns:
            Tuple of (canonical meeting, list of duplicates to merge)
        """
        if len(duplicates) <= 1:
            return duplicates[0], []

        # Sort by priority
        def priority_key(m: MockMeeting) -> Tuple:
            data = m.to_dict()
            has_bot = 1 if data.get("bot_instance_id") else 0
            created_at = data.get("created_at", datetime.max)
            return (-has_bot, created_at, m.id)

        sorted_meetings = sorted(duplicates, key=priority_key)

        return sorted_meetings[0], sorted_meetings[1:]


def test_normalize_url():
    """Test URL normalization."""
    norm = DuplicateMeetingPrevention.normalize_meeting_url

    # URL decoding
    assert (
        norm("https://teams.microsoft.com/meet/123%3fabc")
        == "https://teams.microsoft.com/meet/123?abc"
    )

    # Case insensitivity
    assert (
        norm("https://TEAMS.microsoft.com/MEET/123")
        == "https://teams.microsoft.com/meet/123"
    )

    # Trailing slash removal
    assert (
        norm("https://meet.google.com/abc-def-ghi/")
        == "https://meet.google.com/abc-def-ghi"
    )

    # Fragment removal
    assert norm("https://zoom.us/j/123#stuff") == "https://zoom.us/j/123"

    print("✅ URL normalization works correctly")


def test_extract_meeting_key():
    """Test meeting key extraction from various URL formats."""
    extract = DuplicateMeetingPrevention.extract_meeting_key

    # Teams /meet/ format
    assert (
        extract("https://teams.microsoft.com/meet/4393898968980?p=5RBY7tVvhbYL0u8L6J")
        == "teams:4393898968980"
    )

    # Teams meetup-join format
    assert (
        extract(
            "https://teams.microsoft.com/l/meetup-join/19%3ameeting_MzljY123/0?context=..."
        )
        == "teams:meeting_MzljY123"
    )

    # Google Meet
    assert extract("https://meet.google.com/abc-defg-hij") == "meet:abc-defg-hij"

    # Zoom
    assert extract("https://zoom.us/j/12345678901?pwd=xxx") == "zoom:12345678901"

    # Unknown format returns None
    assert extract("https://example.com/meeting") is None

    print("✅ Meeting key extraction works correctly")


def test_find_duplicate_meetings():
    """Test finding duplicate meetings by URL."""
    meetings = [
        MockMeeting(
            "meeting1",
            {
                "organization_id": "org1",
                "join_url": "https://teams.microsoft.com/meet/123?p=abc",
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
            },
        ),
        MockMeeting(
            "meeting2",
            {
                "organization_id": "org1",
                "join_url": "https://teams.microsoft.com/meet/123?p=xyz",  # Same meeting ID!
                "created_at": datetime(2025, 1, 1, 10, 0, 1),
            },
        ),
        MockMeeting(
            "meeting3",
            {
                "organization_id": "org1",
                "join_url": "https://teams.microsoft.com/meet/456?p=abc",  # Different meeting
                "created_at": datetime(2025, 1, 1, 10, 0, 2),
            },
        ),
    ]

    by_key = DuplicateMeetingPrevention.find_duplicate_meetings(meetings, "org1")

    # Should have 2 groups: one with 2 duplicates, one with 1
    assert len(by_key) == 2, f"Expected 2 groups, got {len(by_key)}"

    teams_123 = by_key.get("teams:123", [])
    assert (
        len(teams_123) == 2
    ), f"Expected 2 duplicates for teams:123, got {len(teams_123)}"

    teams_456 = by_key.get("teams:456", [])
    assert (
        len(teams_456) == 1
    ), f"Expected 1 meeting for teams:456, got {len(teams_456)}"

    print("✅ Duplicate meeting detection works correctly")


def test_choose_canonical_meeting():
    """Test choosing the canonical meeting from duplicates."""
    now = datetime.now(timezone.utc)

    # Test 1: Prefer meeting with bot_instance_id
    meetings = [
        MockMeeting(
            "newer_no_bot",
            {
                "created_at": now,
                "bot_instance_id": None,
            },
        ),
        MockMeeting(
            "older_with_bot",
            {
                "created_at": now - timedelta(minutes=1),
                "bot_instance_id": "bot123",
            },
        ),
    ]

    canonical, duplicates = DuplicateMeetingPrevention.choose_canonical_meeting(
        meetings
    )
    assert (
        canonical.id == "older_with_bot"
    ), f"Expected older_with_bot, got {canonical.id}"
    assert len(duplicates) == 1
    assert duplicates[0].id == "newer_no_bot"
    print("✅ Prefers meeting with bot_instance_id")

    # Test 2: Without bot_instance_id, prefer oldest
    meetings = [
        MockMeeting(
            "newer",
            {
                "created_at": now,
            },
        ),
        MockMeeting(
            "older",
            {
                "created_at": now - timedelta(minutes=1),
            },
        ),
    ]

    canonical, duplicates = DuplicateMeetingPrevention.choose_canonical_meeting(
        meetings
    )
    assert canonical.id == "older", f"Expected older, got {canonical.id}"
    print("✅ Prefers oldest meeting when no bot_instance_id")

    # Test 3: Single meeting returns itself with no duplicates
    meetings = [MockMeeting("only_one", {"created_at": now})]
    canonical, duplicates = DuplicateMeetingPrevention.choose_canonical_meeting(
        meetings
    )
    assert canonical.id == "only_one"
    assert len(duplicates) == 0
    print("✅ Single meeting handled correctly")


def test_end_to_end_deduplication():
    """Test the full deduplication flow."""
    now = datetime.now(timezone.utc)

    # Simulate the Firebase function creating 3 meetings within milliseconds
    # (as seen in production data)
    meetings = [
        MockMeeting(
            "H8iOn0oau28jqyoDW3MX",
            {
                "organization_id": "advisewell",
                "join_url": "https://teams.microsoft.com/meet/4393898968980?p=5RBY7tVvhbYL0u8L6J",
                "created_at": datetime(2025, 12, 18, 9, 23, 14, 940000),
                "synced_by_user_id": "Mw6Awnh1WtN7631PrSYyGXlpozz1",
                "bot_instance_id": "IeVMoBF9RfGHFfn0YFQs",
            },
        ),
        MockMeeting(
            "rVvvmcMwh87IayoHdTG6",
            {
                "organization_id": "advisewell",
                "join_url": "https://teams.microsoft.com/meet/4393898968980?p=5RBY7tVvhbYL0u8L6J",
                "created_at": datetime(2025, 12, 18, 9, 23, 14, 962000),  # 22ms later
                "synced_by_user_id": "Mw6Awnh1WtN7631PrSYyGXlpozz1",
                "bot_instance_id": "9ThuvR5wekbAqIHSo5OU",
            },
        ),
    ]

    # Find duplicates
    by_key = DuplicateMeetingPrevention.find_duplicate_meetings(meetings, "advisewell")

    assert len(by_key) == 1, "Should have 1 group of duplicates"

    duplicates = by_key["teams:4393898968980"]
    assert len(duplicates) == 2, "Should have 2 duplicates"

    # Choose canonical
    canonical, to_merge = DuplicateMeetingPrevention.choose_canonical_meeting(
        duplicates
    )

    # Both have bot_instance_id, so should choose the older one
    assert (
        canonical.id == "H8iOn0oau28jqyoDW3MX"
    ), f"Expected H8iOn... (older), got {canonical.id}"
    assert len(to_merge) == 1
    assert to_merge[0].id == "rVvvmcMwh87IayoHdTG6"

    print("✅ End-to-end deduplication works correctly")
    print(f"   Canonical meeting: {canonical.id}")
    print(f"   Meetings to merge: {[m.id for m in to_merge]}")


def run_all_tests():
    """Run all tests."""
    print("=" * 70)
    print("DUPLICATE MEETING PREVENTION TEST SUITE")
    print("=" * 70)
    print()

    tests = [
        ("URL Normalization", test_normalize_url),
        ("Meeting Key Extraction", test_extract_meeting_key),
        ("Find Duplicate Meetings", test_find_duplicate_meetings),
        ("Choose Canonical Meeting", test_choose_canonical_meeting),
        ("End-to-End Deduplication", test_end_to_end_deduplication),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        print(f"\n--- Test: {name} ---")
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {e}")
            import traceback

            traceback.print_exc()
            failed += 1

    print()
    print("=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 70)

    return failed == 0


if __name__ == "__main__":
    import sys

    success = run_all_tests()
    sys.exit(0 if success else 1)
