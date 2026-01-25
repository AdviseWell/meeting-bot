#!/usr/bin/env python3
"""
Comprehensive test script to diagnose why AU Standup meeting bot is not joining.

This script tests the meeting data against ALL controller filtering checks:
1. Start time parsing and window matching
2. Meeting status validation
3. Bot instance existence check
4. AI assistant / auto_join settings
5. Teams meeting URL check
6. Session creation/deduplication logic

Run with:
    python controller/test_au_standup_meeting.py --project=<PROJECT_ID> --meeting-id=<MEETING_ID>

Or to find AU Standup meetings:
    python controller/test_au_standup_meeting.py --project=<PROJECT_ID> --find-au-standup
"""

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

# Mock Firestore for unit tests
try:
    from google.cloud import firestore

    HAS_FIRESTORE = True
except ImportError:
    HAS_FIRESTORE = False


def normalize_meeting_url(url: str) -> str:
    """Normalize meeting URL for deduplication (same as controller)."""
    if not url:
        return ""

    parsed = urlparse(url)

    # For Teams meetings, extract the meeting ID from the path
    if "teams.microsoft.com" in parsed.netloc:
        # Extract just the meeting path, ignoring query params
        path = parsed.path
        # Normalize the path
        path = re.sub(r"/+", "/", path)  # Remove duplicate slashes
        path = path.rstrip("/")  # Remove trailing slash
        return f"teams://{path}"

    # For other URLs, use the full URL minus query params
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def meeting_session_id(org_id: str, meeting_url: str) -> str:
    """Generate session ID (same as controller)."""
    normalized = normalize_meeting_url(meeting_url)
    key = f"{org_id}:{normalized}"
    return hashlib.sha256(key.encode()).hexdigest()


def parse_start_time(start_value):
    """Parse start time from various formats (same as controller)."""
    if start_value is None:
        return None

    # Already a datetime
    if isinstance(start_value, datetime):
        if start_value.tzinfo is None:
            return start_value.replace(tzinfo=timezone.utc)
        return start_value

    # Firestore Timestamp
    if hasattr(start_value, "timestamp"):
        return datetime.fromtimestamp(start_value.timestamp(), tz=timezone.utc)

    # ISO string
    if isinstance(start_value, str):
        try:
            # Handle various ISO formats
            dt = datetime.fromisoformat(start_value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None

    return None


class MeetingDiagnostics:
    """Diagnose why a meeting is not being picked up by the controller."""

    # Controller configuration (defaults)
    MEETING_STATUS_FIELD = "status"
    MEETING_STATUS_VALUES = ["confirmed", "scheduled"]
    MEETING_BOT_INSTANCE_FIELD = "bot_instance_id"

    def __init__(self, project_id: str, firestore_db: str = "(default)"):
        self.project_id = project_id
        if HAS_FIRESTORE:
            self.db = firestore.Client(project=project_id, database=firestore_db)
        else:
            self.db = None
        self.issues = []
        self.warnings = []
        self.passed = []

    def diagnose_meeting(
        self,
        meeting_data: dict,
        meeting_id: str = "test",
        org_id: str = "test_org",
        user_data: dict = None,
    ) -> dict:
        """
        Run all controller checks against a meeting document.

        Returns a dict with:
            - issues: List of blocking issues
            - warnings: List of non-blocking warnings
            - passed: List of passed checks
            - would_join: Boolean - would the bot join this meeting?
        """
        self.issues = []
        self.warnings = []
        self.passed = []

        print("=" * 80)
        print(f"DIAGNOSING MEETING: {meeting_id}")
        print("=" * 80)
        print()

        # Check 1: Start time parsing
        self._check_start_time(meeting_data)

        # Check 2: Start time in scan window
        self._check_time_window(meeting_data)

        # Check 3: Meeting status
        self._check_status(meeting_data)

        # Check 4: Bot instance already exists
        self._check_bot_instance(meeting_data)

        # Check 5: AI assistant or auto_join enabled
        self._check_ai_settings(meeting_data, user_data)

        # Check 6: Teams meeting URL
        self._check_teams_url(meeting_data)

        # Check 7: Session deduplication
        self._check_session_dedup(meeting_data, org_id)

        # Check 8: Required fields
        self._check_required_fields(meeting_data)

        # Print results
        print()
        print("=" * 80)
        print("DIAGNOSIS RESULTS")
        print("=" * 80)

        if self.passed:
            print(f"\n✅ PASSED ({len(self.passed)}):")
            for p in self.passed:
                print(f"   • {p}")

        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for w in self.warnings:
                print(f"   • {w}")

        if self.issues:
            print(f"\n❌ BLOCKING ISSUES ({len(self.issues)}):")
            for i in self.issues:
                print(f"   • {i}")
            print("\n🚫 BOT WOULD NOT JOIN - Fix the blocking issues above")
        else:
            print("\n✅ BOT SHOULD JOIN - All checks passed")

        return {
            "issues": self.issues,
            "warnings": self.warnings,
            "passed": self.passed,
            "would_join": len(self.issues) == 0,
        }

    def _check_start_time(self, data: dict):
        """Check if start time can be parsed."""
        start = data.get("start")

        if start is None:
            self.issues.append("Missing 'start' field")
            return

        parsed = parse_start_time(start)
        if parsed is None:
            self.issues.append(
                f"Cannot parse 'start' field: {start!r} (type: {type(start).__name__})"
            )
        else:
            start_type = type(start).__name__
            self.passed.append(
                f"Start time parseable: {parsed.isoformat()} (from {start_type})"
            )

    def _check_time_window(self, data: dict):
        """Check if meeting is within scan window (8 minutes from now ± 30 seconds)."""
        start = parse_start_time(data.get("start"))
        if start is None:
            return  # Already caught in start time check

        now = datetime.now(timezone.utc)
        target_time = now + timedelta(minutes=2)
        window_start = target_time - timedelta(seconds=30)
        window_end = target_time + timedelta(seconds=30)

        if window_start <= start <= window_end:
            self.passed.append(
                f"Start time in current scan window ({window_start.isoformat()} to {window_end.isoformat()})"
            )
        else:
            # Not an issue - meeting may just be scheduled for later
            time_until = (start - now).total_seconds()
            if time_until > 0:
                minutes = int(time_until // 60)
                self.warnings.append(
                    f"Meeting starts in {minutes} minutes - will be scanned ~{minutes - 8} min before start"
                )
            else:
                self.warnings.append(
                    f"Meeting start time is in the past: {start.isoformat()}"
                )

    def _check_status(self, data: dict):
        """Check meeting status."""
        status = data.get(self.MEETING_STATUS_FIELD)

        if status is None:
            self.warnings.append(f"Missing '{self.MEETING_STATUS_FIELD}' field")
            # Check for alternative status indicators
            bot_status = data.get("bot_status")
            session_status = data.get("session_status")
            if bot_status == "queued" or session_status == "queued":
                self.passed.append(
                    f"Alternative status found: bot_status={bot_status}, session_status={session_status}"
                )
            else:
                self.issues.append(
                    f"No valid status: '{self.MEETING_STATUS_FIELD}' is None and no queued bot_status/session_status"
                )
            return

        if status in self.MEETING_STATUS_VALUES:
            self.passed.append(
                f"Status '{status}' is in allowed values {self.MEETING_STATUS_VALUES}"
            )
        else:
            # Check for queued override
            bot_status = data.get("bot_status")
            session_status = data.get("session_status")
            if bot_status == "queued" or session_status == "queued":
                self.passed.append(
                    f"Status '{status}' not in {self.MEETING_STATUS_VALUES} but "
                    f"bot_status={bot_status}/session_status={session_status} allows processing"
                )
            else:
                self.issues.append(
                    f"Status '{status}' not in allowed values {self.MEETING_STATUS_VALUES} "
                    f"(and no queued bot_status/session_status override)"
                )

    def _check_bot_instance(self, data: dict):
        """Check if bot instance already exists."""
        bot_instance = data.get(self.MEETING_BOT_INSTANCE_FIELD)

        if bot_instance:
            self.issues.append(
                f"Bot instance already exists: {self.MEETING_BOT_INSTANCE_FIELD}='{bot_instance}'"
            )
        else:
            self.passed.append(
                f"No existing bot instance ({self.MEETING_BOT_INSTANCE_FIELD} is empty)"
            )

    def _check_ai_settings(self, data: dict, user_data: dict = None):
        """Check AI assistant and auto_join settings."""
        ai_enabled = data.get("ai_assistant_enabled", False)

        # Get user_id
        user_id = data.get("user_id") or data.get("created_by")

        # Check user's auto_join setting
        auto_join = False
        if user_data:
            auto_join = user_data.get("auto_join_meetings", False)
        elif user_id and self.db:
            try:
                user_ref = self.db.collection("users").document(user_id)
                user_doc = user_ref.get()
                if user_doc.exists:
                    auto_join = user_doc.to_dict().get("auto_join_meetings", False)
            except Exception as e:
                self.warnings.append(f"Could not fetch user data: {e}")

        if ai_enabled:
            self.passed.append(f"ai_assistant_enabled=True")
        else:
            self.warnings.append(f"ai_assistant_enabled={ai_enabled}")

        if auto_join:
            self.passed.append(f"User has auto_join_meetings=True")
        else:
            self.warnings.append(f"User auto_join_meetings={auto_join}")

        if not (ai_enabled or auto_join):
            self.issues.append(
                f"Neither ai_assistant_enabled ({ai_enabled}) nor user auto_join_meetings ({auto_join}) is True"
            )

    def _check_teams_url(self, data: dict):
        """Check if this is a Teams meeting."""
        join_url = data.get("join_url") or ""

        if not join_url:
            self.issues.append("Missing 'join_url' field")
            return

        if "teams.microsoft.com" in join_url:
            self.passed.append(f"Valid Teams meeting URL: {join_url[:60]}...")
        else:
            self.issues.append(f"Not a Teams meeting URL: {join_url[:60]}...")

    def _check_session_dedup(self, data: dict, org_id: str):
        """Check session deduplication status."""
        join_url = data.get("join_url") or ""

        if not join_url or not org_id:
            return

        session_id = meeting_session_id(org_id, join_url)
        self.warnings.append(f"Session ID would be: {session_id[:16]}...")

        # Check if session exists in Firestore
        if self.db and org_id != "test_org":
            try:
                session_ref = (
                    self.db.collection("organizations")
                    .document(org_id)
                    .collection("meeting_sessions")
                    .document(session_id)
                )
                session_doc = session_ref.get()

                if session_doc.exists:
                    session_data = session_doc.to_dict()
                    status = session_data.get("status")
                    created_at = session_data.get("created_at")

                    terminal_states = ["complete", "failed", "cancelled", "error"]

                    if status in terminal_states:
                        self.passed.append(
                            f"Existing session in terminal state '{status}' - would be re-queued"
                        )
                    elif status == "queued":
                        self.warnings.append(
                            f"Session already queued (created {created_at}) - would add as subscriber"
                        )
                    elif status in ["processing", "claimed"]:
                        self.issues.append(
                            f"Session in active state '{status}' - bot may already be joining"
                        )
                    else:
                        self.warnings.append(f"Session exists with status '{status}'")
                else:
                    self.passed.append("No existing session - would create new one")
            except Exception as e:
                self.warnings.append(f"Could not check session: {e}")

    def _check_required_fields(self, data: dict):
        """Check for required fields."""
        required = ["join_url", "start"]
        recommended = ["user_id", "created_by", "title", "org_id"]

        for field in required:
            if not data.get(field):
                if field not in [i.split("'")[1] for i in self.issues if "'" in i]:
                    self.issues.append(f"Missing required field: '{field}'")

        for field in recommended:
            if not data.get(field):
                self.warnings.append(f"Missing recommended field: '{field}'")

    def find_au_standup_meetings(self, days: int = 7) -> list:
        """Find AU Standup meetings in the last N days."""
        if not self.db:
            print("Firestore not available")
            return []

        print(f"Searching for AU Standup meetings in last {days} days...")

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        meetings = []

        for org_doc in self.db.collection("organizations").stream():
            org_id = org_doc.id

            try:
                meetings_ref = (
                    self.db.collection("organizations")
                    .document(org_id)
                    .collection("meetings")
                )

                for meeting_doc in meetings_ref.stream():
                    data = meeting_doc.to_dict() or {}
                    title = data.get("title", "").lower()

                    if (
                        "au standup" in title
                        or "au-standup" in title
                        or "australia standup" in title
                    ):
                        meetings.append(
                            {
                                "org_id": org_id,
                                "meeting_id": meeting_doc.id,
                                "data": data,
                            }
                        )
            except Exception as e:
                print(f"Error querying org {org_id}: {e}")

        return meetings


def run_unit_tests():
    """Run unit tests without Firestore connection."""
    print("=" * 80)
    print("RUNNING UNIT TESTS")
    print("=" * 80)

    diag = MeetingDiagnostics("test-project")
    diag.db = None  # Disable Firestore

    # Test 1: Valid meeting
    print("\n--- Test 1: Valid meeting ---")
    valid_meeting = {
        "start": datetime.now(timezone.utc) + timedelta(minutes=2),
        "status": "confirmed",
        "ai_assistant_enabled": True,
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
        "user_id": "user123",
        "title": "AU Standup",
    }
    result = diag.diagnose_meeting(
        valid_meeting, "valid_meeting", "test_org", {"auto_join_meetings": True}
    )
    assert result["would_join"], "Valid meeting should join"
    print("✅ Test 1 passed")

    # Test 2: Missing ai_assistant_enabled and auto_join
    print("\n--- Test 2: No AI/auto_join ---")
    no_ai_meeting = {
        "start": datetime.now(timezone.utc) + timedelta(minutes=2),
        "status": "confirmed",
        "ai_assistant_enabled": False,
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
        "user_id": "user123",
    }
    result = diag.diagnose_meeting(
        no_ai_meeting, "no_ai_meeting", "test_org", {"auto_join_meetings": False}
    )
    assert not result["would_join"], "Meeting without AI/auto_join should not join"
    print("✅ Test 2 passed")

    # Test 3: Wrong status
    print("\n--- Test 3: Wrong status ---")
    wrong_status = {
        "start": datetime.now(timezone.utc) + timedelta(minutes=2),
        "status": "cancelled",
        "ai_assistant_enabled": True,
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
        "user_id": "user123",
    }
    result = diag.diagnose_meeting(wrong_status, "wrong_status", "test_org")
    assert not result["would_join"], "Cancelled meeting should not join"
    print("✅ Test 3 passed")

    # Test 4: Bot already exists
    print("\n--- Test 4: Bot already exists ---")
    bot_exists = {
        "start": datetime.now(timezone.utc) + timedelta(minutes=2),
        "status": "confirmed",
        "ai_assistant_enabled": True,
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
        "bot_instance_id": "existing-bot-123",
    }
    result = diag.diagnose_meeting(bot_exists, "bot_exists", "test_org")
    assert not result["would_join"], "Meeting with existing bot should not join"
    print("✅ Test 4 passed")

    # Test 5: Non-Teams URL
    print("\n--- Test 5: Non-Teams URL ---")
    non_teams = {
        "start": datetime.now(timezone.utc) + timedelta(minutes=2),
        "status": "confirmed",
        "ai_assistant_enabled": True,
        "join_url": "https://zoom.us/j/123456789",
        "user_id": "user123",
    }
    result = diag.diagnose_meeting(non_teams, "non_teams", "test_org")
    assert not result["would_join"], "Non-Teams meeting should not join"
    print("✅ Test 5 passed")

    # Test 6: ISO string start time
    print("\n--- Test 6: ISO string start time ---")
    iso_start = {
        "start": (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
        "status": "confirmed",
        "ai_assistant_enabled": True,
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
        "user_id": "user123",
    }
    result = diag.diagnose_meeting(
        iso_start, "iso_start", "test_org", {"auto_join_meetings": True}
    )
    assert result["would_join"], "ISO string start time should work"
    print("✅ Test 6 passed")

    # Test 7: Missing start time
    print("\n--- Test 7: Missing start time ---")
    no_start = {
        "status": "confirmed",
        "ai_assistant_enabled": True,
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
    }
    result = diag.diagnose_meeting(no_start, "no_start", "test_org")
    assert not result["would_join"], "Meeting without start time should not join"
    print("✅ Test 7 passed")

    print("\n" + "=" * 80)
    print("ALL UNIT TESTS PASSED ✅")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Diagnose AU Standup meeting issues")
    parser.add_argument("--project", help="GCP project ID")
    parser.add_argument("--database", default="(default)", help="Firestore database")
    parser.add_argument("--meeting-id", help="Specific meeting ID to diagnose")
    parser.add_argument("--org-id", help="Organization ID (required with --meeting-id)")
    parser.add_argument(
        "--find-au-standup",
        action="store_true",
        help="Find and diagnose AU Standup meetings",
    )
    parser.add_argument(
        "--unit-tests", action="store_true", help="Run unit tests without Firestore"
    )

    args = parser.parse_args()

    if args.unit_tests:
        run_unit_tests()
        return

    if not args.project:
        print("Error: --project is required (or use --unit-tests)")
        sys.exit(1)

    diag = MeetingDiagnostics(args.project, args.database)

    if args.find_au_standup:
        meetings = diag.find_au_standup_meetings()

        if not meetings:
            print("No AU Standup meetings found")
            return

        print(f"\nFound {len(meetings)} AU Standup meeting(s):\n")

        for m in meetings:
            print(f"Org: {m['org_id']}, Meeting: {m['meeting_id']}")
            print(f"  Title: {m['data'].get('title')}")
            print(f"  Start: {m['data'].get('start')}")
            print()

            diag.diagnose_meeting(m["data"], m["meeting_id"], m["org_id"])
            print()

    elif args.meeting_id:
        if not args.org_id:
            print("Error: --org-id is required with --meeting-id")
            sys.exit(1)

        # Fetch meeting from Firestore
        meeting_ref = (
            diag.db.collection("organizations")
            .document(args.org_id)
            .collection("meetings")
            .document(args.meeting_id)
        )
        meeting_doc = meeting_ref.get()

        if not meeting_doc.exists:
            print(f"Meeting {args.meeting_id} not found in org {args.org_id}")
            sys.exit(1)

        diag.diagnose_meeting(meeting_doc.to_dict(), args.meeting_id, args.org_id)

    else:
        print("Specify --meeting-id/--org-id, --find-au-standup, or --unit-tests")
        sys.exit(1)


if __name__ == "__main__":
    main()
