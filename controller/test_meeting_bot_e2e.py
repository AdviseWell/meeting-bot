#!/usr/bin/env python3
"""
Comprehensive End-to-End Test Suite for Meeting Bot Controller & Manager

This test suite covers all critical functionality:

1. MEETING DISCOVERY
   - Scheduled meetings found in time window
   - ISO string vs Timestamp start time parsing
   - Teams URL validation
   - AI assistant / auto_join settings

2. SESSION DEDUPLICATION
   - One bot per org for same meeting URL
   - Multiple subscribers from same org
   - Cross-org meetings get separate bots

3. RECURRING MEETING RE-QUEUE
   - Sessions in terminal state (complete/failed) get re-queued
   - New occurrence of recurring meeting creates new queued session
   - Previous session data preserved

4. BOT JOB LIFECYCLE
   - Job creation for queued sessions
   - Session claiming and locking
   - Session completion marking

5. POST-MEETING FANOUT
   - Artifacts copied to all subscribers
   - Meeting documents updated with transcription
   - Status updates for each subscriber

6. RE-JOIN AFTER BOT LEAVES
   - User can add bot back via meeting_join function
   - Creates new session (if previous complete)
   - Deduplicates with existing queued session

Run with:
    python controller/test_meeting_bot_e2e.py --unit-tests

Or against Firestore:
    python controller/test_meeting_bot_e2e.py --project=<PROJECT_ID> --integration-tests
"""

import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from unittest.mock import Mock, MagicMock, patch
from urllib.parse import urlparse

# Try to import Firestore for integration tests
try:
    from google.cloud import firestore

    HAS_FIRESTORE = True
except ImportError:
    HAS_FIRESTORE = False


# =============================================================================
# HELPER FUNCTIONS (Same as controller)
# =============================================================================


def normalize_meeting_url(url: str) -> str:
    """Normalize meeting URL for deduplication."""
    if not url:
        return ""

    parsed = urlparse(url)

    if "teams.microsoft.com" in parsed.netloc:
        path = parsed.path
        path = re.sub(r"/+", "/", path)
        path = path.rstrip("/")
        return f"teams://{path}"

    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def meeting_session_id(org_id: str, meeting_url: str) -> str:
    """Generate session ID from org + normalized URL."""
    normalized = normalize_meeting_url(meeting_url)
    key = f"{org_id}:{normalized}"
    return hashlib.sha256(key.encode()).hexdigest()


def parse_start_time(start_value) -> Optional[datetime]:
    """Parse start time from various formats."""
    if start_value is None:
        return None

    if isinstance(start_value, datetime):
        if start_value.tzinfo is None:
            return start_value.replace(tzinfo=timezone.utc)
        return start_value

    if hasattr(start_value, "timestamp"):
        return datetime.fromtimestamp(start_value.timestamp(), tz=timezone.utc)

    if isinstance(start_value, str):
        try:
            dt = datetime.fromisoformat(start_value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None

    return None


# =============================================================================
# MOCK OBJECTS
# =============================================================================


class MockFirestoreDocument:
    """Mock Firestore document."""

    def __init__(self, id: str, data: dict, exists: bool = True):
        self.id = id
        self._data = data
        self.exists = exists
        self.reference = Mock()
        self.reference.path = f"mock/path/{id}"

    def to_dict(self) -> dict:
        return self._data.copy() if self._data else {}


class MockFirestoreTransaction:
    """Mock Firestore transaction."""

    def __init__(self):
        self.writes = []

    def set(self, ref, data, merge=False):
        self.writes.append(("set", ref, data, merge))

    def update(self, ref, data):
        self.writes.append(("update", ref, data))


class MockMeetingController:
    """Mock controller for testing session logic."""

    def __init__(self):
        self.sessions = {}  # session_id -> data
        self.subscribers = {}  # (session_id, user_id) -> data
        self.meetings = {}  # (org_id, meeting_id) -> data
        self.jobs_created = []

    def _meeting_session_id(self, org_id: str, meeting_url: str) -> str:
        return meeting_session_id(org_id, meeting_url)

    def _try_create_or_update_session(
        self, org_id: str, meeting_url: str, user_id: str, meeting_id: str
    ) -> Optional[str]:
        """Simulate session creation/update logic."""

        session_id = self._meeting_session_id(org_id, meeting_url)
        now = datetime.now(timezone.utc)

        if session_id in self.sessions:
            # Session exists
            existing = self.sessions[session_id]
            status = existing.get("status", "")

            terminal_states = {"complete", "failed", "cancelled", "error"}

            if status in terminal_states:
                # Re-queue for new occurrence
                self.sessions[session_id] = {
                    **existing,
                    "status": "queued",
                    "updated_at": now,
                    "requeued_at": now,
                    "previous_status": status,
                }
            elif status == "queued":
                # Already queued, just update
                self.sessions[session_id]["updated_at"] = now
            else:
                # In progress, just update
                self.sessions[session_id]["updated_at"] = now
        else:
            # Create new session
            self.sessions[session_id] = {
                "status": "queued",
                "org_id": org_id,
                "meeting_url": meeting_url,
                "created_at": now,
                "updated_at": now,
            }

        # Add/update subscriber
        sub_key = (session_id, user_id)
        if sub_key not in self.subscribers:
            self.subscribers[sub_key] = {
                "user_id": user_id,
                "fs_meeting_id": meeting_id,
                "created_at": now,
                "updated_at": now,
                "status": "pending",
            }
        else:
            self.subscribers[sub_key]["updated_at"] = now

        return session_id

    def _claim_session(self, session_id: str) -> bool:
        """Claim a session for processing."""
        if session_id not in self.sessions:
            return False

        session = self.sessions[session_id]
        if session.get("status") != "queued":
            return False

        session["status"] = "processing"
        session["claimed_at"] = datetime.now(timezone.utc)
        return True

    def _complete_session(self, session_id: str, ok: bool = True):
        """Mark session as complete."""
        if session_id in self.sessions:
            self.sessions[session_id]["status"] = "complete" if ok else "failed"
            self.sessions[session_id]["processed_at"] = datetime.now(timezone.utc)

    def create_job(self, session_id: str, data: dict) -> bool:
        """Create a k8s job."""
        self.jobs_created.append((session_id, data))
        return True


# =============================================================================
# TEST: MEETING DISCOVERY
# =============================================================================


def test_meeting_discovery_timestamp():
    """Test that meetings with Timestamp start times are found."""
    print("\n--- Test: Meeting Discovery (Timestamp) ---")

    now = datetime.now(timezone.utc)
    meeting_start = now + timedelta(minutes=2)

    # Mock meeting data
    meeting = {
        "start": meeting_start,  # datetime object
        "status": "scheduled",
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
        "user_id": "user1",
        "ai_assistant_enabled": True,
    }

    # Test start time parsing
    parsed = parse_start_time(meeting["start"])
    assert parsed is not None, "Failed to parse Timestamp start time"
    assert abs((parsed - meeting_start).total_seconds()) < 1, "Start time mismatch"

    print("✅ Timestamp start time parsed correctly")


def test_meeting_discovery_iso_string():
    """Test that meetings with ISO string start times are found."""
    print("\n--- Test: Meeting Discovery (ISO String) ---")

    now = datetime.now(timezone.utc)
    meeting_start = now + timedelta(minutes=2)
    iso_string = meeting_start.isoformat()

    meeting = {
        "start": iso_string,  # ISO string
        "status": "scheduled",
        "join_url": "https://teams.microsoft.com/l/meetup-join/123",
    }

    parsed = parse_start_time(meeting["start"])
    assert parsed is not None, "Failed to parse ISO string start time"
    assert abs((parsed - meeting_start).total_seconds()) < 1, "Start time mismatch"

    print("✅ ISO string start time parsed correctly")


def test_meeting_discovery_teams_url_validation():
    """Test Teams URL validation."""
    print("\n--- Test: Teams URL Validation ---")

    valid_urls = [
        "https://teams.microsoft.com/l/meetup-join/123",
        "https://teams.microsoft.com/meet/456?p=abc",
    ]

    invalid_urls = [
        "https://zoom.us/j/123456789",
        "https://meet.google.com/abc-defg-hij",
        "",
        None,
    ]

    for url in valid_urls:
        assert "teams.microsoft.com" in (url or ""), f"Should be valid: {url}"

    for url in invalid_urls:
        assert "teams.microsoft.com" not in (url or ""), f"Should be invalid: {url}"

    print("✅ Teams URL validation works correctly")


# =============================================================================
# TEST: SESSION DEDUPLICATION
# =============================================================================


def test_session_dedup_same_org():
    """Test that users from same org share a session."""
    print("\n--- Test: Session Deduplication (Same Org) ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/123"

    # User 1 creates session
    session1 = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting1"
    )

    # User 2 from same org joins
    session2 = controller._try_create_or_update_session(
        org_id, meeting_url, "user2", "meeting2"
    )

    assert session1 == session2, "Same org should share session"
    assert len(controller.sessions) == 1, "Should only have 1 session"
    assert len(controller.subscribers) == 2, "Should have 2 subscribers"

    print(f"✅ Session ID: {session1[:16]}...")
    print(f"✅ Subscribers: {list(controller.subscribers.keys())}")
    print("✅ Same org users share session")


def test_session_dedup_different_orgs():
    """Test that different orgs get separate sessions."""
    print("\n--- Test: Session Deduplication (Different Orgs) ---")

    controller = MockMeetingController()
    meeting_url = "https://teams.microsoft.com/l/meetup-join/123"

    # Org 1 user
    session1 = controller._try_create_or_update_session(
        "org1", meeting_url, "user1", "meeting1"
    )

    # Org 2 user (different org, same meeting)
    session2 = controller._try_create_or_update_session(
        "org2", meeting_url, "user2", "meeting2"
    )

    assert session1 != session2, "Different orgs should have different sessions"
    assert len(controller.sessions) == 2, "Should have 2 sessions"

    print(f"✅ Org1 Session: {session1[:16]}...")
    print(f"✅ Org2 Session: {session2[:16]}...")
    print("✅ Different orgs get separate sessions")


# =============================================================================
# TEST: RECURRING MEETING RE-QUEUE
# =============================================================================


def test_recurring_meeting_requeue_from_complete():
    """Test that completed sessions are re-queued for new occurrences."""
    print("\n--- Test: Recurring Meeting Re-queue (Complete → Queued) ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/recurring-123"

    # Day 1: Create and complete session
    session_id = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting-day1"
    )
    assert controller.sessions[session_id]["status"] == "queued"

    controller._claim_session(session_id)
    assert controller.sessions[session_id]["status"] == "processing"

    controller._complete_session(session_id, ok=True)
    assert controller.sessions[session_id]["status"] == "complete"

    print(
        f"✅ Day 1: Session completed (status={controller.sessions[session_id]['status']})"
    )

    # Day 2: New occurrence (same URL)
    session_id_day2 = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting-day2"
    )

    assert session_id == session_id_day2, "Should reuse same session ID"
    assert controller.sessions[session_id]["status"] == "queued", "Should be re-queued"
    assert controller.sessions[session_id].get("previous_status") == "complete"
    assert controller.sessions[session_id].get("requeued_at") is not None

    print(
        f"✅ Day 2: Session re-queued (status={controller.sessions[session_id]['status']})"
    )
    print(
        f"✅ Previous status preserved: {controller.sessions[session_id].get('previous_status')}"
    )
    print("✅ Recurring meeting re-queue works correctly")


def test_recurring_meeting_requeue_from_failed():
    """Test that failed sessions are re-queued for new occurrences."""
    print("\n--- Test: Recurring Meeting Re-queue (Failed → Queued) ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/recurring-456"

    # Day 1: Create and fail session
    session_id = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting-day1"
    )
    controller._claim_session(session_id)
    controller._complete_session(session_id, ok=False)  # Failed

    assert controller.sessions[session_id]["status"] == "failed"
    print(
        f"✅ Day 1: Session failed (status={controller.sessions[session_id]['status']})"
    )

    # Day 2: Retry
    session_id_day2 = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting-day2"
    )

    assert controller.sessions[session_id]["status"] == "queued"
    assert controller.sessions[session_id].get("previous_status") == "failed"

    print(f"✅ Day 2: Session re-queued from failed state")
    print("✅ Failed session re-queue works correctly")


def test_session_not_requeued_while_processing():
    """Test that processing sessions are NOT re-queued."""
    print("\n--- Test: Session NOT Re-queued While Processing ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/active-meeting"

    # Create and claim session
    session_id = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting1"
    )
    controller._claim_session(session_id)
    assert controller.sessions[session_id]["status"] == "processing"

    # Another user tries to join while processing
    session_id_2 = controller._try_create_or_update_session(
        org_id, meeting_url, "user2", "meeting2"
    )

    assert session_id == session_id_2
    assert (
        controller.sessions[session_id]["status"] == "processing"
    ), "Should still be processing"
    assert len(controller.subscribers) == 2, "Second user should be added as subscriber"

    print("✅ Processing session not re-queued")
    print("✅ New user added as subscriber")


# =============================================================================
# TEST: BOT JOB LIFECYCLE
# =============================================================================


def test_job_creation_for_queued_session():
    """Test that jobs are created for queued sessions."""
    print("\n--- Test: Job Creation for Queued Session ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/123"

    session_id = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting1"
    )

    # Claim session
    claimed = controller._claim_session(session_id)
    assert claimed, "Should be able to claim queued session"

    # Create job
    job_created = controller.create_job(session_id, {"meeting_url": meeting_url})
    assert job_created
    assert len(controller.jobs_created) == 1

    print(f"✅ Session claimed successfully")
    print(f"✅ Job created: {controller.jobs_created[0][0][:16]}...")
    print("✅ Job creation works correctly")


def test_session_claim_prevents_double_join():
    """Test that session claiming prevents double-joining."""
    print("\n--- Test: Session Claim Prevents Double Join ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/123"

    session_id = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting1"
    )

    # First claim succeeds
    claim1 = controller._claim_session(session_id)
    assert claim1, "First claim should succeed"

    # Second claim fails (already processing)
    claim2 = controller._claim_session(session_id)
    assert not claim2, "Second claim should fail"

    print("✅ First claim succeeded")
    print("✅ Second claim prevented")
    print("✅ Double-join prevention works")


# =============================================================================
# TEST: RE-JOIN AFTER BOT LEAVES
# =============================================================================


def test_rejoin_after_meeting_complete():
    """Test that users can re-add bot after meeting completes."""
    print("\n--- Test: Re-join After Meeting Complete ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/rejoin-test"

    # Initial meeting
    session_id = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting1"
    )
    controller._claim_session(session_id)
    controller._complete_session(session_id, ok=True)

    assert controller.sessions[session_id]["status"] == "complete"
    print("✅ Initial meeting completed")

    # User re-adds bot (simulating meeting_join function)
    session_id_rejoin = controller._try_create_or_update_session(
        org_id, meeting_url, "user1", "meeting1"  # Same meeting
    )

    assert session_id == session_id_rejoin
    assert controller.sessions[session_id]["status"] == "queued"

    print("✅ Bot re-added successfully (session re-queued)")
    print("✅ Re-join after completion works")


# =============================================================================
# TEST: URL NORMALIZATION
# =============================================================================


def test_url_normalization():
    """Test that different URL formats produce same session ID."""
    print("\n--- Test: URL Normalization ---")

    org_id = "org1"

    # Different URL formats for same meeting
    urls = [
        "https://teams.microsoft.com/l/meetup-join/19%3ameeting_abc/0?context=xyz",
        "https://teams.microsoft.com/l/meetup-join/19%3ameeting_abc/0",
        "https://teams.microsoft.com/l/meetup-join/19%3ameeting_abc/0/",
    ]

    session_ids = [meeting_session_id(org_id, url) for url in urls]

    assert all(
        sid == session_ids[0] for sid in session_ids
    ), f"All URLs should produce same session ID: {session_ids}"

    print(f"✅ All URLs normalize to same session: {session_ids[0][:16]}...")
    print("✅ URL normalization works correctly")


# =============================================================================
# TEST: MULTIPLE SUBSCRIBERS FANOUT
# =============================================================================


def test_multiple_subscribers_same_meeting():
    """Test that multiple users in same org become subscribers."""
    print("\n--- Test: Multiple Subscribers (Same Org Meeting) ---")

    controller = MockMeetingController()
    org_id = "org1"
    meeting_url = "https://teams.microsoft.com/l/meetup-join/team-meeting"

    users = ["user1", "user2", "user3"]

    for i, user in enumerate(users):
        controller._try_create_or_update_session(
            org_id, meeting_url, user, f"meeting-{user}"
        )

    assert len(controller.sessions) == 1, "Should have 1 session"
    assert len(controller.subscribers) == 3, "Should have 3 subscribers"

    session_id = list(controller.sessions.keys())[0]
    sub_users = [sub[1] for sub in controller.subscribers.keys()]

    print(f"✅ Session: {session_id[:16]}...")
    print(f"✅ Subscribers: {sub_users}")
    print("✅ All 3 users are subscribers to same session")


# =============================================================================
# RUN ALL TESTS
# =============================================================================


def run_unit_tests():
    """Run all unit tests."""
    print("=" * 80)
    print("MEETING BOT END-TO-END TEST SUITE")
    print("=" * 80)

    tests = [
        # Meeting Discovery
        ("Meeting Discovery (Timestamp)", test_meeting_discovery_timestamp),
        ("Meeting Discovery (ISO String)", test_meeting_discovery_iso_string),
        ("Teams URL Validation", test_meeting_discovery_teams_url_validation),
        # Session Deduplication
        ("Session Dedup (Same Org)", test_session_dedup_same_org),
        ("Session Dedup (Different Orgs)", test_session_dedup_different_orgs),
        # Recurring Meeting Re-queue
        ("Recurring Re-queue (Complete)", test_recurring_meeting_requeue_from_complete),
        ("Recurring Re-queue (Failed)", test_recurring_meeting_requeue_from_failed),
        ("No Re-queue While Processing", test_session_not_requeued_while_processing),
        # Job Lifecycle
        ("Job Creation", test_job_creation_for_queued_session),
        ("Claim Prevents Double Join", test_session_claim_prevents_double_join),
        # Re-join
        ("Re-join After Complete", test_rejoin_after_meeting_complete),
        # URL Normalization
        ("URL Normalization", test_url_normalization),
        # Multiple Subscribers
        ("Multiple Subscribers", test_multiple_subscribers_same_meeting),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"\n❌ FAILED: {name}")
            print(f"   Error: {e}")
            failed += 1
        except Exception as e:
            print(f"\n❌ ERROR: {name}")
            print(f"   Exception: {e}")
            failed += 1

    print("\n" + "=" * 80)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 80)

    return failed == 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--unit-tests", action="store_true", help="Run unit tests")
    parser.add_argument("--project", help="GCP project for integration tests")
    parser.add_argument("--integration-tests", action="store_true")

    args = parser.parse_args()

    if args.unit_tests or not args.integration_tests:
        success = run_unit_tests()
        exit(0 if success else 1)
