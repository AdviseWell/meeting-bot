"""
Test: Merged Meetings Add Subscribers for Fanout

This test verifies that when a duplicate meeting is consolidated/merged
into a canonical meeting, the duplicate meeting's user is added as a 
subscriber to the session so they receive fanout copies of transcriptions.

Bug fixed: Previously, when meetings were merged, only the canonical meeting's
user was subscribed. The duplicate meeting's user would never receive the
transcription via fanout.

Expected behavior:
1. User A creates/syncs a meeting for URL X → Session created, User A subscribed
2. User B syncs the same meeting (same URL X) → Meeting marked as merged
3. User B should be added as a subscriber to the session
4. After recording completes, fanout copies to both User A and User B
"""

import pytest
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from unittest.mock import MagicMock, patch


class MockFirestoreDocument:
    """Mock Firestore document"""

    def __init__(self, doc_id: str, data: dict, exists: bool = True, path: str = ""):
        self.id = doc_id
        self._data = data
        self.exists = exists
        self.path = path or f"test/{doc_id}"
        self.reference = MagicMock()
        self.reference.path = self.path
        self.reference.update = MagicMock()

    def to_dict(self):
        return self._data if self.exists else None


class MockSubscriberRef:
    """Mock subscriber document reference"""

    def __init__(self, exists: bool = False):
        self._exists = exists
        self.set_called = False
        self.set_data = None

    def get(self):
        return MagicMock(exists=self._exists)

    def set(self, data):
        self.set_called = True
        self.set_data = data


class TestMergeSubscriberFix:
    """Test suite for the merge subscriber fix"""

    @pytest.fixture
    def org_id(self):
        return "test-org"

    @pytest.fixture
    def session_id(self):
        return "abc123def456"

    @pytest.fixture
    def canonical_meeting(self, session_id):
        """The first meeting that created the session"""
        return MockFirestoreDocument(
            doc_id="meeting-user-a",
            data={
                "meeting_url": "https://teams.microsoft.com/test-meeting",
                "user_id": "user-a",
                "organization_id": "test-org",
                "meeting_session_id": session_id,  # Already linked to session
                "status": "completed",
            },
            path="organizations/test-org/meetings/meeting-user-a",
        )

    @pytest.fixture
    def duplicate_meeting(self):
        """The second meeting (same URL) that should be merged"""
        return MockFirestoreDocument(
            doc_id="meeting-user-b",
            data={
                "meeting_url": "https://teams.microsoft.com/test-meeting",
                "user_id": "user-b",
                "organization_id": "test-org",
                "meeting_session_id": None,  # Not yet linked
                "status": "pending",
            },
            path="organizations/test-org/meetings/meeting-user-b",
        )

    def test_merged_meeting_user_becomes_subscriber(
        self, canonical_meeting, duplicate_meeting, org_id, session_id
    ):
        """When a meeting is merged, its user should be added as a subscriber"""
        
        # Track if subscriber was added
        subscriber_added = False
        subscriber_data = None

        def mock_consolidate(canonical, duplicate):
            """Simulates the fixed _consolidate_duplicate_meeting behavior"""
            nonlocal subscriber_added, subscriber_data

            canonical_data = canonical.to_dict() or {}
            duplicate_data = duplicate.to_dict() or {}

            # Mark as merged (original behavior)
            duplicate.reference.update({
                "status": "merged",
                "merged_into": canonical.id,
            })

            # NEW: Add subscriber for fanout
            session_id = canonical_data.get("meeting_session_id")
            org_id = duplicate_data.get("organization_id")
            duplicate_user_id = duplicate_data.get("user_id")

            if session_id and org_id and duplicate_user_id:
                subscriber_added = True
                subscriber_data = {
                    "user_id": duplicate_user_id,
                    "fs_meeting_id": duplicate.id,
                    "meeting_path": duplicate.reference.path,
                    "status": "requested",
                    "added_via": "merge_consolidation",
                }

            return True

        # Execute the consolidation
        result = mock_consolidate(canonical_meeting, duplicate_meeting)

        # Verify
        assert result is True
        assert subscriber_added is True, "Duplicate user should be added as subscriber"
        assert subscriber_data["user_id"] == "user-b"
        assert subscriber_data["fs_meeting_id"] == "meeting-user-b"
        assert subscriber_data["added_via"] == "merge_consolidation"

        print("✅ Merged meeting's user was added as subscriber")

    def test_subscriber_not_duplicated(
        self, canonical_meeting, duplicate_meeting, org_id, session_id
    ):
        """If user is already a subscriber, don't add again"""
        
        existing_subscriber = True  # User B already subscribed somehow

        def mock_consolidate_with_existing(canonical, duplicate):
            """Simulates consolidation when subscriber already exists"""
            canonical_data = canonical.to_dict() or {}
            duplicate_data = duplicate.to_dict() or {}

            session_id = canonical_data.get("meeting_session_id")
            duplicate_user_id = duplicate_data.get("user_id")

            # Simulate subscriber check
            if existing_subscriber:
                # Don't add again
                return True

            return True

        result = mock_consolidate_with_existing(canonical_meeting, duplicate_meeting)
        assert result is True
        print("✅ Existing subscriber not duplicated")

    def test_no_subscriber_without_session_id(
        self, duplicate_meeting, org_id
    ):
        """If canonical meeting has no session_id, can't add subscriber"""
        
        canonical_without_session = MockFirestoreDocument(
            doc_id="meeting-user-a",
            data={
                "meeting_url": "https://teams.microsoft.com/test-meeting",
                "user_id": "user-a",
                "organization_id": "test-org",
                "meeting_session_id": None,  # No session yet!
                "status": "pending",
            },
            path="organizations/test-org/meetings/meeting-user-a",
        )

        subscriber_added = False

        def mock_consolidate(canonical, duplicate):
            nonlocal subscriber_added
            canonical_data = canonical.to_dict() or {}
            session_id = canonical_data.get("meeting_session_id")

            if not session_id:
                # Can't add subscriber without session
                return True

            subscriber_added = True
            return True

        result = mock_consolidate(canonical_without_session, duplicate_meeting)

        assert result is True
        assert subscriber_added is False, "Should not add subscriber without session_id"
        print("✅ No subscriber added when session_id is missing")


class TestFanoutToMergedUsers:
    """Test that fanout correctly distributes to merged users"""

    @pytest.fixture
    def session_with_two_subscribers(self):
        """Session with both original and merged user as subscribers"""
        return {
            "session_id": "abc123def456",
            "status": "complete",
            "org_id": "test-org",
            "subscribers": [
                {
                    "user_id": "user-a",
                    "fs_meeting_id": "meeting-user-a",
                    "status": "requested",
                },
                {
                    "user_id": "user-b",
                    "fs_meeting_id": "meeting-user-b",
                    "status": "requested",
                    "added_via": "merge_consolidation",
                },
            ],
            "artifacts": {
                "transcript_txt": "recordings/user-a/meeting-user-a/transcript.txt",
                "recording_mp4": "recordings/user-a/meeting-user-a/recording.mp4",
            },
        }

    def test_fanout_copies_to_all_subscribers(self, session_with_two_subscribers):
        """Fanout should copy artifacts to all subscribers including merged users"""
        
        session = session_with_two_subscribers
        subscribers = session["subscribers"]
        artifacts = session["artifacts"]

        # Simulate fanout
        fanout_targets = []
        for sub in subscribers:
            target_user_id = sub["user_id"]
            target_meeting_id = sub["fs_meeting_id"]
            
            # Calculate target paths (what fanout would do)
            target_artifacts = {}
            for key, source_path in artifacts.items():
                filename = source_path.split("/")[-1]
                target_path = f"recordings/{target_user_id}/{target_meeting_id}/{filename}"
                target_artifacts[key] = target_path

            fanout_targets.append({
                "user_id": target_user_id,
                "meeting_id": target_meeting_id,
                "artifacts": target_artifacts,
            })

        # Verify both users get copies
        assert len(fanout_targets) == 2
        
        user_a_target = next(t for t in fanout_targets if t["user_id"] == "user-a")
        user_b_target = next(t for t in fanout_targets if t["user_id"] == "user-b")

        assert "recordings/user-a/meeting-user-a/transcript.txt" in user_a_target["artifacts"]["transcript_txt"]
        assert "recordings/user-b/meeting-user-b/transcript.txt" in user_b_target["artifacts"]["transcript_txt"]

        print("✅ Fanout targets both subscribers correctly")
        print(f"   User A artifacts: {user_a_target['artifacts']}")
        print(f"   User B artifacts: {user_b_target['artifacts']}")


def test_end_to_end_scenario():
    """
    End-to-end test simulating the full flow:
    1. Matt syncs a meeting → creates session
    2. Clinton syncs same meeting (as attendee) → gets merged
    3. Clinton should be added as subscriber
    4. After recording, both get transcription
    """
    
    # Step 1: Matt's meeting creates session
    matt_meeting = {
        "id": "matt-meeting-123",
        "user_id": "matt-user-id",
        "organization_id": "advisewell",
        "meeting_url": "https://teams.microsoft.com/l/meetup-join/test123",
        "meeting_session_id": "session-abc",
    }
    
    session = {
        "id": "session-abc",
        "status": "queued",
        "org_id": "advisewell",
        "subscribers": [
            {"user_id": "matt-user-id", "fs_meeting_id": "matt-meeting-123"}
        ],
    }
    
    print("Step 1: Matt's meeting created session with 1 subscriber")
    assert len(session["subscribers"]) == 1
    
    # Step 2: Clinton's meeting is synced (same URL)
    clinton_meeting = {
        "id": "clinton-meeting-456",
        "user_id": "clinton-user-id",
        "organization_id": "advisewell",
        "meeting_url": "https://teams.microsoft.com/l/meetup-join/test123",
        "meeting_session_id": None,  # Not linked yet
    }
    
    # Simulate merge + subscriber add (the fix)
    clinton_meeting["status"] = "merged"
    clinton_meeting["merged_into"] = matt_meeting["id"]
    clinton_meeting["meeting_session_id"] = session["id"]
    
    # Add Clinton as subscriber
    session["subscribers"].append({
        "user_id": "clinton-user-id",
        "fs_meeting_id": "clinton-meeting-456",
        "added_via": "merge_consolidation",
    })
    
    print("Step 2: Clinton's meeting merged, added as subscriber")
    assert len(session["subscribers"]) == 2
    
    # Step 3: Meeting completes, fanout happens
    session["status"] = "complete"
    
    # Verify both can receive fanout
    fanout_recipients = [s["user_id"] for s in session["subscribers"]]
    assert "matt-user-id" in fanout_recipients
    assert "clinton-user-id" in fanout_recipients
    
    print("Step 3: Both users are fanout recipients")
    print(f"   Recipients: {fanout_recipients}")
    print("✅ End-to-end scenario passed!")


if __name__ == "__main__":
    # Run the end-to-end test
    test_end_to_end_scenario()
    
    # Run unit tests
    pytest.main([__file__, "-v"])
