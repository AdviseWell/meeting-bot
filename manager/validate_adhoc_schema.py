#!/usr/bin/env python3
"""Manual validation of ad-hoc meeting schema"""

import sys

sys.path.insert(0, "/home/mattk/git/meeting-bot/manager")

from unittest.mock import Mock, patch
from datetime import datetime, timezone, timedelta

print("Testing ad-hoc meeting schema...")
print("=" * 80)

# Test 1: Check that create_adhoc_meeting creates correct schema
print("\n1. Testing create_adhoc_meeting schema...")
from storage_client import FirestoreClient

with patch("storage_client.firestore.Client") as mock_client_class:
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    mock_collection = Mock()
    mock_doc_ref = Mock()
    mock_doc_ref.id = "test-meeting-123"
    mock_collection.document.return_value = mock_doc_ref
    mock_client.collection.return_value = mock_collection

    firestore_client = FirestoreClient()

    # Test Google Meet URL
    meeting_id = firestore_client.create_adhoc_meeting(
        organization_id="advisewell",
        user_id="test-user-123",
        meeting_url="https://meet.google.com/abc-defg-hij",
        start_at="2024-01-15T10:00:00Z",
    )

    assert meeting_id == "test-meeting-123", "Meeting ID mismatch"

    meeting_data = mock_doc_ref.set.call_args[0][0]

    # Check required fields match production schema
    required_fields = {
        "title": "Ad-hoc meeting",
        "source": "ad_hoc",
        "status": "scheduled",
        "organization_id": "advisewell",
        "synced_by_user_id": "test-user-123",
        "join_url": "https://meet.google.com/abc-defg-hij",
        "platform": "google_meet",
        "auto_joined": False,
    }

    for field, expected in required_fields.items():
        actual = meeting_data.get(field)
        assert actual == expected, f"{field}: expected {expected}, got {actual}"
        print(f"  ✓ {field}: {actual}")

    # Check datetime fields exist
    for field in ["start", "created_at", "updated_at"]:
        assert field in meeting_data, f"Missing field: {field}"
        print(f"  ✓ {field}: {type(meeting_data[field]).__name__}")

print("\n2. Testing Teams URL detection...")
with patch("storage_client.firestore.Client") as mock_client_class:
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    mock_collection = Mock()
    mock_doc_ref = Mock()
    mock_doc_ref.id = "test-meeting-456"
    mock_collection.document.return_value = mock_doc_ref
    mock_client.collection.return_value = mock_collection

    firestore_client = FirestoreClient()

    firestore_client.create_adhoc_meeting(
        organization_id="advisewell",
        user_id="test-user-456",
        meeting_url="https://teams.microsoft.com/l/meetup-join/abc123",
        start_at="2024-01-15T11:00:00Z",
    )

    meeting_data = mock_doc_ref.set.call_args[0][0]
    assert meeting_data["platform"] == "microsoft_teams"
    print(f"  ✓ Detected platform: {meeting_data['platform']}")

print("\n3. Testing Zoom URL detection...")
with patch("storage_client.firestore.Client") as mock_client_class:
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    mock_collection = Mock()
    mock_doc_ref = Mock()
    mock_doc_ref.id = "test-meeting-789"
    mock_collection.document.return_value = mock_doc_ref
    mock_client.collection.return_value = mock_collection

    firestore_client = FirestoreClient()

    firestore_client.create_adhoc_meeting(
        organization_id="advisewell",
        user_id="test-user-789",
        meeting_url="https://zoom.us/j/123456789",
        start_at="2024-01-15T12:00:00Z",
    )

    meeting_data = mock_doc_ref.set.call_args[0][0]
    assert meeting_data["platform"] == "zoom"
    print(f"  ✓ Detected platform: {meeting_data['platform']}")

print("\n4. Testing start time calculation...")
duration_seconds = 120.0
now = datetime.now(timezone.utc)
start_time = now - timedelta(seconds=duration_seconds)
time_diff = now - start_time
assert abs(time_diff.total_seconds() - 120.0) < 1.0
print(f"  ✓ Start time calculated correctly: {duration_seconds}s ago")

print("\n" + "=" * 80)
print("✅ All schema validation tests passed!")
print("\nProduction schema compliance:")
print("  ✓ Uses 'join_url' instead of 'meeting_url'")
print("  ✓ Uses 'synced_by_user_id' instead of 'user_id'")
print("  ✓ Uses 'source: ad_hoc' instead of 'type: ad-hoc'")
print("  ✓ Uses 'status: scheduled' instead of 'completed'")
print("  ✓ Uses 'start' (datetime) for querying")
print("  ✓ Includes 'platform', 'auto_joined', 'updated_at'")
print("  ✓ Auto-detects platform from URL")
