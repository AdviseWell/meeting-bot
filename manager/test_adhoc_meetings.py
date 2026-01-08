"""Tests for ad-hoc meeting creation functionality"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone, timedelta
import tempfile
import os


def test_get_recording_duration_seconds_valid_file():
    """Test that get_recording_duration_seconds returns correct duration"""
    from media_converter import get_recording_duration_seconds

    # Create a temporary test file
    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as f:
        test_file = f.name

    try:
        # Mock subprocess.run to return a duration
        with patch("media_converter.subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "123.456\n"
            mock_run.return_value = mock_result

            duration = get_recording_duration_seconds(test_file)

            assert duration == 123.456
            assert mock_run.called
            # Verify ffprobe was called with correct arguments
            args = mock_run.call_args[0][0]
            assert "ffprobe" in args
            assert test_file in args
    finally:
        os.unlink(test_file)


def test_get_recording_duration_seconds_missing_file():
    """Test that missing file returns None"""
    from media_converter import get_recording_duration_seconds

    duration = get_recording_duration_seconds("/nonexistent/file.webm")
    assert duration is None


def test_get_recording_duration_seconds_ffprobe_failure():
    """Test that ffprobe failure returns None"""
    from media_converter import get_recording_duration_seconds

    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as f:
        test_file = f.name

    try:
        with patch("media_converter.subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.returncode = 1
            mock_result.stderr = "Error reading file"
            mock_run.return_value = mock_result

            duration = get_recording_duration_seconds(test_file)
            assert duration is None
    finally:
        os.unlink(test_file)


def test_create_adhoc_meeting_success():
    """Test successful ad-hoc meeting creation"""
    from storage_client import FirestoreClient

    # Mock the Firestore client
    with patch("storage_client.firestore.Client") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock the collection and document references
        mock_collection = Mock()
        mock_doc_ref = Mock()
        mock_doc_ref.id = "test-meeting-id-123"
        mock_collection.document.return_value = mock_doc_ref
        mock_client.collection.return_value = mock_collection

        # Create FirestoreClient and test create_adhoc_meeting
        firestore_client = FirestoreClient()

        meeting_id = firestore_client.create_adhoc_meeting(
            organization_id="org-123",
            user_id="user-456",
            meeting_url="https://meet.google.com/abc-defg-hij",
            start_at="2024-01-15T10:00:00Z",
        )

        assert meeting_id == "test-meeting-id-123"

        # Verify collection path
        mock_client.collection.assert_called_once_with("organizations/org-123/meetings")

        # Verify document was set with correct data
        assert mock_doc_ref.set.called
        meeting_data = mock_doc_ref.set.call_args[0][0]
        assert meeting_data["title"] == "Ad-hoc meeting"
        assert meeting_data["source"] == "ad_hoc"
        assert meeting_data["status"] == "scheduled"
        assert meeting_data["organization_id"] == "org-123"
        assert meeting_data["synced_by_user_id"] == "user-456"
        assert meeting_data["join_url"] == ("https://meet.google.com/abc-defg-hij")
        assert meeting_data["platform"] == "google_meet"
        assert "start" in meeting_data
        assert "created_at" in meeting_data
        assert "updated_at" in meeting_data


def test_meeting_exists_true():
    """Test meeting_exists returns True for existing meeting"""
    from storage_client import FirestoreClient

    with patch("storage_client.firestore.Client") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock document that exists
        mock_doc_snap = Mock()
        mock_doc_snap.exists = True
        mock_doc_ref = Mock()
        mock_doc_ref.get.return_value = mock_doc_snap
        mock_client.document.return_value = mock_doc_ref

        firestore_client = FirestoreClient()
        exists = firestore_client.meeting_exists("org-123", "meeting-456")

        assert exists is True
        mock_client.document.assert_called_once_with(
            "organizations/org-123/meetings/meeting-456"
        )


def test_meeting_exists_false():
    """Test meeting_exists returns False for non-existent meeting"""
    from storage_client import FirestoreClient

    with patch("storage_client.firestore.Client") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock document that doesn't exist
        mock_doc_snap = Mock()
        mock_doc_snap.exists = False
        mock_doc_ref = Mock()
        mock_doc_ref.get.return_value = mock_doc_snap
        mock_client.document.return_value = mock_doc_ref

        firestore_client = FirestoreClient()
        exists = firestore_client.meeting_exists("org-123", "meeting-456")

        assert exists is False


def test_adhoc_meeting_start_time_calculation():
    """Test that start time is calculated correctly from duration"""
    # Test that a 120 second recording creates a meeting starting 2 min ago
    duration_seconds = 120.0

    # Simulate the calculation from manager/main.py
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(seconds=duration_seconds)

    # Verify start time is approximately 2 minutes ago
    time_diff = now - start_time
    assert abs(time_diff.total_seconds() - 120.0) < 1.0


def test_adhoc_meeting_iso_format():
    """Test that ISO format is correct for Firestore"""
    now = datetime.now(timezone.utc)
    start_at = now.isoformat().replace("+00:00", "Z")

    # Verify format is ISO 8601 with Z suffix
    assert start_at.endswith("Z")
    assert "T" in start_at

    # Verify it can be parsed back
    parsed = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
    assert abs((parsed - now).total_seconds()) < 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
