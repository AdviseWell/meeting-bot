"""
Test that the manager correctly detects session mode and marks sessions complete.

This tests the fix for the fanout bug where the manager was checking for
'recordings/sessions/' GCS path prefix instead of MEETING_SESSION_ID env var.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call


class TestSessionModeDetection(unittest.TestCase):
    """Test that session mode is correctly detected via MEETING_SESSION_ID."""

    def setUp(self):
        """Clear any cached modules and set up clean environment."""
        # Remove cached module if present
        if "manager.main" in sys.modules:
            del sys.modules["manager.main"]
        if "__main__" in sys.modules:
            # Can't delete __main__, but we'll import fresh
            pass

    def tearDown(self):
        """Clean up environment variables."""
        for key in [
            "MEETING_SESSION_ID",
            "meeting_session_id",
            "MEETING_URL",
            "MEETING_ID",
            "USER_ID",
            "GCS_BUCKET",
            "GCS_PATH",
            "teamId",
            "TEAM_ID",
        ]:
            if key in os.environ:
                del os.environ[key]

    @patch("manager.main.load_meeting_metadata")
    def test_session_mode_detected_with_meeting_session_id(self, mock_metadata):
        """Manager should detect session mode when MEETING_SESSION_ID is set."""
        mock_metadata.return_value = {}

        os.environ["MEETING_URL"] = "https://teams.microsoft.com/l/meetup-join/test"
        os.environ["MEETING_ID"] = "test-meeting-id"
        os.environ["USER_ID"] = "test-user-id"
        os.environ["GCS_BUCKET"] = "test-bucket"
        os.environ["GCS_PATH"] = "recordings/test-user-id/test-meeting-id"
        os.environ["MEETING_SESSION_ID"] = "abc123session"
        os.environ["teamId"] = "advisewell"

        from manager.main import MeetingManager

        mgr = MeetingManager()

        self.assertEqual(mgr.meeting_session_id, "abc123session")

    @patch("manager.main.load_meeting_metadata")
    def test_session_mode_not_detected_without_meeting_session_id(self, mock_metadata):
        """Manager should NOT detect session mode without MEETING_SESSION_ID."""
        mock_metadata.return_value = {}

        os.environ["MEETING_URL"] = "https://teams.microsoft.com/l/meetup-join/test"
        os.environ["MEETING_ID"] = "test-meeting-id"
        os.environ["USER_ID"] = "test-user-id"
        os.environ["GCS_BUCKET"] = "test-bucket"
        os.environ["GCS_PATH"] = "recordings/test-user-id/test-meeting-id"
        os.environ["teamId"] = "advisewell"

        from manager.main import MeetingManager

        mgr = MeetingManager()

        self.assertEqual(mgr.meeting_session_id, "")

    @patch("manager.main.load_meeting_metadata")
    def test_mark_session_complete_called_when_session_mode(self, mock_metadata):
        """_mark_session_complete should update Firestore when session_id is set."""
        mock_metadata.return_value = {}

        os.environ["MEETING_URL"] = "https://teams.microsoft.com/l/meetup-join/test"
        os.environ["MEETING_ID"] = "test-meeting-id"
        os.environ["USER_ID"] = "test-user-id"
        os.environ["GCS_BUCKET"] = "test-bucket"
        os.environ["GCS_PATH"] = "recordings/test-user-id/test-meeting-id"
        os.environ["MEETING_SESSION_ID"] = "abc123session"
        os.environ["teamId"] = "advisewell"

        from manager.main import MeetingManager

        mgr = MeetingManager()

        # Mock the Firestore client
        with patch("manager.main.firestore") as mock_firestore:
            mock_db = MagicMock()
            mock_firestore.Client.return_value = mock_db
            mock_ref = MagicMock()
            mock_db.collection.return_value.document.return_value.collection.return_value.document.return_value = (
                mock_ref
            )

            # Call _mark_session_complete
            artifacts = {"video": "test.mp4", "transcript": "test.txt"}
            mgr._mark_session_complete(ok=True, artifacts=artifacts)

            # Verify Firestore was called
            mock_db.collection.assert_called_with("organizations")
            mock_ref.set.assert_called_once()

            # Check the payload
            call_args = mock_ref.set.call_args
            payload = call_args[0][0]
            self.assertEqual(payload["status"], "complete")
            self.assertIn("artifacts", payload)
            self.assertEqual(payload["artifacts"]["video"], "test.mp4")

    @patch("manager.main.load_meeting_metadata")
    def test_mark_session_complete_skipped_without_session_mode(self, mock_metadata):
        """_mark_session_complete should do nothing without session_id."""
        mock_metadata.return_value = {}

        os.environ["MEETING_URL"] = "https://teams.microsoft.com/l/meetup-join/test"
        os.environ["MEETING_ID"] = "test-meeting-id"
        os.environ["USER_ID"] = "test-user-id"
        os.environ["GCS_BUCKET"] = "test-bucket"
        os.environ["GCS_PATH"] = "recordings/test-user-id/test-meeting-id"
        os.environ["teamId"] = "advisewell"

        from manager.main import MeetingManager

        mgr = MeetingManager()

        # Mock the Firestore client - should NOT be called
        with patch("manager.main.firestore") as mock_firestore:
            mock_db = MagicMock()
            mock_firestore.Client.return_value = mock_db

            # Call _mark_session_complete
            mgr._mark_session_complete(ok=True, artifacts={"video": "test.mp4"})

            # Verify Firestore was NOT called
            mock_firestore.Client.assert_not_called()

    @patch("manager.main.load_meeting_metadata")
    def test_mark_session_complete_skipped_without_org_id(self, mock_metadata):
        """_mark_session_complete should warn and skip without org_id."""
        mock_metadata.return_value = {}

        os.environ["MEETING_URL"] = "https://teams.microsoft.com/l/meetup-join/test"
        os.environ["MEETING_ID"] = "test-meeting-id"
        os.environ["USER_ID"] = "test-user-id"
        os.environ["GCS_BUCKET"] = "test-bucket"
        os.environ["GCS_PATH"] = "recordings/test-user-id/test-meeting-id"
        os.environ["MEETING_SESSION_ID"] = "abc123session"
        # NO teamId set!

        from manager.main import MeetingManager

        mgr = MeetingManager()

        # Mock the Firestore client - should NOT be called
        with patch("manager.main.firestore") as mock_firestore:
            mock_db = MagicMock()
            mock_firestore.Client.return_value = mock_db

            # Call _mark_session_complete
            mgr._mark_session_complete(ok=True, artifacts={"video": "test.mp4"})

            # Verify Firestore was NOT called (no org_id)
            mock_firestore.Client.assert_not_called()


class TestSessionModeDetectionRegression(unittest.TestCase):
    """Regression test: ensure old GCS path check no longer controls session mode."""

    def tearDown(self):
        """Clean up environment variables."""
        for key in [
            "MEETING_SESSION_ID",
            "MEETING_URL",
            "MEETING_ID",
            "USER_ID",
            "GCS_BUCKET",
            "GCS_PATH",
            "teamId",
        ]:
            if key in os.environ:
                del os.environ[key]

    @patch("manager.main.load_meeting_metadata")
    def test_gcs_path_does_not_control_session_mode(self, mock_metadata):
        """
        Regression test: GCS path starting with 'recordings/sessions/' should NOT
        be the trigger for session mode. Only MEETING_SESSION_ID should matter.
        """
        mock_metadata.return_value = {}

        os.environ["MEETING_URL"] = "https://teams.microsoft.com/l/meetup-join/test"
        os.environ["MEETING_ID"] = "test-meeting-id"
        os.environ["USER_ID"] = "test-user-id"
        os.environ["GCS_BUCKET"] = "test-bucket"
        # Old-style session path, but NO MEETING_SESSION_ID
        os.environ["GCS_PATH"] = "recordings/sessions/abc123"
        os.environ["teamId"] = "advisewell"

        from manager.main import MeetingManager

        mgr = MeetingManager()

        # Session mode should NOT be detected without MEETING_SESSION_ID
        self.assertEqual(mgr.meeting_session_id, "")

        # _mark_session_complete should do nothing
        with patch("manager.main.firestore") as mock_firestore:
            mgr._mark_session_complete(ok=True, artifacts={"video": "test.mp4"})
            mock_firestore.Client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
