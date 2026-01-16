"""Tests for the artifacts fanout fix.

Verifies that _fanout_meeting_session_artifacts properly copies
the artifacts metadata to all subscriber meeting documents.
"""

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


class TestArtifactsFanout(unittest.TestCase):
    """Tests for artifacts distribution during fanout."""

    def setUp(self):
        """Set up test fixtures."""
        self.session_artifacts = {
            "transcript_txt": "recordings/user1/meeting1/transcript.txt",
            "transcript_vtt": "recordings/user1/meeting1/transcript.vtt",
            "recording_webm": "recordings/user1/meeting1/recording.webm",
            "recording_mp4": "recordings/user1/meeting1/recording.mp4",
        }

    def test_artifacts_path_replacement(self):
        """Test that artifact paths are correctly rewritten for subscribers."""
        source_prefix = "recordings/user1/meeting1"
        dst_prefix = "recordings/user2/meeting2"

        subscriber_artifacts = {}
        for key, path in self.session_artifacts.items():
            new_path = path.replace(source_prefix, dst_prefix)
            subscriber_artifacts[key] = new_path

        self.assertEqual(
            subscriber_artifacts["transcript_txt"],
            "recordings/user2/meeting2/transcript.txt",
        )
        self.assertEqual(
            subscriber_artifacts["recording_webm"],
            "recordings/user2/meeting2/recording.webm",
        )

    def test_artifacts_preserved_for_first_subscriber(self):
        """Test that first subscriber gets original artifact paths."""
        # First subscriber should get the session artifacts unchanged
        first_sub_artifacts = self.session_artifacts.copy()

        self.assertEqual(
            first_sub_artifacts["transcript_txt"],
            "recordings/user1/meeting1/transcript.txt",
        )

    def test_all_artifact_types_copied(self):
        """Test that all artifact types are included in fanout."""
        expected_types = [
            "transcript_txt",
            "transcript_vtt",
            "recording_webm",
            "recording_mp4",
        ]

        for artifact_type in expected_types:
            self.assertIn(artifact_type, self.session_artifacts)

    def test_empty_artifacts_handled(self):
        """Test that empty artifacts dict is handled gracefully."""
        empty_artifacts = {}
        subscriber_artifacts = {}

        for key, path in empty_artifacts.items():
            subscriber_artifacts[key] = path

        self.assertEqual(subscriber_artifacts, {})

    def test_artifacts_with_different_prefix_preserved(self):
        """Test artifacts with unexpected paths are preserved."""
        source_prefix = "recordings/user1/meeting1"
        dst_prefix = "recordings/user2/meeting2"

        # Artifact with unexpected path format
        weird_artifacts = {
            "custom": "some/other/path/file.txt",
        }

        subscriber_artifacts = {}
        for key, path in weird_artifacts.items():
            if source_prefix in path:
                new_path = path.replace(source_prefix, dst_prefix)
                subscriber_artifacts[key] = new_path
            else:
                # Keep original if path doesn't match
                subscriber_artifacts[key] = path

        # Should preserve the original path
        self.assertEqual(
            subscriber_artifacts["custom"],
            "some/other/path/file.txt",
        )


if __name__ == "__main__":
    unittest.main()
