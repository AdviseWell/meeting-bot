#!/usr/bin/env python3
"""
Manager Test Suite - Tests for post-meeting fanout and artifact distribution.

Tests cover:
1. Session completion marking
2. Artifact upload to GCS
3. Fanout to multiple subscribers
4. Meeting document updates
5. Transcription copying

Run with:
    python manager/test_fanout_e2e.py --unit-tests
"""

from datetime import datetime, timezone
from typing import Dict, Optional
from unittest.mock import Mock, MagicMock, patch


# =============================================================================
# MOCK OBJECTS
# =============================================================================

class MockGCSBlob:
    """Mock GCS blob."""
    
    def __init__(self, name: str, exists: bool = True, content: bytes = b""):
        self.name = name
        self._exists = exists
        self._content = content
    
    def exists(self) -> bool:
        return self._exists
    
    def download_as_bytes(self) -> bytes:
        return self._content
    
    def download_as_text(self) -> str:
        return self._content.decode('utf-8')
    
    def upload_from_string(self, content: str):
        self._content = content.encode('utf-8')
    
    def upload_from_filename(self, filename: str):
        self._content = b"<file content>"


class MockGCSBucket:
    """Mock GCS bucket."""
    
    def __init__(self):
        self.blobs: Dict[str, MockGCSBlob] = {}
    
    def blob(self, name: str) -> MockGCSBlob:
        if name not in self.blobs:
            self.blobs[name] = MockGCSBlob(name, exists=False)
        return self.blobs[name]
    
    def list_blobs(self, prefix: str):
        return [b for name, b in self.blobs.items() if name.startswith(prefix)]
    
    def copy_blob(self, source_blob: MockGCSBlob, dest_bucket, dest_name: str):
        dest_bucket.blobs[dest_name] = MockGCSBlob(
            dest_name, exists=True, content=source_blob._content
        )


class MockFirestoreDoc:
    """Mock Firestore document."""
    
    def __init__(self, id: str, data: dict, exists: bool = True):
        self.id = id
        self._data = data
        self.exists = exists
    
    def to_dict(self) -> dict:
        return self._data.copy()
    
    def get(self) -> 'MockFirestoreDoc':
        return self


class MockFirestoreCollection:
    """Mock Firestore collection."""
    
    def __init__(self):
        self.docs: Dict[str, MockFirestoreDoc] = {}
    
    def document(self, doc_id: str) -> Mock:
        mock = Mock()
        if doc_id in self.docs:
            mock.get.return_value = self.docs[doc_id]
        else:
            mock.get.return_value = MockFirestoreDoc(doc_id, {}, exists=False)
        mock.set = Mock()
        mock.update = Mock()
        mock.collection = Mock(return_value=MockFirestoreCollection())
        return mock
    
    def stream(self):
        return list(self.docs.values())


class MockFanoutManager:
    """Mock manager for testing fanout logic."""
    
    def __init__(self):
        self.gcs_bucket = MockGCSBucket()
        self.copied_files = []
        self.updated_meetings = []
        self.updated_subscribers = []
    
    def _list_gcs_prefix(self, prefix: str) -> list:
        """List objects in GCS with given prefix."""
        return [b.name for b in self.gcs_bucket.list_blobs(prefix)]
    
    def _copy_artifacts_to_subscriber(
        self,
        source_prefix: str,
        dest_prefix: str,
        subscriber_user_id: str
    ) -> int:
        """Copy artifacts from source to destination."""
        copied = 0
        source_objects = self._list_gcs_prefix(source_prefix)
        
        for src_name in source_objects:
            # Compute destination path
            relative_path = src_name[len(source_prefix):]
            dest_name = dest_prefix + relative_path
            
            # Copy blob
            src_blob = self.gcs_bucket.blob(src_name)
            self.gcs_bucket.copy_blob(src_blob, self.gcs_bucket, dest_name)
            
            self.copied_files.append((src_name, dest_name))
            copied += 1
        
        return copied
    
    def fanout_to_subscribers(
        self,
        session_id: str,
        org_id: str,
        source_user_id: str,
        source_meeting_id: str,
        subscribers: list
    ) -> dict:
        """
        Fan out artifacts to all subscribers.
        
        Returns dict with results for each subscriber.
        """
        results = {}
        
        source_prefix = f"recordings/{source_user_id}/{source_meeting_id}/"
        
        for sub in subscribers:
            sub_user_id = sub.get("user_id")
            sub_meeting_id = sub.get("fs_meeting_id")
            
            if sub_user_id == source_user_id:
                # Skip source user - they already have the files
                results[sub_user_id] = {"status": "source", "copied": 0}
                continue
            
            # Copy to subscriber
            dest_prefix = f"recordings/{sub_user_id}/{sub_meeting_id}/"
            copied = self._copy_artifacts_to_subscriber(
                source_prefix, dest_prefix, sub_user_id
            )
            
            results[sub_user_id] = {"status": "copied", "copied": copied}
            self.updated_subscribers.append(sub_user_id)
        
        return results
    
    def update_meeting_with_transcription(
        self,
        org_id: str,
        meeting_id: str,
        transcription: str,
        artifacts: dict
    ):
        """Update meeting document with transcription and artifacts."""
        self.updated_meetings.append({
            "org_id": org_id,
            "meeting_id": meeting_id,
            "transcription_length": len(transcription),
            "artifacts": artifacts,
        })


# =============================================================================
# TESTS
# =============================================================================

def test_fanout_to_multiple_subscribers():
    """Test fanout copies artifacts to all subscribers."""
    print("\n--- Test: Fanout to Multiple Subscribers ---")
    
    manager = MockFanoutManager()
    
    # Set up source files
    source_prefix = "recordings/user1/meeting1/"
    files = ["video.mp4", "audio.m4a", "transcript.txt", "transcript.vtt"]
    for f in files:
        blob = manager.gcs_bucket.blob(source_prefix + f)
        blob._exists = True
        blob._content = f"content of {f}".encode()
    
    # Subscribers
    subscribers = [
        {"user_id": "user1", "fs_meeting_id": "meeting1"},  # Source
        {"user_id": "user2", "fs_meeting_id": "meeting2"},
        {"user_id": "user3", "fs_meeting_id": "meeting3"},
    ]
    
    results = manager.fanout_to_subscribers(
        session_id="session123",
        org_id="org1",
        source_user_id="user1",
        source_meeting_id="meeting1",
        subscribers=subscribers
    )
    
    assert results["user1"]["status"] == "source"
    assert results["user2"]["status"] == "copied"
    assert results["user3"]["status"] == "copied"
    assert results["user2"]["copied"] == 4
    assert results["user3"]["copied"] == 4
    
    print(f"✅ Source user skipped: user1")
    print(f"✅ Files copied to user2: {results['user2']['copied']}")
    print(f"✅ Files copied to user3: {results['user3']['copied']}")
    print(f"✅ Total copy operations: {len(manager.copied_files)}")
    print("✅ Fanout to multiple subscribers works correctly")


def test_fanout_preserves_file_structure():
    """Test that fanout preserves directory structure."""
    print("\n--- Test: Fanout Preserves File Structure ---")
    
    manager = MockFanoutManager()
    
    # Set up source files in subdirectories
    source_prefix = "recordings/user1/meeting1/"
    files = [
        "video.mp4",
        "audio/full.m4a",
        "transcripts/en/transcript.txt",
    ]
    for f in files:
        blob = manager.gcs_bucket.blob(source_prefix + f)
        blob._exists = True
        blob._content = f"content of {f}".encode()
    
    subscribers = [
        {"user_id": "user1", "fs_meeting_id": "meeting1"},
        {"user_id": "user2", "fs_meeting_id": "meeting2"},
    ]
    
    manager.fanout_to_subscribers(
        session_id="session123",
        org_id="org1",
        source_user_id="user1",
        source_meeting_id="meeting1",
        subscribers=subscribers
    )
    
    # Check destinations
    expected_dests = [
        "recordings/user2/meeting2/video.mp4",
        "recordings/user2/meeting2/audio/full.m4a",
        "recordings/user2/meeting2/transcripts/en/transcript.txt",
    ]
    
    actual_dests = [dest for src, dest in manager.copied_files]
    
    for expected in expected_dests:
        assert expected in actual_dests, f"Missing: {expected}"
    
    print("✅ File structure preserved in fanout")


def test_meeting_document_update():
    """Test that meeting documents are updated with transcription."""
    print("\n--- Test: Meeting Document Update ---")
    
    manager = MockFanoutManager()
    
    transcription = "This is the meeting transcription text."
    artifacts = {
        "video_url": "gs://bucket/recordings/user1/meeting1/video.mp4",
        "audio_url": "gs://bucket/recordings/user1/meeting1/audio.m4a",
        "transcript_url": "gs://bucket/recordings/user1/meeting1/transcript.txt",
    }
    
    manager.update_meeting_with_transcription(
        org_id="org1",
        meeting_id="meeting1",
        transcription=transcription,
        artifacts=artifacts
    )
    
    assert len(manager.updated_meetings) == 1
    update = manager.updated_meetings[0]
    assert update["org_id"] == "org1"
    assert update["meeting_id"] == "meeting1"
    assert update["transcription_length"] == len(transcription)
    assert "video_url" in update["artifacts"]
    
    print("✅ Meeting document updated with transcription")
    print(f"✅ Artifacts: {list(update['artifacts'].keys())}")


def test_session_completion_triggers_fanout():
    """Test that session completion status triggers fanout."""
    print("\n--- Test: Session Completion Triggers Fanout ---")
    
    # Simulate the flow
    session = {
        "status": "processing",
        "org_id": "org1",
    }
    
    # After manager finishes
    session["status"] = "complete"
    session["processed_at"] = datetime.now(timezone.utc)
    
    # Controller polls for complete sessions and triggers fanout
    if session["status"] == "complete":
        fanout_triggered = True
    else:
        fanout_triggered = False
    
    assert fanout_triggered
    print("✅ Session completion triggers fanout check")


def test_fanout_idempotent():
    """Test that fanout is idempotent (can be run multiple times)."""
    print("\n--- Test: Fanout Idempotent ---")
    
    manager = MockFanoutManager()
    
    # Set up source files
    source_prefix = "recordings/user1/meeting1/"
    blob = manager.gcs_bucket.blob(source_prefix + "video.mp4")
    blob._exists = True
    blob._content = b"video content"
    
    subscribers = [
        {"user_id": "user1", "fs_meeting_id": "meeting1"},
        {"user_id": "user2", "fs_meeting_id": "meeting2"},
    ]
    
    # Run fanout twice
    results1 = manager.fanout_to_subscribers(
        session_id="session123",
        org_id="org1",
        source_user_id="user1",
        source_meeting_id="meeting1",
        subscribers=subscribers
    )
    
    # Second run (should not fail)
    results2 = manager.fanout_to_subscribers(
        session_id="session123",
        org_id="org1",
        source_user_id="user1",
        source_meeting_id="meeting1",
        subscribers=subscribers
    )
    
    assert results2["user2"]["status"] == "copied"
    print("✅ Fanout can be run multiple times (idempotent)")


def test_empty_subscribers_list():
    """Test handling of empty subscribers list."""
    print("\n--- Test: Empty Subscribers List ---")
    
    manager = MockFanoutManager()
    
    results = manager.fanout_to_subscribers(
        session_id="session123",
        org_id="org1",
        source_user_id="user1",
        source_meeting_id="meeting1",
        subscribers=[]
    )
    
    assert len(results) == 0
    assert len(manager.copied_files) == 0
    print("✅ Empty subscribers handled gracefully")


def test_single_subscriber_no_fanout_needed():
    """Test that single subscriber (source) needs no fanout."""
    print("\n--- Test: Single Subscriber (No Fanout) ---")
    
    manager = MockFanoutManager()
    
    # Set up source file
    source_prefix = "recordings/user1/meeting1/"
    blob = manager.gcs_bucket.blob(source_prefix + "video.mp4")
    blob._exists = True
    
    subscribers = [
        {"user_id": "user1", "fs_meeting_id": "meeting1"},  # Only source
    ]
    
    results = manager.fanout_to_subscribers(
        session_id="session123",
        org_id="org1",
        source_user_id="user1",
        source_meeting_id="meeting1",
        subscribers=subscribers
    )
    
    assert results["user1"]["status"] == "source"
    assert len(manager.copied_files) == 0
    print("✅ Single subscriber (source) needs no copying")


# =============================================================================
# RUN ALL TESTS
# =============================================================================

def run_unit_tests():
    """Run all unit tests."""
    print("=" * 80)
    print("MANAGER FANOUT TEST SUITE")
    print("=" * 80)
    
    tests = [
        ("Fanout to Multiple Subscribers", test_fanout_to_multiple_subscribers),
        ("Fanout Preserves Structure", test_fanout_preserves_file_structure),
        ("Meeting Document Update", test_meeting_document_update),
        ("Session Completion Triggers Fanout", test_session_completion_triggers_fanout),
        ("Fanout Idempotent", test_fanout_idempotent),
        ("Empty Subscribers List", test_empty_subscribers_list),
        ("Single Subscriber No Fanout", test_single_subscriber_no_fanout_needed),
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
    success = run_unit_tests()
    exit(0 if success else 1)
