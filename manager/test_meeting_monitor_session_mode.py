import os
from unittest.mock import patch

import pytest

from meeting_monitor import MeetingMonitor


@pytest.fixture
def monitor():
    return MeetingMonitor(api_base_url="http://example.invalid")


def _completed_status(job_id: str):
    return {"status": "completed", "state": "finished", "job_id": job_id}


def test_session_mode_allows_missing_user_id_and_uses_session_dir(monitor, tmp_path):
    session_id = "abc123"
    fake_recording_dir = tmp_path / "sessions" / session_id
    fake_recording_dir.mkdir(parents=True)
    fake_recording = fake_recording_dir / "recording.webm"
    fake_recording.write_bytes(b"hello")

    metadata = {
        "gcs_path": f"recordings/sessions/{session_id}",
        "userId": None,
        "user_id": None,
    }

    with (
        patch.object(
            MeetingMonitor,
            "get_job_status",
            return_value=_completed_status("job"),
        ),
        patch.dict(os.environ, {"TEMPVIDEO_DIR": str(tmp_path)}),
    ):
        recording_path = monitor.monitor_until_complete(
            job_id="job", metadata=metadata, check_interval=0, max_wait_time=1
        )

    assert recording_path == str(fake_recording)


def test_session_mode_fallback_to_auto_generated_dir(monitor, tmp_path):
    """Test session jobs find recordings in AUTO-GENERATED folder.

    This happens when join_payload sets userId="AUTO-GENERATED" because
    metadata has no userId, and the bot writes recordings there.
    """
    session_id = "c7db60f411652e00daf262436cd2d09ac6c2bf6d7ed07f714724d7a06896b849"
    # Bot writes to AUTO-GENERATED folder when userId is missing
    fake_recording_dir = tmp_path / "AUTO-GENERATED"
    fake_recording_dir.mkdir(parents=True)
    fake_recording = fake_recording_dir / "recording.webm"
    fake_recording.write_bytes(b"test recording data")

    metadata = {
        "gcs_path": f"recordings/sessions/{session_id}",
        "userId": None,
        "user_id": None,
    }

    with (
        patch.object(
            MeetingMonitor,
            "get_job_status",
            return_value=_completed_status("job"),
        ),
        patch.dict(os.environ, {"TEMPVIDEO_DIR": str(tmp_path)}),
    ):
        recording_path = monitor.monitor_until_complete(
            job_id="job", metadata=metadata, check_interval=0, max_wait_time=1
        )

    assert recording_path == str(fake_recording)


def test_non_session_mode_requires_user_id(monitor):
    metadata = {
        "gcs_path": "recordings/someuser/somemeeting",
        "userId": None,
        "user_id": None,
    }

    with patch.object(
        MeetingMonitor, "get_job_status", return_value=_completed_status("job")
    ):
        recording_path = monitor.monitor_until_complete(
            job_id="job", metadata=metadata, check_interval=0, max_wait_time=1
        )

    assert recording_path is None
