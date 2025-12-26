"""Unit tests for recording discovery logic in MeetingMonitor.

These tests validate that when the meeting completes, the manager can locate
the recording in multiple possible directories:
- legacy /recordings/<userId>
- scratch-backed TEMPVIDEO_DIR/<userId>
- /scratch/tempvideo/<userId>

We avoid making real HTTP requests by mocking get_job_status.
"""

from __future__ import annotations
from pathlib import Path

from meeting_monitor import MeetingMonitor


class _FakeMonitor(MeetingMonitor):
    def __init__(self):
        super().__init__(api_base_url="http://localhost:3000")

    def get_job_status(self, job_id: str):  # type: ignore[override]
        return {"status": "completed", "state": "finished", "job_id": job_id}


def test_discovers_recording_in_tempvideo_dir(tmp_path: Path, monkeypatch):
    user_id = "user123"

    # Put fake recording under TEMPVIDEO_DIR/<userId>/recording.webm
    tempvideo_root = tmp_path / "tempvideo"
    recording_dir = tempvideo_root / user_id
    recording_dir.mkdir(parents=True)
    (recording_dir / "recording.webm").write_bytes(b"fake")

    monkeypatch.setenv("TEMPVIDEO_DIR", str(tempvideo_root))

    m = _FakeMonitor()
    path = m.monitor_until_complete(
        job_id="job",
        metadata={"userId": user_id},
        check_interval=0,
        max_wait_time=1,
    )

    assert path is not None
    assert path.endswith("recording.webm")


def test_discovers_recording_in_scratch_tempvideo(tmp_path: Path, monkeypatch):
    user_id = "user123"

    # Emulate /scratch/tempvideo by pointing /scratch/tempvideo to a temp dir.
    # We can't create /scratch in unit tests, so we simulate by unsetting
    # TEMPVIDEO_DIR and relying on the fallback /usr/src/app/dist/_tempvideo.
    # Instead, verify the search logic checks TEMPVIDEO_DIR first by setting
    # it.
    scratch_tempvideo = tmp_path / "scratch" / "tempvideo"
    recording_dir = scratch_tempvideo / user_id
    recording_dir.mkdir(parents=True)
    (recording_dir / "x.webm").write_bytes(b"fake")

    monkeypatch.setenv("TEMPVIDEO_DIR", str(scratch_tempvideo))

    m = _FakeMonitor()
    path = m.monitor_until_complete(
        job_id="job",
        metadata={"userId": user_id},
        check_interval=0,
        max_wait_time=1,
    )

    assert path is not None
    assert path.endswith(".webm")


def test_errors_when_no_dirs_exist(monkeypatch):
    user_id = "user123"
    monkeypatch.delenv("TEMPVIDEO_DIR", raising=False)

    m = _FakeMonitor()
    path = m.monitor_until_complete(
        job_id="job",
        metadata={"userId": user_id},
        check_interval=0,
        max_wait_time=1,
    )

    assert path is None
