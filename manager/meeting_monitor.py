"""
Meeting Monitor - Interface with the meeting-bot API
"""

import os
import glob
import shutil
import tempfile
import time
import logging
import requests
from typing import Optional, Dict, Literal

from join_payload import build_join_payload

logger = logging.getLogger(__name__)

# Type alias for meeting providers
Provider = Literal["google", "microsoft", "zoom"]


def detect_meeting_provider(meeting_url: str) -> Optional[Provider]:
    """
    Detect the meeting provider from the URL

    Args:
        meeting_url: The meeting URL

    Returns:
        Provider name ('google', 'microsoft', or 'zoom'), or None if unknown
    """
    url_lower = meeting_url.lower()

    if "meet.google.com" in url_lower or "google.com/meet" in url_lower:
        return "google"
    elif "teams.microsoft.com" in url_lower or "teams.live.com" in url_lower:
        return "microsoft"
    elif "zoom.us" in url_lower or "zoom.com" in url_lower:
        return "zoom"
    else:
        logger.warning(f"Unknown meeting provider for URL: {meeting_url}")
        return None


class MeetingMonitor:
    """Monitor and control meetings via the meeting-bot API"""

    def __init__(self, api_base_url: str, startup_timeout: int = 60):
        """
        Initialize meeting monitor

        Args:
            api_base_url: Base URL of the meeting-bot API
            startup_timeout: Maximum seconds to wait for API to become ready
        """
        self.api_base_url = api_base_url.rstrip("/")
        self.startup_timeout = startup_timeout
        logger.info(f"Initialized MeetingMonitor with API: {self.api_base_url}")

    def wait_for_api_ready(self) -> bool:
        """
        Wait for the meeting-bot API to become ready

        Returns:
            True if API is ready, False if timeout exceeded
        """
        logger.info(
            f"Waiting for meeting-bot API to become ready (timeout: {self.startup_timeout}s)..."
        )

        start_time = time.time()
        retry_count = 0

        while time.time() - start_time < self.startup_timeout:
            try:
                # Try health check endpoint first
                endpoint = f"{self.api_base_url}/health"
                response = requests.get(endpoint, timeout=2)

                if response.status_code == 200:
                    logger.info(
                        f"✅ Meeting-bot API is ready (took {time.time() - start_time:.1f}s)"
                    )
                    return True

            except requests.exceptions.ConnectionError:
                # Connection refused - service not ready yet
                pass
            except requests.exceptions.RequestException as e:
                logger.debug(f"Health check failed: {e}")

            retry_count += 1
            wait_time = min(
                2 ** min(retry_count - 1, 4), 5
            )  # Exponential backoff, max 5s

            if retry_count % 5 == 0:  # Log every 5 attempts
                elapsed = time.time() - start_time
                logger.info(f"Still waiting for API... ({elapsed:.1f}s elapsed)")

            time.sleep(wait_time)

        logger.error(
            f"❌ Meeting-bot API did not become ready within {self.startup_timeout}s"
        )
        return False

    def join_meeting(self, meeting_url: str, metadata: Dict) -> Optional[str]:
        """
        Join a meeting via the meeting-bot API

        Args:
            meeting_url: URL of the meeting to join
            metadata: Additional metadata to pass to the API (must include required fields)

        Returns:
            Job ID if successful, None otherwise
        """
        try:
            # Detect provider from URL
            provider = detect_meeting_provider(meeting_url)
            if not provider:
                logger.error(
                    f"Could not detect meeting provider from URL: {meeting_url}"
                )
                return None

            # Build provider-specific endpoint
            endpoint = f"{self.api_base_url}/{provider}/join"

            # Build payload with required fields.
            # Some meeting-bot deployments validate presence of bearerToken and
            # userId even when they can be auto-generated. To stay compatible,
            # always include keys and allow empty/None values.
            payload = build_join_payload(
                meeting_url=meeting_url,
                metadata=metadata,
            )

            logger.info(f"Joining {provider} meeting: {meeting_url}")
            masked_token = (
                "***"
                if payload.get("bearerToken")
                and payload.get("bearerToken") != "AUTO-GENERATED"
                else "AUTO-GENERATED"
            )
            logger.info(
                "Payload: name=%s, teamId=%s, timezone=%s, userId=%s, "
                "botId=%s, bearerToken=%s",
                payload.get("name"),
                payload.get("teamId"),
                payload.get("timezone"),
                payload.get("userId"),
                payload.get("botId"),
                masked_token,
            )

            try:
                response = requests.post(endpoint, json=payload, timeout=30)

                # Log response details for debugging
                logger.info(f"API Response Status: {response.status_code}")
                logger.debug(f"API Response Headers: {dict(response.headers)}")

                response.raise_for_status()

            except requests.exceptions.HTTPError as http_err:
                # Log detailed error information
                logger.error(f"HTTP error occurred: {http_err}")
                logger.error(f"Response status: {response.status_code}")
                logger.error(f"Response body: {response.text}")
                raise
            except requests.exceptions.Timeout as timeout_err:
                logger.error(f"Request timeout after 30s: {timeout_err}")
                raise
            except requests.exceptions.ConnectionError as conn_err:
                logger.error(f"Connection error: {conn_err}")
                raise
            except Exception as req_err:
                logger.error(f"Request failed: {req_err}")
                raise

            result = response.json()

            # Check if job was accepted
            if not result.get("success"):
                error_msg = result.get("error", "Unknown error")
                logger.error(f"Meeting join request failed: {error_msg}")
                return None

            # Extract job/bot ID from response
            data = result.get("data", {})
            job_id = (
                data.get("botId")
                or data.get("eventId")
                or payload.get("botId")
                or payload.get("eventId")
            )

            if not job_id:
                logger.error(f"No job ID in response: {result}")
                return None

            logger.info(f"Successfully joined {provider} meeting. Job ID: {job_id}")
            return job_id

        except requests.exceptions.RequestException as e:
            logger.exception(f"Failed to join meeting: {e}")
            return None

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """
        Get the status of a meeting job by checking if the bot is busy

        Note: Meeting-bot doesn't have a /api/job/{id} endpoint. Instead, it uses
        a single-job execution model where we check the /isbusy endpoint.

        Args:
            job_id: The job ID to check (not actually used by meeting-bot)

        Returns:
            Job status dict if successful, None otherwise
        """
        try:
            # Check if meeting-bot is currently busy processing
            endpoint = f"{self.api_base_url}/isbusy"

            response = requests.get(endpoint, timeout=10)
            response.raise_for_status()

            result = response.json()
            is_busy = result.get("data", 0)

            # Map busy status to job state
            # When busy=1, meeting is in progress
            # When busy=0, meeting is complete or bot is idle
            if is_busy == 1:
                return {
                    "status": "processing",
                    "state": "in_progress",
                    "job_id": job_id,
                }
            else:
                # Bot is no longer busy - meeting either completed or failed
                # We can't distinguish without additional state tracking
                return {
                    "status": "completed",
                    "state": "finished",
                    "job_id": job_id,
                    "note": "Meeting-bot is no longer busy. Check logs for recording location.",
                }

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get job status: {e}")
            return None

    def monitor_until_complete(
        self,
        job_id: str,
        metadata: Dict,
        check_interval: int = 10,
        max_wait_time: int = 14400,  # 4 hours default
    ) -> Optional[str]:
        """
        Monitor a meeting job until it completes

        Args:
            job_id: The job ID to monitor
            metadata: Meeting metadata containing userId and other info
            check_interval: How often to check status (seconds)
            max_wait_time: Maximum time to wait (seconds)

        Returns:
            Path to the recording if successful, None otherwise
        """
        logger.info(f"Monitoring job {job_id} (checking every {check_interval}s)")

        start_time = time.time()

        while True:
            # Check if we've exceeded max wait time
            elapsed = time.time() - start_time
            if elapsed > max_wait_time:
                logger.error(f"Exceeded max wait time of {max_wait_time}s")
                return None

            # Get job status
            status = self.get_job_status(job_id)

            if not status:
                logger.warning(f"Failed to get status for job {job_id}, will retry...")
                time.sleep(check_interval)
                continue

            state = status.get("status") or status.get("state")
            logger.info(f"Job {job_id} status: {state}")

            # Check if job is complete
            if state in ["completed", "finished", "done"]:
                # Recording file is in the shared volume
                # Meeting-bot saves to: /usr/src/app/dist/_tempvideo/{userId}/recording.webm
                # This is mounted to /recordings in the manager container (read-only access)
                user_id = metadata.get("userId") or metadata.get("user_id")
                if not user_id:
                    logger.error(
                        "❌ No userId found in metadata - cannot locate recording"
                    )
                    logger.debug(f"Available metadata keys: {list(metadata.keys())}")
                    return None

                logger.info(f"Looking for recording in directory for userId: {user_id}")

                # Look for the recording file in the shared volume
                recording_dir = f"/recordings/{user_id}"
                if not os.path.exists(recording_dir):
                    logger.error(f"❌ Recording directory not found: {recording_dir}")
                    return None

                # Find .webm files in the directory
                recording_files = glob.glob(f"{recording_dir}/*.webm")

                if not recording_files:
                    logger.error(
                        f"❌ No .webm recording files found in {recording_dir}"
                    )
                    return None

                if len(recording_files) > 1:
                    logger.warning(
                        f"⚠️  Multiple recording files found, using first: {recording_files}"
                    )

                source_path = recording_files[0]
                logger.info(f"📹 Found recording file: {source_path}")

                # Verify file exists and is readable
                if not os.path.isfile(source_path):
                    logger.error(f"❌ Recording path is not a file: {source_path}")
                    return None

                file_size = os.path.getsize(source_path)
                logger.info(f"📊 Recording file size: {file_size / (1024*1024):.2f} MB")

                # Copy to /tmp for processing (shared volume is read-only for manager)
                temp_dir = tempfile.mkdtemp(prefix="recording_")
                filename = os.path.basename(source_path)
                temp_path = os.path.join(temp_dir, filename)

                logger.info(f"📋 Copying recording to temp directory: {temp_path}")
                shutil.copy2(source_path, temp_path)
                logger.info(f"✅ Recording copied successfully")

                # Also copy WAV backup file if it exists
                wav_files = glob.glob(f"{recording_dir}/backup_*.wav")
                if wav_files:
                    wav_source = wav_files[0]
                    wav_filename = os.path.basename(wav_source)
                    wav_temp_path = os.path.join(temp_dir, wav_filename)

                    logger.info(
                        f"📋 Copying WAV backup to temp directory: {wav_temp_path}"
                    )
                    shutil.copy2(wav_source, wav_temp_path)
                    logger.info(
                        f"✅ WAV backup copied successfully ({os.path.getsize(wav_source) / (1024*1024):.2f} MB)"
                    )
                else:
                    logger.debug(f"No WAV backup file found in {recording_dir}")

                return temp_path

            # Check if job failed
            if state in ["failed", "error", "cancelled"]:
                logger.error(f"Job {job_id} failed with state: {state}")
                error_msg = status.get("error") or status.get("error_message")
                if error_msg:
                    logger.error(f"Error details: {error_msg}")
                return None

            # Job still in progress, wait before next check
            logger.debug(
                f"Job still in progress ({state}), waiting {check_interval}s..."
            )
            time.sleep(check_interval)

    def shutdown(self) -> bool:
        """
        Trigger graceful shutdown of the meeting-bot service

        Returns:
            True if shutdown request was successful, False otherwise
        """
        try:
            endpoint = f"{self.api_base_url}/shutdown"

            logger.info("Triggering meeting-bot shutdown...")
            response = requests.post(endpoint, timeout=10)

            response.raise_for_status()
            logger.info("Shutdown request sent successfully")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to trigger shutdown: {e}")
            return False
