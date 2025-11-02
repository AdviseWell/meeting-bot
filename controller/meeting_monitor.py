"""
Meeting Monitor - Interface with the meeting-bot API
"""

import time
import logging
import requests
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class MeetingMonitor:
    """Monitor and control meetings via the meeting-bot API"""
    
    def __init__(self, api_base_url: str):
        """
        Initialize meeting monitor
        
        Args:
            api_base_url: Base URL of the meeting-bot API
        """
        self.api_base_url = api_base_url.rstrip('/')
        logger.info(f"Initialized MeetingMonitor with API: {self.api_base_url}")
    
    def join_meeting(self, meeting_url: str, metadata: Dict) -> Optional[str]:
        """
        Join a meeting via the meeting-bot API
        
        Args:
            meeting_url: URL of the meeting to join
            metadata: Additional metadata to pass to the API
            
        Returns:
            Job ID if successful, None otherwise
        """
        try:
            endpoint = f"{self.api_base_url}/api/join"
            
            payload = {
                "meeting_url": meeting_url,
                "metadata": metadata
            }
            
            logger.info(f"Joining meeting: {meeting_url}")
            response = requests.post(
                endpoint,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            
            result = response.json()
            job_id = result.get('job_id') or result.get('id')
            
            if not job_id:
                logger.error(f"No job_id in response: {result}")
                return None
            
            logger.info(f"Successfully joined meeting. Job ID: {job_id}")
            return job_id
            
        except requests.exceptions.RequestException as e:
            logger.exception(f"Failed to join meeting: {e}")
            return None
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """
        Get the status of a meeting job
        
        Args:
            job_id: The job ID to check
            
        Returns:
            Job status dict if successful, None otherwise
        """
        try:
            endpoint = f"{self.api_base_url}/api/job/{job_id}"
            
            response = requests.get(endpoint, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get job status: {e}")
            return None
    
    def monitor_until_complete(
        self,
        job_id: str,
        check_interval: int = 10,
        max_wait_time: int = 28800  # 8 hours default
    ) -> Optional[str]:
        """
        Monitor a meeting job until it completes
        
        Args:
            job_id: The job ID to monitor
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
            
            state = status.get('status') or status.get('state')
            logger.info(f"Job {job_id} status: {state}")
            
            # Check if job is complete
            if state in ['completed', 'finished', 'done']:
                recording_path = status.get('recording_path') or status.get('output_path')
                if recording_path:
                    logger.info(f"Meeting completed successfully. Recording: {recording_path}")
                    return recording_path
                else:
                    logger.warning("Meeting completed but no recording path found")
                    return None
            
            # Check if job failed
            if state in ['failed', 'error', 'cancelled']:
                logger.error(f"Job {job_id} failed with state: {state}")
                error_msg = status.get('error') or status.get('error_message')
                if error_msg:
                    logger.error(f"Error details: {error_msg}")
                return None
            
            # Job still in progress, wait before next check
            logger.debug(f"Job still in progress ({state}), waiting {check_interval}s...")
            time.sleep(check_interval)
