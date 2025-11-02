#!/usr/bin/env python3
"""
Meeting Bot Manager - Job Execution

This manager processes a single meeting recording job:
1. Reads job details from environment variables
2. Initiates meeting join via meeting-bot API
3. Monitors meeting status
4. Converts recordings (MP4 + AAC)
5. Uploads to GCS

Designed to run as a Kubernetes Job, spawned by the controller.
"""

import os
import sys
import time
import logging
from typing import Optional, Dict

from meeting_monitor import MeetingMonitor
from media_converter import MediaConverter
from storage_client import StorageClient

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Reduce noise from some verbose libraries
logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('google.cloud').setLevel(logging.INFO)


class MeetingManager:
    """Main manager for processing a single meeting recording job"""
    
    def __init__(self):
        # Required environment variables for the meeting job
        self.meeting_url = os.environ.get('MEETING_URL')
        self.meeting_id = os.environ.get('MEETING_ID')
        self.gcs_path = os.environ.get('GCS_PATH')
        
        # Optional meeting metadata
        self.metadata = self._load_metadata()
        
        # GCS and API configuration
        self.gcs_bucket = os.environ.get('GCS_BUCKET')
        self.meeting_bot_api = os.environ.get('MEETING_BOT_API_URL', 'http://localhost:3000')
        
        # Validate required environment variables
        self._validate_config()
        
        # Initialize clients
        self.meeting_monitor = MeetingMonitor(self.meeting_bot_api)
        self.media_converter = MediaConverter()
        self.storage_client = StorageClient(self.gcs_bucket)
    
    def _load_metadata(self) -> Dict:
        """Load optional metadata from environment variables"""
        metadata = {}
        
        # Add any additional metadata fields from environment
        if os.environ.get('MEETING_TITLE'):
            metadata['meeting_title'] = os.environ.get('MEETING_TITLE')
        if os.environ.get('MEETING_ORGANIZER'):
            metadata['meeting_organizer'] = os.environ.get('MEETING_ORGANIZER')
        if os.environ.get('MEETING_START_TIME'):
            metadata['meeting_start_time'] = os.environ.get('MEETING_START_TIME')
        
        # Include the core fields
        if self.meeting_id:
            metadata['meeting_id'] = self.meeting_id
        if self.gcs_path:
            metadata['gcs_path'] = self.gcs_path
            
        return metadata
        
    def _validate_config(self):
        """Validate required environment variables"""
        required_vars = {
            'MEETING_URL': self.meeting_url,
            'MEETING_ID': self.meeting_id,
            'GCS_PATH': self.gcs_path,
            'GCS_BUCKET': self.gcs_bucket,
        }
        
        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def process_meeting(self) -> bool:
        """
        Process the meeting recording job
        
        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            logger.info(f"Processing meeting {self.meeting_id}")
            logger.info(f"Meeting URL: {self.meeting_url}")
            logger.info(f"Target GCS path: {self.gcs_path}")
            
            # Step 1: Join the meeting
            logger.info("Step 1: Joining meeting...")
            job_id = self.meeting_monitor.join_meeting(self.meeting_url, self.metadata)
            if not job_id:
                logger.error("Failed to join meeting")
                return False
            
            logger.info(f"Successfully joined meeting with job ID: {job_id}")
            
            # Step 2: Monitor the meeting (check every 10 seconds)
            logger.info("Step 2: Monitoring meeting status...")
            recording_path = self.meeting_monitor.monitor_until_complete(job_id, check_interval=10)
            if not recording_path:
                logger.error("Meeting monitoring failed or no recording generated")
                return False
            
            logger.info(f"Meeting completed. Recording at: {recording_path}")
            
            # Step 3: Convert media files
            logger.info("Step 3: Converting media files...")
            mp4_path, aac_path = self.media_converter.convert(recording_path)
            if not mp4_path or not aac_path:
                logger.error("Media conversion failed")
                return False
            
            logger.info(f"Conversion complete - MP4: {mp4_path}, AAC: {aac_path}")
            
            # Step 4: Upload to GCS
            logger.info("Step 4: Uploading to GCS...")
            mp4_uploaded = self.storage_client.upload_file(mp4_path, f"{self.gcs_path}/video.mp4")
            aac_uploaded = self.storage_client.upload_file(aac_path, f"{self.gcs_path}/audio.aac")
            
            if mp4_uploaded and aac_uploaded:
                logger.info(f"Successfully uploaded files to gs://{self.gcs_bucket}/{self.gcs_path}/")
                
                # Cleanup local files
                self.media_converter.cleanup(recording_path, mp4_path, aac_path)
                
                return True
            else:
                logger.error("Failed to upload one or more files to GCS")
                return False
                
        except Exception as e:
            logger.exception(f"Error processing meeting: {e}")
            return False
    
    def run(self):
        """Main run - process the meeting job"""
        logger.info("=" * 50)
        logger.info("Meeting Bot Manager starting...")
        logger.info("=" * 50)
        logger.info(f"Meeting ID: {self.meeting_id}")
        logger.info(f"Meeting URL: {self.meeting_url}")
        logger.info(f"GCS Bucket: {self.gcs_bucket}")
        logger.info(f"GCS Path: {self.gcs_path}")
        logger.info(f"Meeting Bot API: {self.meeting_bot_api}")
        logger.info("=" * 50)
        
        exit_code = 0
        
        try:
            # Process the meeting
            success = self.process_meeting()
            
            if success:
                logger.info("=" * 50)
                logger.info("Processing completed successfully")
                logger.info("=" * 50)
                exit_code = 0
            else:
                logger.error("=" * 50)
                logger.error("Processing failed")
                logger.error("=" * 50)
                exit_code = 1
        
        finally:
            # ALWAYS trigger shutdown of meeting-bot, regardless of success or failure
            logger.info("Triggering meeting-bot shutdown...")
            shutdown_success = self.meeting_monitor.shutdown()
            if shutdown_success:
                logger.info("Meeting-bot shutdown triggered successfully")
            else:
                logger.warning("Failed to trigger meeting-bot shutdown")
        
        return exit_code


def main():
    """Entry point"""
    manager = None
    exit_code = 1
    
    try:
        manager = MeetingManager()
        exit_code = manager.run()
    except Exception as e:
        logger.exception(f"Fatal error during initialization: {e}")
        exit_code = 1
        
        # Try to shutdown even if initialization failed partway through
        if manager and hasattr(manager, 'meeting_monitor'):
            try:
                logger.info("Attempting meeting-bot shutdown after fatal error...")
                manager.meeting_monitor.shutdown()
            except Exception as shutdown_error:
                logger.error(f"Error during shutdown after fatal error: {shutdown_error}")
    
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
