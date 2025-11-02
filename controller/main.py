#!/usr/bin/env python3
"""
Meeting Bot Controller - Main Entry Point

This controller manages the lifecycle of meeting recordings:
1. Pulls messages from GCP Pub/Sub
2. Initiates meeting join via meeting-bot API
3. Monitors meeting status
4. Converts recordings (MP4 + AAC)
5. Uploads to GCS
"""

import os
import sys
import time
import logging
from typing import Optional

from pubsub_client import PubSubClient
from meeting_monitor import MeetingMonitor
from media_converter import MediaConverter
from storage_client import StorageClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class MeetingController:
    """Main controller for managing meeting recording lifecycle"""
    
    def __init__(self):
        self.project_id = os.environ.get('GCP_PROJECT_ID')
        self.subscription_name = os.environ.get('PUBSUB_SUBSCRIPTION')
        self.gcs_bucket = os.environ.get('GCS_BUCKET')
        self.meeting_bot_api = os.environ.get('MEETING_BOT_API_URL', 'http://localhost:3000')
        
        # Validate required environment variables
        self._validate_config()
        
        # Initialize clients
        self.pubsub_client = PubSubClient(self.project_id, self.subscription_name)
        self.meeting_monitor = MeetingMonitor(self.meeting_bot_api)
        self.media_converter = MediaConverter()
        self.storage_client = StorageClient(self.gcs_bucket)
        
    def _validate_config(self):
        """Validate required environment variables"""
        required_vars = {
            'GCP_PROJECT_ID': self.project_id,
            'PUBSUB_SUBSCRIPTION': self.subscription_name,
            'GCS_BUCKET': self.gcs_bucket,
        }
        
        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def process_message(self, message_data: dict) -> bool:
        """
        Process a single meeting message
        
        Args:
            message_data: Decoded message data containing meeting details
            
        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            meeting_url = message_data.get('meeting_url')
            gcs_path = message_data.get('gcs_path')
            meeting_id = message_data.get('meeting_id')
            
            if not all([meeting_url, gcs_path, meeting_id]):
                logger.error(f"Invalid message data: {message_data}")
                return False
            
            logger.info(f"Processing meeting {meeting_id}")
            logger.info(f"Meeting URL: {meeting_url}")
            logger.info(f"Target GCS path: {gcs_path}")
            
            # Step 1: Join the meeting
            logger.info("Step 1: Joining meeting...")
            job_id = self.meeting_monitor.join_meeting(meeting_url, message_data)
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
            mp4_uploaded = self.storage_client.upload_file(mp4_path, f"{gcs_path}/video.mp4")
            aac_uploaded = self.storage_client.upload_file(aac_path, f"{gcs_path}/audio.aac")
            
            if mp4_uploaded and aac_uploaded:
                logger.info(f"Successfully uploaded files to gs://{self.gcs_bucket}/{gcs_path}/")
                
                # Cleanup local files
                self.media_converter.cleanup(recording_path, mp4_path, aac_path)
                
                # Trigger shutdown of meeting-bot container
                logger.info("Triggering meeting-bot shutdown...")
                shutdown_success = self.meeting_monitor.shutdown()
                if shutdown_success:
                    logger.info("Meeting-bot shutdown triggered successfully")
                else:
                    logger.warning("Failed to trigger meeting-bot shutdown, but processing completed successfully")
                
                return True
            else:
                logger.error("Failed to upload one or more files to GCS")
                return False
                
        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            return False
    
    def run(self):
        """Main run loop - pull and process one message"""
        logger.info("=" * 50)
        logger.info("Meeting Bot Controller starting...")
        logger.info("=" * 50)
        logger.info(f"Project ID: {self.project_id}")
        logger.info(f"Subscription: {self.subscription_name}")
        logger.info(f"GCS Bucket: {self.gcs_bucket}")
        logger.info(f"Meeting Bot API: {self.meeting_bot_api}")
        logger.info("=" * 50)
        
        # Pull one message from Pub/Sub
        message_data = self.pubsub_client.pull_one_message()
        
        if not message_data:
            logger.info("No messages available. Exiting successfully.")
            return 0
        
        # Process the message
        success = self.process_message(message_data)
        
        if success:
            logger.info("=" * 50)
            logger.info("Processing completed successfully")
            logger.info("=" * 50)
            return 0
        else:
            logger.error("=" * 50)
            logger.error("Processing failed")
            logger.error("=" * 50)
            return 1


def main():
    """Entry point"""
    try:
        controller = MeetingController()
        exit_code = controller.run()
        sys.exit(exit_code)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
