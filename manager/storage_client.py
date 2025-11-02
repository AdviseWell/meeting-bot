"""
Google Cloud Storage Client for uploading processed files
"""

import logging
import os
from typing import Optional
from google.cloud import storage

logger = logging.getLogger(__name__)


class StorageClient:
    """Client for uploading files to Google Cloud Storage"""
    
    def __init__(self, bucket_name: str):
        """
        Initialize GCS client
        
        Args:
            bucket_name: Name of the GCS bucket
        """
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        
        logger.info(f"Initialized GCS client for bucket: {bucket_name}")
    
    def upload_file(
        self,
        local_path: str,
        gcs_path: str,
        content_type: Optional[str] = None
    ) -> bool:
        """
        Upload a file to GCS
        
        Args:
            local_path: Local file path to upload
            gcs_path: Destination path in GCS (without gs://bucket/ prefix)
            content_type: Optional content type (auto-detected if not provided)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(local_path):
                logger.error(f"Local file not found: {local_path}")
                return False
            
            # Remove leading slash if present
            gcs_path = gcs_path.lstrip('/')
            
            logger.info(f"Uploading {local_path} to gs://{self.bucket_name}/{gcs_path}")
            
            # Create blob and upload
            blob = self.bucket.blob(gcs_path)
            
            # Auto-detect content type if not provided
            if content_type:
                blob.content_type = content_type
            elif gcs_path.endswith('.mp4'):
                blob.content_type = 'video/mp4'
            elif gcs_path.endswith('.aac'):
                blob.content_type = 'audio/aac'
            
            # Upload the file
            blob.upload_from_filename(local_path)
            
            logger.info(f"Successfully uploaded to gs://{self.bucket_name}/{gcs_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to upload file to GCS: {e}")
            return False
    
    def file_exists(self, gcs_path: str) -> bool:
        """
        Check if a file exists in GCS
        
        Args:
            gcs_path: Path in GCS to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            gcs_path = gcs_path.lstrip('/')
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking if file exists: {e}")
            return False
    
    def delete_file(self, gcs_path: str) -> bool:
        """
        Delete a file from GCS
        
        Args:
            gcs_path: Path in GCS to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            gcs_path = gcs_path.lstrip('/')
            blob = self.bucket.blob(gcs_path)
            blob.delete()
            logger.info(f"Deleted gs://{self.bucket_name}/{gcs_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file from GCS: {e}")
            return False
