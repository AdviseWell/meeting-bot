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
        self, local_path: str, gcs_path: str, content_type: Optional[str] = None
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
            gcs_path = gcs_path.lstrip("/")

            logger.info(f"Uploading {local_path} to gs://{self.bucket_name}/{gcs_path}")

            # Create blob and upload
            blob = self.bucket.blob(gcs_path)

            # Auto-detect content type if not provided
            if content_type:
                blob.content_type = content_type
            elif gcs_path.endswith(".mp4"):
                blob.content_type = "video/mp4"
            elif gcs_path.endswith(".aac"):
                blob.content_type = "audio/aac"

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
            gcs_path = gcs_path.lstrip("/")
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
            gcs_path = gcs_path.lstrip("/")
            blob = self.bucket.blob(gcs_path)
            blob.delete()
            logger.info(f"Deleted gs://{self.bucket_name}/{gcs_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file from GCS: {e}")
            return False

    def get_signed_url(
        self, gcs_path: str, expiration_minutes: int = 60
    ) -> Optional[str]:
        """
        Get a signed URL for a file in GCS

        Uses IAM-based signing when service account keys are not available.
        This keeps files private while allowing authenticated access.

        Args:
            gcs_path: Path in GCS
            expiration_minutes: URL expiration time in minutes (default: 60)

        Returns:
            Signed URL string, or None if failed
        """
        try:
            from datetime import timedelta
            from google.auth import compute_engine
            from google.auth.transport import requests as auth_requests

            gcs_path = gcs_path.lstrip("/")
            blob = self.bucket.blob(gcs_path)

            # Check if we're using default credentials without a private key
            credentials = self.client._credentials
            using_default_credentials = isinstance(
                credentials, compute_engine.Credentials
            )

            if using_default_credentials:
                # Use IAM-based signing instead of key-based signing
                # This keeps files private but generates signed URLs
                logger.info(
                    "Using IAM-based signing for signed URL "
                    "(no service account key required)"
                )

                try:
                    # Get the default service account email
                    auth_req = auth_requests.Request()
                    credentials.refresh(auth_req)
                    service_account_email = credentials.service_account_email

                    logger.info(f"Service account: {service_account_email}")

                    # Generate signed URL using IAM signing
                    url = blob.generate_signed_url(
                        version="v4",
                        expiration=timedelta(minutes=expiration_minutes),
                        method="GET",
                        service_account_email=service_account_email,
                        access_token=credentials.token,
                    )

                    logger.info(
                        f"✅ Generated IAM-based signed URL for "
                        f"gs://{self.bucket_name}/{gcs_path}"
                    )
                except Exception as iam_error:
                    logger.error(
                        f"IAM-based signing failed: {iam_error}", exc_info=True
                    )
                    logger.warning(
                        "This likely means the service account lacks "
                        "'iam.serviceAccountTokenCreator' role. "
                        "Falling back to public URL approach..."
                    )

                    # Fallback: Make blob temporarily public
                    blob.make_public()
                    url = blob.public_url

                    logger.warning(
                        f"⚠️  Made blob PUBLIC (temporary): "
                        f"gs://{self.bucket_name}/{gcs_path}"
                    )
                    logger.warning(
                        "Remember to revoke public access after transcription!"
                    )
            else:
                # We have a service account key, use standard signed URL
                url = blob.generate_signed_url(
                    version="v4",
                    expiration=timedelta(minutes=expiration_minutes),
                    method="GET",
                )
                logger.info(
                    f"Generated key-based signed URL for "
                    f"gs://{self.bucket_name}/{gcs_path}"
                )

            return url

        except Exception as e:
            logger.error(f"Failed to generate signed URL: {e}", exc_info=True)
            return None

    def revoke_public_access(self, gcs_path: str) -> bool:
        """
        Revoke public access from a blob

        Args:
            gcs_path: Path in GCS

        Returns:
            True if successful, False otherwise
        """
        try:
            gcs_path = gcs_path.lstrip("/")
            blob = self.bucket.blob(gcs_path)

            # Remove public access
            blob.acl.all().revoke_read()
            blob.acl.save()

            logger.info(
                f"Revoked public access for " f"gs://{self.bucket_name}/{gcs_path}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to revoke public access: {e}")
            return False
