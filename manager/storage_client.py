"""
Google Cloud Storage Client for uploading processed files
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from google.cloud import storage, firestore

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
            elif gcs_path.endswith(".webm"):
                blob.content_type = "video/webm"
            elif gcs_path.endswith(".aac"):
                blob.content_type = "audio/aac"
            elif gcs_path.endswith(".m4a"):
                blob.content_type = "audio/mp4"
            elif gcs_path.endswith(".wav"):
                blob.content_type = "audio/wav"

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

            # Determine whether we have key-based signing available.
            # Many runtime creds (GCE, Workload Identity, gcloud user ADC) do NOT
            # have a private key, which makes blob.generate_signed_url() fail unless
            # we pass service_account_email + access_token.
            credentials = self.client._credentials

            # Heuristic: key-based credentials typically expose a signer/private key.
            has_private_key = bool(getattr(credentials, "signer", None)) or bool(
                getattr(credentials, "_signer", None)
            )

            # Keep the old compute_engine check, but broaden to token-only creds.
            using_default_credentials = isinstance(
                credentials, compute_engine.Credentials
            )
            using_token_only_credentials = not has_private_key

            if using_default_credentials or using_token_only_credentials:
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

                    service_account_email = getattr(
                        credentials, "service_account_email", None
                    )
                    if not service_account_email:
                        # Some token-only ADC types (e.g., user credentials) do not
                        # expose service_account_email.
                        raise RuntimeError(
                            "Current credentials do not expose service_account_email; "
                            "cannot perform IAM-based signed URL generation."
                        )

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


class FirestoreClient:
    """Client for storing data in Google Cloud Firestore"""

    def __init__(self, database: str = "(default)"):
        """
        Initialize Firestore client

        Args:
            database: Firestore database ID (default: "(default)")
        """
        self.database = database
        self.client = firestore.Client(database=database)

        logger.info(f"Initialized Firestore client for database: {database}")

    def store_transcription(self, meeting_id: str, transcription_text: str) -> bool:
        """
        Store transcription text in Firestore

        Args:
            meeting_id: Meeting ID to use in the document path
            transcription_text: The transcription text to store

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create document reference: organizations/advisewell/meetings/{meeting_id}
            doc_ref = self.client.document(
                f"organizations/advisewell/meetings/{meeting_id}"
            )

            # Update the transcription field
            doc_ref.update({"transcription": transcription_text})

            logger.info(f"Successfully stored transcription for meeting: {meeting_id}")
            return True

        except Exception as e:
            logger.exception(f"Failed to store transcription in Firestore: {e}")
            return False

    def set_transcription(self, meeting_id: str, transcription_text: str) -> bool:
        """
        Set transcription text in Firestore (creates document if it doesn't exist)

        Args:
            meeting_id: Meeting ID to use in the document path
            transcription_text: The transcription text to store

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create document reference: organizations/advisewell/meetings/{meeting_id}
            doc_ref = self.client.document(
                f"organizations/advisewell/meetings/{meeting_id}"
            )

            # Set the transcription field (creates document if it doesn't exist)
            doc_ref.set(
                {"transcription": transcription_text}, merge=True
            )  # merge=True to preserve other fields

            logger.info(f"Successfully stored transcription for meeting: {meeting_id}")
            return True

        except Exception as e:
            logger.exception(f"Failed to store transcription in Firestore: {e}")
            return False

    def create_adhoc_meeting(
        self,
        organization_id: str,
        user_id: str,
        meeting_url: str,
        start_at: str,
    ) -> Optional[str]:
        """
        Create an ad-hoc meeting document in Firestore.

        Args:
            organization_id: Organization ID
            user_id: User ID who initiated the meeting
            meeting_url: Meeting URL (join URL)
            start_at: ISO 8601 timestamp for meeting start time

        Returns:
            Meeting document ID if successful, None otherwise
        """
        try:
            from datetime import datetime, timezone

            # Create new meeting document with auto-generated ID
            meetings_collection = self.client.collection(
                f"organizations/{organization_id}/meetings"
            )

            now = datetime.now(timezone.utc)

            # Detect platform from URL
            platform = "unknown"
            url_lower = meeting_url.lower()
            if "meet.google.com" in url_lower:
                platform = "google_meet"
            elif "teams.microsoft.com" in url_lower:
                platform = "microsoft_teams"
            elif "zoom.us" in url_lower or "zoom.com" in url_lower:
                platform = "zoom"

            # Parse start_at to datetime for the 'start' field
            # Handle both Z and +00:00 formats
            start_datetime = datetime.fromisoformat(start_at.replace("Z", "+00:00"))

            meeting_data = {
                "title": "Ad-hoc meeting",
                "join_url": meeting_url,  # Use join_url not meeting_url
                "organization_id": organization_id,
                "synced_by_user_id": user_id,  # Match existing schema
                "start": start_datetime,  # datetime object for queries
                "created_at": now,
                "updated_at": now,
                "status": "scheduled",  # Match existing ad-hocs
                "source": "ad_hoc",  # Use source not type
                "platform": platform,
                "auto_joined": False,  # Will be updated by controller
            }

            # Create document with auto-generated ID
            doc_ref = meetings_collection.document()
            doc_ref.set(meeting_data)

            meeting_id = doc_ref.id
            logger.info(
                f"Created ad-hoc meeting {meeting_id} in org {organization_id}"
                f" with start={start_at}, platform={platform}"
            )
            return meeting_id

        except Exception as e:
            logger.exception(f"Failed to create ad-hoc meeting in Firestore: {e}")
            return None

    def meeting_exists(self, organization_id: str, meeting_id: str) -> bool:
        """
        Check if a meeting document exists in Firestore.

        Args:
            organization_id: Organization ID
            meeting_id: Meeting document ID

        Returns:
            True if meeting exists, False otherwise
        """
        try:
            doc_ref = self.client.document(
                f"organizations/{organization_id}/meetings/{meeting_id}"
            )
            doc_snap = doc_ref.get()
            return doc_snap.exists

        except Exception as e:
            logger.warning(f"Error checking if meeting exists: {e}")
            return False

    def get_meeting(
        self, organization_id: str, meeting_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get a meeting document from Firestore.

        Args:
            organization_id: Organization ID
            meeting_id: Meeting document ID

        Returns:
            Meeting data dict if exists, None otherwise
        """
        try:
            doc_ref = self.client.document(
                f"organizations/{organization_id}/meetings/{meeting_id}"
            )
            doc_snap = doc_ref.get()

            if doc_snap.exists:
                return doc_snap.to_dict()
            return None

        except Exception as e:
            logger.warning(f"Error getting meeting document: {e}")
            return None

    def update_adhoc_meeting_times(
        self,
        organization_id: str,
        meeting_id: str,
        start_time: datetime,
        end_time: datetime,
        duration_seconds: float,
    ) -> bool:
        """
        Update an ad-hoc meeting document with start/end times.

        This is needed when the frontend creates an ad-hoc meeting but
        doesn't set the 'start' field, which is required for UI display.

        Args:
            organization_id: Organization ID
            meeting_id: Meeting document ID
            start_time: Meeting start datetime
            end_time: Meeting end datetime
            duration_seconds: Meeting duration in seconds

        Returns:
            True if update successful, False otherwise
        """
        try:
            from datetime import timezone

            doc_ref = self.client.document(
                f"organizations/{organization_id}/meetings/{meeting_id}"
            )

            now = datetime.now(timezone.utc)

            update_data = {
                "start": start_time,
                "end": end_time,
                "duration_seconds": duration_seconds,
                "updated_at": now,
                # Mark as completed since recording is done
                "status": "completed",
            }

            doc_ref.update(update_data)

            logger.info(
                f"Updated ad-hoc meeting {meeting_id} "
                f"with start={start_time}, end={end_time}, "
                f"duration={duration_seconds}s"
            )
            return True

        except Exception as e:
            logger.exception(f"Failed to update ad-hoc meeting times: {e}")
            return False
