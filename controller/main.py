#!/usr/bin/env python3
"""controller.main  # noqa: E501

Meeting Bot Controller - Kubernetes Job Orchestrator

This controller polls Firestore for queued meeting/bot-instance work and
spawns Kubernetes Jobs to process each meeting via the manager.

Why Firestore polling?
- Removes Pub/Sub + Firebase Functions infrastructure.
- Uses existing Firestore state as the source of truth.

Workflow:
1. Query Firestore for queued bot instances
2. Atomically claim a bot instance (best-effort distributed lock)
3. Build a job payload compatible with the existing manager env contract
4. Create a Kubernetes Job for the claimed item
5. Repeat
"""

# NOTE: This module is operational and contains long env var / YAML-ish lines.
# flake8: noqa: E501

import os
import sys
import time
import socket
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from google.cloud import firestore, pubsub_v1
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import json

# Configure logging
log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
log_level = getattr(logging, log_level_name, logging.DEBUG)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

# Reduce noise from some verbose libraries (unless DEBUG is explicitly set)
if log_level > logging.DEBUG:
    logging.getLogger("google.auth").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google.cloud").setLevel(logging.INFO)
    logging.getLogger("kubernetes").setLevel(logging.INFO)
    logging.getLogger("google.cloud.pubsub_v1").setLevel(logging.WARNING)


class HealthCheckServer:
    """Simple HTTP server for health checks"""

    def __init__(self, port: int = 8080):
        from http.server import HTTPServer, BaseHTTPRequestHandler

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/health":
                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"OK")
                elif self.path == "/ready":
                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"READY")
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                pass  # Suppress default logging

        self.server = HTTPServer(("0.0.0.0", port), HealthHandler)

    def start(self):
        import threading

        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        logger.info("Health check server started on port 8080")


class MeetingController:
    """Controller that creates Kubernetes Jobs for meeting processing"""

    def __init__(self):
        # Required environment variables
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.gcs_bucket = os.getenv("GCS_BUCKET")
        self.manager_image = os.getenv("MANAGER_IMAGE")
        self.meeting_bot_image = os.getenv("MEETING_BOT_IMAGE")

        # Firestore configuration
        # NOTE: We keep the GCP project id as the canonical project identifier.
        self.firestore_database = os.getenv("FIRESTORE_DATABASE", "(default)")
        # When claim TTL expires, another controller instance may re-claim.
        self.claim_ttl_seconds = int(os.getenv("CLAIM_TTL_SECONDS", "600"))
        self.max_claim_per_poll = int(os.getenv("MAX_CLAIM_PER_POLL", "10"))

        # Firestore query behavior
        # Query for bot instances in queued state.
        self.bot_instance_status_field = os.getenv(
            "BOT_INSTANCE_STATUS_FIELD", "status"
        )
        self.bot_instance_queued_value = os.getenv(
            "BOT_INSTANCE_QUEUED_VALUE", "queued"
        )

        # Leader election configuration
        self.instance_id = socket.gethostname()
        self.leader_collection_path = "system"
        self.leader_doc_id = "controller_leader"
        self.leader_lease_seconds = 30
        self.is_leader = False

        # Meeting discovery / creation behavior
        # The controller is the source of truth for creating bot_instances.
        # It discovers meetings that need a bot and creates a bot_instances doc
        # in queued state.
        self.meetings_collection_path = os.getenv(
            "MEETINGS_COLLECTION_PATH",
            # Default to a flat collection for simplicity.
            # If your schema is per-org, set MEETINGS_COLLECTION_PATH to
            # organizations/<org_id>/meetings and also set MEETINGS_QUERY_MODE.
            "meetings",
        )
        self.meetings_query_mode = (
            os.getenv(
                "MEETINGS_QUERY_MODE",
                # 'collection' -> use MEETINGS_COLLECTION_PATH as a collection
                # 'collection_group' -> treat MEETINGS_COLLECTION_PATH as a collection id
                #                     and query across all parents.
                "collection",
            )
            .strip()
            .lower()
        )
        self.meeting_status_field = os.getenv("MEETING_STATUS_FIELD", "status")
        self.meeting_status_values = [
            s.strip()
            for s in os.getenv("MEETING_STATUS_VALUES", "scheduled").split(",")
            if s.strip()
        ]

        # Only create a bot instance when meeting doesn't already have one.
        self.meeting_bot_instance_field = os.getenv(
            "MEETING_BOT_INSTANCE_FIELD", "bot_instance_id"
        )

        # Kubernetes configuration
        self.k8s_namespace = os.getenv("KUBERNETES_NAMESPACE", "default")
        self.job_service_account = os.getenv("JOB_SERVICE_ACCOUNT", "meeting-bot-job")

        # Optional configuration
        self.node_env = os.getenv("NODE_ENV", "development")
        self.max_recording_duration = int(
            os.getenv("MAX_RECORDING_DURATION_MINUTES", "600")
        )
        self.meeting_inactivity = int(os.getenv("MEETING_INACTIVITY_MINUTES", "15"))
        self.inactivity_detection_delay = int(
            os.getenv("INACTIVITY_DETECTION_START_DELAY_MINUTES", "5")
        )

        # How often the controller checks Firestore for new meetings/bot work.
        # Kept configurable; default matches prior behavior.
        self.poll_interval = int(os.getenv("POLL_INTERVAL", "10"))

        # Pub/Sub configuration
        self.pubsub_subscription = os.getenv("PUBSUB_SUBSCRIPTION")
        self.subscriber = None
        self.streaming_pull_future = None

        # Validate required environment variables
        self._validate_config()

        # Initialize Firestore client
        self.db = firestore.Client(
            project=self.project_id, database=self.firestore_database
        )

        # Initialize Kubernetes client
        try:
            # Try to load in-cluster config first
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            # Fall back to kubeconfig for local development
            config.load_kube_config()
            logger.info("Loaded kubeconfig configuration")

        self.batch_v1 = client.BatchV1Api()
        self.core_v1 = client.CoreV1Api()

        logger.info("Controller initialized:")
        logger.info(f"  Project: {self.project_id}")
        logger.info(f"  Firestore DB: {self.firestore_database}")
        logger.info(f"  Namespace: {self.k8s_namespace}")
        logger.info(f"  Manager Image: {self.manager_image}")
        logger.info(f"  Meeting Bot Image: {self.meeting_bot_image}")
        logger.info(
            "  Meeting discovery: mode=%s path=%s",
            self.meetings_query_mode,
            self.meetings_collection_path,
        )

    def _validate_config(self):
        """Validate required environment variables"""
        required_vars = {
            "GCP_PROJECT_ID": self.project_id,
            "MANAGER_IMAGE": self.manager_image,
            "MEETING_BOT_IMAGE": self.meeting_bot_image,
            "GCS_BUCKET": self.gcs_bucket,
        }

        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    def create_manager_job(self, message_data: Dict[str, Any], message_id: str) -> bool:
        """
        Create a Kubernetes Job to process the meeting

        Args:
            message_data: Message data containing meeting details
            message_id: Pub/Sub message ID for unique job naming

        Returns:
            True if job created successfully, False otherwise
        """
        try:
            meeting_id = message_data.get("meeting_id", message_id)
            meeting_url = message_data.get("meeting_url")

            # Storage layout is always:
            #   recordings/<user_firebase_document_id>/<meeting_firebase_document_id>/<files>
            # The manager container will append fixed filenames.
            meeting_doc_id = (
                message_data.get("fs_meeting_id")
                or message_data.get("FS_MEETING_ID")
                or message_data.get("meeting_firebase_document_id")
                or message_data.get("meeting_doc_id")
                or meeting_id
            )

            user_doc_id = (
                message_data.get("user_id")
                or message_data.get("USER_ID")
                or message_data.get("fs_user_id")
                or message_data.get("FS_USER_ID")
                or message_data.get("creator_user_id")
                or message_data.get("user_firebase_document_id")
                or message_data.get("user_doc_id")
                or ""
            )

            gcs_path = (
                f"recordings/{user_doc_id}/{meeting_doc_id}"
                if user_doc_id
                else f"recordings/{meeting_doc_id}"
            )

            if not meeting_url:
                logger.error(
                    f"Invalid message data - missing meeting_url: {message_data}"
                )
                return False

            # Generate unique job name (must be DNS-1123 compliant)
            # K8s names must be lowercase alphanumeric + hyphens
            timestamp = int(time.time())
            job_name = f"meeting-{meeting_id.lower()[:50]}-{timestamp}"
            job_name = job_name.replace("_", "-")[:63]  # K8s name length limit

            logger.info(f"Creating Kubernetes Job: {job_name}")

            # Build environment variables for the manager
            env_vars = [
                client.V1EnvVar(name="MEETING_URL", value=meeting_url),
                client.V1EnvVar(name="MEETING_ID", value=meeting_id),
                client.V1EnvVar(name="FS_MEETING_ID", value=str(meeting_doc_id)),
                client.V1EnvVar(name="USER_ID", value=str(user_doc_id)),
                client.V1EnvVar(name="GCS_PATH", value=gcs_path),
                client.V1EnvVar(name="GCS_BUCKET", value=self.gcs_bucket),
                client.V1EnvVar(name="MEETING_BOT_IMAGE", value=self.meeting_bot_image),
                client.V1EnvVar(name="NODE_ENV", value=self.node_env),
                client.V1EnvVar(
                    name="MAX_RECORDING_DURATION_MINUTES",
                    value=str(self.max_recording_duration),
                ),
                client.V1EnvVar(
                    name="MEETING_INACTIVITY_MINUTES",
                    value=str(self.meeting_inactivity),
                ),
                client.V1EnvVar(
                    name="INACTIVITY_DETECTION_START_DELAY_MINUTES",
                    value=str(self.inactivity_detection_delay),
                ),
            ]

            # Add ALL fields from message payload as environment variables
            # This ensures the manager has all the data it needs for the meeting-bot API
            # We add both original case AND uppercase versions for compatibility
            for key, value in message_data.items():
                if value is not None and isinstance(value, (str, int, float, bool)):
                    # Skip keys we've already added
                    if key.lower() not in ["meeting_url", "meeting_id", "gcs_path"]:
                        # Add original case (e.g., bearerToken, teamId, userId)
                        env_vars.append(client.V1EnvVar(name=key, value=str(value)))

                        # Also add UPPERCASE version for backward compatibility (e.g., BEARERTOKEN, TEAM_ID)
                        env_key_upper = key.upper().replace("-", "_")
                        if env_key_upper != key:  # Only add if different from original
                            env_vars.append(
                                client.V1EnvVar(name=env_key_upper, value=str(value))
                            )

            # Add optional metadata fields (for backward compatibility)
            if message_data.get("meeting_title"):
                env_vars.append(
                    client.V1EnvVar(
                        name="MEETING_TITLE", value=message_data["meeting_title"]
                    )
                )
            if message_data.get("organizer"):
                env_vars.append(
                    client.V1EnvVar(
                        name="MEETING_ORGANIZER", value=message_data["organizer"]
                    )
                )
            if message_data.get("start_time"):
                env_vars.append(
                    client.V1EnvVar(
                        name="MEETING_START_TIME", value=message_data["start_time"]
                    )
                )

            # Container 1: meeting-bot (TypeScript app that joins meetings)
            meeting_bot_container = client.V1Container(
                name="meeting-bot",
                image=self.meeting_bot_image,
                image_pull_policy="IfNotPresent",
                env=[
                    client.V1EnvVar(name="PORT", value="3000"),
                    client.V1EnvVar(name="NODE_ENV", value=self.node_env),
                    # Prefer using the RWX scratch PVC for temp files.
                    client.V1EnvVar(name="TMPDIR", value="/scratch/tmp"),
                    client.V1EnvVar(name="TMP", value="/scratch/tmp"),
                    client.V1EnvVar(name="TEMP", value="/scratch/tmp"),
                    # Prefer writing recording artifacts to the scratch PVC.
                    client.V1EnvVar(name="TEMPVIDEO_DIR", value="/scratch/tempvideo"),
                    client.V1EnvVar(
                        name="MAX_RECORDING_DURATION_MINUTES",
                        value=str(self.max_recording_duration),
                    ),
                    client.V1EnvVar(
                        name="MEETING_INACTIVITY_MINUTES",
                        value=str(self.meeting_inactivity),
                    ),
                    client.V1EnvVar(
                        name="INACTIVITY_DETECTION_START_DELAY_MINUTES",
                        value=str(self.inactivity_detection_delay),
                    ),
                    # Disable S3 upload - manager will handle the recording file
                    client.V1EnvVar(name="S3_ENDPOINT", value=""),
                    # Required by meeting-bot src/config.ts
                    client.V1EnvVar(name="GCP_MISC_BUCKET", value=self.gcs_bucket),
                    client.V1EnvVar(
                        name="GCP_DEFAULT_REGION",
                        value=os.getenv("GCP_DEFAULT_REGION", "us-central1"),
                    ),
                ],
                volume_mounts=[
                    client.V1VolumeMount(
                        name="scratch", mount_path="/usr/src/app/dist/_tempvideo"
                    ),
                    client.V1VolumeMount(name="scratch", mount_path="/scratch"),
                    # Mount shared memory for Chrome (prevents crashes)
                    client.V1VolumeMount(name="dshm", mount_path="/dev/shm"),
                    # Mount tmp for XDG and PulseAudio runtime directories
                    client.V1VolumeMount(name="tmp", mount_path="/tmp"),
                ],
                resources=client.V1ResourceRequirements(
                    requests={
                        "cpu": "3000m",  # 2 CPU cores for smooth audio/video processing
                        "memory": "2Gi",  # 2 GB memory
                        "ephemeral-storage": "8Gi",
                    },
                    limits={
                        "cpu": "4000m",  # 4 CPU cores (doubled for better performance)
                        "memory": "3Gi",  # 4 GB memory (increased for high-quality recording)
                        "ephemeral-storage": "8Gi",
                    },
                ),
            )

            # Container 2: manager (Python orchestrator that calls meeting-bot API)
            manager_container = client.V1Container(
                name="manager",
                image=self.manager_image,
                env=env_vars
                + [
                    # Manager needs to communicate with meeting-bot on localhost
                    client.V1EnvVar(
                        name="MEETING_BOT_API_URL", value="http://localhost:3000"
                    ),
                    # Prefer using the RWX scratch PVC for temp files.
                    client.V1EnvVar(name="TMPDIR", value="/scratch/tmp"),
                    client.V1EnvVar(name="TMP", value="/scratch/tmp"),
                    client.V1EnvVar(name="TEMP", value="/scratch/tmp"),
                ],
                volume_mounts=[
                    client.V1VolumeMount(name="recordings", mount_path="/recordings"),
                    client.V1VolumeMount(name="scratch", mount_path="/scratch"),
                ],
                image_pull_policy="IfNotPresent",
                resources=client.V1ResourceRequirements(
                    requests={
                        "cpu": "2500m",  # 2.5 CPU cores
                        "memory": "1Gi",  # 1 GB memory (doubled)
                        "ephemeral-storage": "2Gi",
                    },
                    limits={
                        "cpu": "3750m",  # 50% higher (3.75 CPU cores)
                        "memory": "1536Mi",  # 1.5 GB memory (doubled)
                        "ephemeral-storage": "2Gi",
                    },
                ),
            )

            # Define the pod template with BOTH containers
            template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "app": "meeting-bot-manager",
                        "meeting-id": meeting_id[:63],  # K8s label value max length
                    },
                    annotations={
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                ),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    priority_class_name="high-priority",
                    init_containers=[
                        client.V1Container(
                            name="init-scratch-dirs",
                            image="busybox:1.36",
                            command=[
                                "sh",
                                "-c",
                                "mkdir -p /scratch/tmp /scratch/tempvideo && chmod 1777 /scratch/tmp && chmod 0777 /scratch/tempvideo",
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name="scratch", mount_path="/scratch"
                                )
                            ],
                        )
                    ],
                    containers=[meeting_bot_container, manager_container],
                    service_account_name=self.job_service_account,
                    # Security context for audio/video capture
                    security_context=client.V1PodSecurityContext(
                        run_as_user=1001,  # nodejs user
                        run_as_group=1001,
                        fs_group=1001,  # Ensures volume mounts have correct permissions
                    ),
                    volumes=[
                        client.V1Volume(
                            name="recordings", empty_dir=client.V1EmptyDirVolumeSource()
                        ),
                        # Scratch will be mounted via a per-job RWO PVC created below.
                        # Shared memory for Chrome
                        client.V1Volume(
                            name="dshm",
                            empty_dir=client.V1EmptyDirVolumeSource(
                                medium="Memory", size_limit="2Gi"
                            ),
                        ),
                        # Temporary storage for runtime dirs (XDG, PulseAudio)
                        client.V1Volume(
                            name="tmp", empty_dir=client.V1EmptyDirVolumeSource()
                        ),
                    ],
                ),
            )

            # Create a per-job RWO scratch PVC for /scratch.
            # This avoids relying on RWX provisioning and keeps large artifacts
            # off node ephemeral storage.
            scratch_pvc_name = f"{job_name}-scratch"
            scratch_pvc = client.V1PersistentVolumeClaim(
                api_version="v1",
                kind="PersistentVolumeClaim",
                metadata=client.V1ObjectMeta(
                    name=scratch_pvc_name,
                    namespace=self.k8s_namespace,
                    labels={
                        "app": "meeting-bot-manager",
                        "meeting-id": meeting_id[:63],
                        "managed-by": "meeting-bot-controller",
                    },
                ),
                spec=client.V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    storage_class_name=os.getenv(
                        "SCRATCH_STORAGE_CLASS", "standard-rwo"
                    ),
                    resources=client.V1ResourceRequirements(
                        requests={"storage": os.getenv("SCRATCH_STORAGE_SIZE", "50Gi")}
                    ),
                ),
            )

            self.core_v1.create_namespaced_persistent_volume_claim(
                namespace=self.k8s_namespace, body=scratch_pvc
            )

            logger.info("Scratch PVC for job %s: %s", job_name, scratch_pvc_name)

            # Add the scratch volume to the template now that the PVC exists.
            template.spec.volumes.insert(
                1,
                client.V1Volume(
                    name="scratch",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=scratch_pvc_name
                    ),
                ),
            )

            # Define and create the job.
            job = client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=client.V1ObjectMeta(
                    name=job_name,
                    namespace=self.k8s_namespace,
                    labels={
                        "app": "meeting-bot-manager",
                        "meeting-id": meeting_id[:63],
                        "managed-by": "meeting-bot-controller",
                    },
                ),
                spec=client.V1JobSpec(
                    template=template,
                    backoff_limit=0,  # Do not retry on failure
                    # Hard cap the overall job runtime. This prevents runaway
                    # pods if recording/monitoring gets stuck.
                    active_deadline_seconds=39600,  # 11 hours
                    ttl_seconds_after_finished=3600,  # Clean up after 1 hour
                ),
            )

            created_job = self.batch_v1.create_namespaced_job(
                namespace=self.k8s_namespace,
                body=job,
            )

            # Update the scratch PVC ownerReference to point at the Job.
            scratch_pvc.metadata.owner_references = [
                client.V1OwnerReference(
                    api_version=created_job.api_version,
                    kind=created_job.kind,
                    name=created_job.metadata.name,
                    uid=created_job.metadata.uid,
                    controller=True,
                    block_owner_deletion=True,
                )
            ]
            self.core_v1.patch_namespaced_persistent_volume_claim(
                name=scratch_pvc_name,
                namespace=self.k8s_namespace,
                body={
                    "metadata": {
                        "ownerReferences": scratch_pvc.metadata.owner_references
                    }
                },
            )

            logger.info(f"✅ Created job '{job_name}' for meeting {meeting_id}")
            return True

        except ApiException as e:
            logger.error(f"❌ Kubernetes API error creating job: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error creating manager job: {e}")
            return False

    def _build_job_payload_from_firestore(
        self, bot_doc: firestore.DocumentSnapshot
    ) -> Dict[str, Any]:
        """Translate a bot_instance Firestore doc into the payload expected by the manager.

        This mirrors (a subset of) what the Firebase callable function used to publish
        to Pub/Sub.
        """
        data = bot_doc.to_dict() or {}

        meeting_url = data.get("meeting_url")
        if not meeting_url:
            raise ValueError("bot_instance missing meeting_url")

        # Best-effort meeting id (join link meeting id).
        meeting_id = (
            data.get("meeting_id")
            or data.get("initial_linked_meeting", {}).get("meeting_id")
            or bot_doc.id
        )

        # Canonical Firebase document id used for storage prefix.
        meeting_doc_id = bot_doc.id

        org_id = (
            data.get("creator_organization_id")
            or data.get("initial_linked_meeting", {}).get("organization_id")
            or ""
        )

        now = datetime.now(timezone.utc)

        user_doc_id = (
            data.get("creator_user_id")
            or data.get("user_id")
            or data.get("initial_linked_meeting", {}).get("user_id")
            or ""
        )

        gcs_path = (
            f"recordings/{user_doc_id}/{meeting_doc_id}"
            if user_doc_id
            else f"recordings/{meeting_doc_id}"
        )

        payload: Dict[str, Any] = {
            "meeting_url": meeting_url,
            "meeting_id": meeting_id,
            "gcs_path": gcs_path,
            "fs_meeting_id": meeting_doc_id,
            # Maintain compatibility with existing manager payload expectations.
            "name": data.get("bot_name") or data.get("name") or "Meeting Bot",
            "teamId": org_id or data.get("teamId") or data.get("team_id") or meeting_id,
            "timezone": data.get("timezone") or "UTC",
            "user_id": user_doc_id,
            "user_email": data.get("user_email", ""),
            "initiated_at": data.get("initiated_at")
            or (now.isoformat().replace("+00:00", "Z")),
            "auto_joined": bool(data.get("auto_joined", False)),
            # Handy for consumers/debugging.
            "bot_instance_id": bot_doc.id,
        }

        # Preserve pass-through fields if present.
        for key in [
            "bearerToken",
            "bearer_token",
            "userId",
            "user_id",
            "botId",
            "bot_id",
            "eventId",
            "event_id",
        ]:
            if key in data and data[key] is not None:
                payload[key] = data[key]

        return payload

    def _query_queued_bot_instances(self) -> List[firestore.DocumentSnapshot]:
        """Find candidate bot instances to process."""
        q = (
            self.db.collection("bot_instances")
            .where(
                field_path=self.bot_instance_status_field,
                op_string="==",
                value=self.bot_instance_queued_value,
            )
            .limit(self.max_claim_per_poll)
        )
        results = list(q.stream())
        logger.debug(
            f"Query bot_instances where {self.bot_instance_status_field}="
            f"'{self.bot_instance_queued_value}': found {len(results)} docs"
        )
        return results

    def _query_meetings_needing_bots(self) -> List[firestore.DocumentSnapshot]:
        """Discover meetings that need a bot instance created.

        This intentionally stays flexible because meeting schemas vary.

        Default behavior:
        - Read from flat `meetings` collection
        - Filter by status in MEETING_STATUS_VALUES (default: scheduled)
        - Require meeting_url
        - Skip if meeting already has bot_instance_id
        """

        if self.meetings_query_mode == "collection_group":
            coll = self.db.collection_group(self.meetings_collection_path)
        else:
            coll = self.db.collection(self.meetings_collection_path)

        # Firestore doesn't support IN queries combined with some inequality
        # patterns consistently without composite indexes. Keep this simple:
        # if multiple statuses provided, just query the first one.
        status_value = (
            self.meeting_status_values[0] if self.meeting_status_values else "scheduled"
        )

        logger.debug(
            "Querying meetings: path=%s, status=%s",
            self.meetings_collection_path,
            status_value,
        )

        # Pagination loop to find meetings that actually need bots
        # (skipping those that already have bot_instance_id)
        candidates: List[firestore.DocumentSnapshot] = []
        last_doc = None
        page_size = 50  # Fetch larger batches to skip processed items efficiently
        max_scan = 500  # Safety limit to prevent infinite scanning

        scanned_count = 0

        while len(candidates) < self.max_claim_per_poll and scanned_count < max_scan:
            q = coll.where(
                field_path=self.meeting_status_field,
                op_string="==",
                value=status_value,
            ).limit(page_size)

            if last_doc:
                q = q.start_after(last_doc)

            batch = list(q.stream())
            if not batch:
                break

            for doc in batch:
                scanned_count += 1
                last_doc = doc
                data = doc.to_dict() or {}

                # Skip if already has bot instance
                if data.get(self.meeting_bot_instance_field):
                    continue

                # Skip if missing meeting_url (required to create bot)
                if not (data.get("meeting_url") or data.get("meetingUrl")):
                    continue

                candidates.append(doc)
                if len(candidates) >= self.max_claim_per_poll:
                    break

            # If we got fewer docs than page_size, we reached the end
            if len(batch) < page_size:
                break

        if scanned_count >= max_scan:
            logger.warning(
                "Scanned %d meetings without finding enough candidates. "
                "Consider cleaning up old 'scheduled' meetings.",
                scanned_count,
            )

        return candidates

    def _try_create_bot_instance_for_meeting(
        self,
        meeting_doc: firestore.DocumentSnapshot,
    ) -> Optional[str]:
        """Create a bot_instances document for a meeting (idempotent).

        Returns:
            bot_instance_id if created or already exists, else None.
        """

        meeting_data = meeting_doc.to_dict() or {}
        meeting_ref = meeting_doc.reference

        # Skip if meeting already linked.
        existing_bot_instance = meeting_data.get(self.meeting_bot_instance_field)
        if existing_bot_instance:
            return str(existing_bot_instance)

        meeting_url = (
            meeting_data.get("meeting_url")
            or meeting_data.get("meetingUrl")
            or meeting_data.get("join_url")
        )
        if not meeting_url:
            logger.warning(
                f"Meeting {meeting_doc.id} has no meeting_url, meetingUrl, "
                f"or join_url field. Available fields: {list(meeting_data.keys())}"
            )
            return None

        org_id = (
            meeting_data.get("organization_id")
            or meeting_data.get("organizationId")
            or meeting_data.get("teamId")
            or meeting_data.get("team_id")
            or ""
        )
        user_id = meeting_data.get("user_id") or meeting_data.get("userId") or ""

        now = datetime.now(timezone.utc)

        # Dedupe by meeting id: One bot instance per meeting doc.
        bot_ref = self.db.collection("bot_instances").document(meeting_doc.id)

        status_field = self.bot_instance_status_field
        queued_value = self.bot_instance_queued_value

        logger.debug(
            f"Creating bot_instance for meeting {meeting_doc.id}: "
            f"{status_field}={queued_value}"
        )

        transaction = self.db.transaction()

        @firestore.transactional
        def _txn(txn: firestore.Transaction) -> Optional[str]:
            fresh_meeting = meeting_ref.get(transaction=txn)
            if not fresh_meeting.exists:
                logger.warning(f"Meeting {meeting_doc.id} no longer exists")
                return None

            fresh_data = fresh_meeting.to_dict() or {}
            if fresh_data.get(self.meeting_bot_instance_field):
                logger.debug(
                    f"Meeting {meeting_doc.id} already has bot_instance: "
                    f"{fresh_data.get(self.meeting_bot_instance_field)}"
                )
                return str(fresh_data.get(self.meeting_bot_instance_field))

            bot_snap = bot_ref.get(transaction=txn)
            if bot_snap.exists:
                # Link meeting to existing bot instance.
                logger.debug(
                    f"Bot instance {bot_ref.id} already exists, linking to "
                    f"meeting {meeting_doc.id}"
                )
                txn.update(
                    meeting_ref,
                    {
                        self.meeting_bot_instance_field: bot_ref.id,
                        "bot_status": "queued",
                        "bot_enqueued_at": now,
                    },
                )
                return bot_ref.id

            # Create bot instance.
            logger.debug(f"Creating new bot_instance {bot_ref.id}")
            txn.set(
                bot_ref,
                {
                    status_field: queued_value,
                    "meeting_url": meeting_url,
                    "meeting_id": meeting_doc.id,
                    "creator_user_id": user_id,
                    "creator_organization_id": org_id,
                    "bot_name": meeting_data.get("bot_name")
                    or meeting_data.get("name")
                    or "Meeting Bot",
                    "created_at": now,
                    "initial_linked_meeting": {
                        "meeting_id": meeting_doc.id,
                        "organization_id": org_id,
                        "user_id": user_id,
                    },
                },
            )

            # Link meeting to bot instance.
            txn.update(
                meeting_ref,
                {
                    self.meeting_bot_instance_field: bot_ref.id,
                    "bot_status": "queued",
                    "bot_enqueued_at": now,
                },
            )

            logger.info(
                f"Created bot_instance {bot_ref.id} with {status_field}="
                f"{queued_value}"
            )
            return bot_ref.id

        try:
            result = _txn(transaction)
            if result:
                logger.debug(f"Transaction successful: bot_instance {result}")
            return result
        except Exception as e:
            logger.error(
                f"Failed to create bot_instance for meeting {meeting_doc.id}: " f"{e}",
                exc_info=True,
            )
            return None

    def _try_claim_bot_instance(self, bot_ref: firestore.DocumentReference) -> bool:
        """Attempt to claim a bot instance.

        We use a Firestore transaction to set claimed_* fields when the bot is still queued
        and either unclaimed or claim has expired.
        """

        claim_expires_at_field = "claim_expires_at"
        claimed_by_field = "claimed_by"
        claimed_at_field = "claimed_at"
        status_field = self.bot_instance_status_field
        queued_value = self.bot_instance_queued_value
        processing_value = os.getenv("BOT_INSTANCE_PROCESSING_VALUE", "processing")

        controller_id = (
            os.getenv("CONTROLLER_ID") or os.getenv("HOSTNAME") or "controller"
        )
        now = datetime.now(timezone.utc)
        expires = datetime.fromtimestamp(
            now.timestamp() + self.claim_ttl_seconds, tz=timezone.utc
        )

        transaction = self.db.transaction()

        @firestore.transactional
        def _txn(txn: firestore.Transaction) -> bool:
            snap = bot_ref.get(transaction=txn)
            if not snap.exists:
                return False

            data = snap.to_dict() or {}

            # Only claim queued items.
            if data.get(status_field) != queued_value:
                return False

            # Allow claim if unclaimed or expired.
            existing_exp = data.get(claim_expires_at_field)
            if existing_exp is not None:
                try:
                    exp_dt = (
                        existing_exp.replace(tzinfo=timezone.utc)
                        if getattr(existing_exp, "tzinfo", None) is None
                        else existing_exp
                    )
                except Exception:
                    exp_dt = None
                if exp_dt and exp_dt > now:
                    return False

            txn.update(
                bot_ref,
                {
                    claimed_by_field: controller_id,
                    claimed_at_field: now,
                    claim_expires_at_field: expires,
                    status_field: processing_value,
                },
            )
            return True

        return bool(_txn(transaction))

    def _mark_bot_instance_done(
        self, bot_ref: firestore.DocumentReference, ok: bool
    ) -> None:
        done_value = os.getenv("BOT_INSTANCE_DONE_VALUE", "done")
        failed_value = os.getenv("BOT_INSTANCE_FAILED_VALUE", "failed")
        status_field = self.bot_instance_status_field
        bot_ref.update(
            {
                status_field: done_value if ok else failed_value,
                "processed_at": datetime.now(timezone.utc),
            }
        )

    def _try_acquire_leadership(self) -> bool:
        """Try to acquire or renew leadership lease.

        Returns:
            True if this instance is the leader, False otherwise.
        """
        leader_ref = self.db.collection(self.leader_collection_path).document(
            self.leader_doc_id
        )

        @firestore.transactional
        def update_in_transaction(transaction, ref):
            snapshot = ref.get(transaction=transaction)
            now = datetime.now(timezone.utc)
            expires_at = now + timedelta(seconds=self.leader_lease_seconds)

            new_data = {
                "leader_id": self.instance_id,
                "lease_expires_at": expires_at,
                "last_renewed_at": now,
            }

            if not snapshot.exists:
                transaction.set(ref, new_data)
                return True

            data = snapshot.to_dict()
            current_leader = data.get("leader_id")
            lease_expires = data.get("lease_expires_at")

            # If lease is valid and held by someone else
            if (
                current_leader != self.instance_id
                and lease_expires
                and lease_expires > now
            ):
                return False

            # Otherwise (expired or held by me), claim/renew it
            transaction.set(ref, new_data)
            return True

        try:
            transaction = self.db.transaction()
            is_leader = update_in_transaction(transaction, leader_ref)

            if is_leader and not self.is_leader:
                logger.info("👑 Acquired leadership (instance: %s)", self.instance_id)
            elif not is_leader and self.is_leader:
                logger.info("Lost leadership")

            self.is_leader = is_leader
            return is_leader
        except Exception as e:
            logger.error("Error during leadership election: %s", e)
            # If we can't talk to Firestore, assume we lost leadership to be safe
            self.is_leader = False
            return False

    def _scan_upcoming_meetings(self):
        """Scan for meetings starting soon and enqueue them."""
        now = datetime.now(timezone.utc)
        target_time = now + timedelta(minutes=2)
        window_start = target_time - timedelta(seconds=30)
        window_end = target_time + timedelta(seconds=30)

        logger.info(f"Scanning meetings: {window_start} to {window_end}")

        if self.meetings_query_mode == "collection_group":
            coll = self.db.collection_group(self.meetings_collection_path)
        else:
            coll = self.db.collection(self.meetings_collection_path)

        # Query by time window
        query = coll.where("start", ">=", window_start).where("start", "<=", window_end)

        try:
            docs = list(query.stream())
            logger.info(f"Found {len(docs)} meetings in time window")

            for doc in docs:
                data = doc.to_dict()

                logger.debug(
                    f"Evaluating meeting {doc.id}: "
                    f"status={data.get(self.meeting_status_field)}, "
                    f"bot_instance_id={data.get(self.meeting_bot_instance_field)}, "
                    f"join_url={data.get('join_url', '')[:50]}"
                )

                # Filter by status
                status = data.get(self.meeting_status_field)
                if (
                    self.meeting_status_values
                    and status not in self.meeting_status_values
                ):
                    logger.debug(
                        f"Skipping {doc.id}: status '{status}' not in "
                        f"{self.meeting_status_values}"
                    )
                    continue

                # Check if bot already exists
                if data.get(self.meeting_bot_instance_field):
                    logger.debug(f"Skipping {doc.id}: bot_instance already exists")
                    continue

                # Check user and meeting settings for auto-join/AI assistant
                user_id = data.get("user_id") or data.get("created_by")
                ai_enabled = data.get("ai_assistant_enabled", False)
                auto_join = False

                if user_id:
                    user_ref = self.db.collection("users").document(user_id)
                    user_doc = user_ref.get()
                    if user_doc.exists:
                        user_data = user_doc.to_dict()
                        auto_join = user_data.get("auto_join_meetings", False)

                logger.debug(
                    f"Meeting {doc.id}: ai_enabled={ai_enabled}, "
                    f"auto_join={auto_join}"
                )

                if not (ai_enabled or auto_join):
                    logger.debug(f"Skipping {doc.id}: neither ai_enabled nor auto_join")
                    continue

                # Check if Teams meeting
                join_url = data.get("join_url") or ""
                if "teams.microsoft.com" not in join_url:
                    logger.debug(f"Skipping {doc.id}: not a Teams meeting")
                    continue

                # Create bot instance
                logger.info(f"Enqueuing bot for meeting {doc.id}")
                bot_id = self._try_create_bot_instance_for_meeting(doc)
                if bot_id:
                    logger.info(f"Created bot_instance {bot_id} for meeting {doc.id}")
                else:
                    logger.warning(
                        f"Failed to create bot_instance for meeting {doc.id}"
                    )
        except Exception as e:
            logger.error(f"Error scanning upcoming meetings: {e}", exc_info=True)

    def _pubsub_callback(self, message: pubsub_v1.subscriber.message.Message):
        """Handle incoming Pub/Sub messages."""
        try:
            data = json.loads(message.data.decode("utf-8"))
            meeting_id = data.get("meeting_id")
            org_id = data.get("teamId")

            logger.info(f"Received Pub/Sub message for meeting: {meeting_id}")

            if not meeting_id:
                logger.error("Message missing meeting_id")
                message.ack()  # Ack invalid messages to remove them
                return

            # Try to find the bot instance to claim it
            bot_instance_id = None

            # 1. Check if meeting doc has bot_instance_id
            if org_id:
                meeting_ref = (
                    self.db.collection("organizations")
                    .document(org_id)
                    .collection("meetings")
                    .document(meeting_id)
                )
                meeting_doc = meeting_ref.get()
                if meeting_doc.exists:
                    bot_instance_id = meeting_doc.get(self.meeting_bot_instance_field)

            # 2. If not found, try the default ID convention (meeting_id)
            if not bot_instance_id:
                bot_instance_id = meeting_id

            # 3. Try to claim the bot instance
            bot_ref = self.db.collection("bot_instances").document(str(bot_instance_id))

            # If bot instance doesn't exist, launch anyway
            # Risk: double-launching if Poller picks it up
            # Mitigation: claim atomically prevents double-launch

            claimed = self._try_claim_bot_instance(bot_ref)

            if claimed:
                logger.info(f"Claimed bot instance {bot_instance_id} via Pub/Sub")
                if self.create_manager_job(data, message.message_id):
                    self._mark_bot_instance_done(bot_ref, ok=True)
                    message.ack()
                else:
                    self._mark_bot_instance_done(bot_ref, ok=False)
                    message.nack()
            else:
                # Could not claim. Check if it exists
                # If missing, launch without state tracking
                # If exists, someone else is handling it
                if not bot_ref.get().exists:
                    logger.warning(
                        f"Bot instance {bot_instance_id} not found. "
                        f"Launching without state tracking."
                    )
                    if self.create_manager_job(data, message.message_id):
                        message.ack()
                    else:
                        message.nack()
                else:
                    logger.info(f"Bot instance {bot_instance_id} already processing")
                    message.ack()  # Ack: someone else handling it

        except Exception as e:
            logger.error(f"Error processing Pub/Sub message: {e}")
            message.nack()

    def _start_pubsub_listener(self):
        """Start the Pub/Sub subscriber in a background thread."""
        if not self.pubsub_subscription:
            logger.warning("No PUBSUB_SUBSCRIPTION configured. Skipping listener.")
            return

        try:
            subscriber = pubsub_v1.SubscriberClient()
            self.subscriber = subscriber
            self.streaming_pull_future = subscriber.subscribe(
                self.pubsub_subscription, callback=self._pubsub_callback
            )
            logger.info(f"Listening on {self.pubsub_subscription}")
        except Exception as e:
            logger.error(f"Failed to start Pub/Sub listener: {e}")

    def run(self):
        """Main run loop - continuously process queued Firestore work"""
        logger.info("=" * 50)
        logger.info("🚀 Meeting Bot Controller starting...")
        logger.info("=" * 50)
        logger.info(f"📡 Project ID: {self.project_id}")
        logger.info(f"�️  Firestore DB: {self.firestore_database}")
        logger.info(f"📁 Namespace: {self.k8s_namespace}")
        logger.info(f"🐳 Manager Image: {self.manager_image}")
        logger.info(f"🐳 Meeting Bot Image: {self.meeting_bot_image}")
        logger.info("=" * 50)

        # Start health check server
        health_server = HealthCheckServer()
        health_server.start()

        logger.info(f"Polling interval: {self.poll_interval}s")

        # Start the Pub/Sub listener
        self._start_pubsub_listener()

        while True:
            try:
                # Check leadership
                if not self._try_acquire_leadership():
                    logger.debug("Not leader, sleeping...")
                    time.sleep(self.poll_interval)
                    continue

                logger.debug("Starting poll cycle...")

                # Step 0: discover meetings starting soon (2min window)
                self._scan_upcoming_meetings()

                # Step 1: process queued bot instances.
                bot_docs = self._query_queued_bot_instances()
                if not bot_docs:
                    logger.info(
                        "No queued meetings found. Waiting %ss...",
                        self.poll_interval,
                    )
                    time.sleep(self.poll_interval)
                    continue

                logger.info("Found %s queued bot instance(s)", len(bot_docs))

                for bot_doc in bot_docs:
                    bot_ref = bot_doc.reference
                    try:
                        if not self._try_claim_bot_instance(bot_ref):
                            continue

                        payload = self._build_job_payload_from_firestore(bot_doc)
                        ok = self.create_manager_job(payload, bot_doc.id)
                        # Mark done/failed based on job creation.
                        self._mark_bot_instance_done(bot_ref, ok=ok)
                    except Exception as e:
                        logger.error(
                            "Failed processing bot instance %s: %s",
                            bot_doc.id,
                            e,
                            exc_info=True,
                        )
                        try:
                            self._mark_bot_instance_done(bot_ref, ok=False)
                        except Exception:
                            # Best-effort only.
                            pass

            except KeyboardInterrupt:
                logger.info("👋 Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"❌ Error in main loop: {e}", exc_info=True)
                time.sleep(self.poll_interval)


def main():
    """Entry point"""
    try:
        controller = MeetingController()
        controller.run()
    except KeyboardInterrupt:
        logger.info("👋 Shutting down controller")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
