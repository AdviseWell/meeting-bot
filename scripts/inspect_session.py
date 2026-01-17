#!/usr/bin/env python3
"""
Inspect a specific meeting session in detail.
"""

import argparse
import sys
from datetime import datetime, timezone
from google.cloud import firestore, storage


def inspect_session(
    project_id, org_id, session_id, firestore_db="(default)", bucket_name=None
):
    """Inspect a specific session in detail."""

    db = firestore.Client(project=project_id, database=firestore_db)

    if bucket_name:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
    else:
        bucket = None

    print("=" * 80)
    print(f"INSPECTING SESSION: {session_id}")
    print("=" * 80)

    session_ref = (
        db.collection("organizations")
        .document(org_id)
        .collection("meeting_sessions")
        .document(session_id)
    )

    session_snap = session_ref.get()

    if not session_snap.exists:
        print(f"❌ Session not found!")
        return 1

    session_data = session_snap.to_dict() or {}

    print("\n📄 SESSION DOCUMENT:")
    print(f"  Path: {session_ref.path}")

    for key, value in sorted(session_data.items()):
        if hasattr(value, "timestamp"):
            dt = datetime.fromtimestamp(value.timestamp(), tz=timezone.utc)
            value_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            value_str = str(value)[:100]
        print(f"  {key}: {value_str}")

    # Get subscribers
    subscribers = list(session_ref.collection("subscribers").stream())

    print(f"\n\n👥 SUBSCRIBERS: {len(subscribers)}")
    print("=" * 80)

    for idx, sub in enumerate(subscribers, 1):
        sub_data = sub.to_dict() or {}
        user_id = sub_data.get("user_id", sub.id)
        fs_meeting_id = sub_data.get("fs_meeting_id", "MISSING")
        meeting_path = sub_data.get("meeting_path", "MISSING")
        status = sub_data.get("status", "MISSING")

        print(f"\nSubscriber #{idx}: {sub.id}")
        print(f"  user_id: {user_id}")
        print(f"  fs_meeting_id: {fs_meeting_id}")
        print(f"  meeting_path: {meeting_path}")
        print(f"  status: {status}")

        # Get meeting doc details
        if meeting_path and meeting_path != "MISSING":
            try:
                meeting_doc = db.document(meeting_path).get()
                if meeting_doc.exists:
                    meeting_data = meeting_doc.to_dict() or {}
                    meeting_name = meeting_data.get("name", "Unknown")
                    has_transcript = "transcription" in meeting_data
                    recording_url = meeting_data.get("recording_url", "")

                    print(f"  📋 Meeting Name: {meeting_name}")
                    print(f"  Has Transcription: {has_transcript}")
                    if recording_url:
                        print(f"  Recording URL: {recording_url[:80]}")
                else:
                    print(f"  ⚠️  Meeting document not found at {meeting_path}")
            except Exception as e:
                print(f"  ⚠️  Error reading meeting doc: {e}")

        # Check GCS files
        if bucket and fs_meeting_id != "MISSING":
            gcs_prefix = f"recordings/{user_id}/{fs_meeting_id}/"
            print(f"  📁 GCS Path: gs://{bucket_name}/{gcs_prefix}")

            try:
                blobs = list(bucket.list_blobs(prefix=gcs_prefix, max_results=20))
                if blobs:
                    print(f"  ✅ Found {len(blobs)} file(s):")
                    for blob in blobs:
                        size_mb = blob.size / (1024 * 1024)
                        rel_path = blob.name.replace(gcs_prefix, "")
                        print(f"     - {rel_path} ({size_mb:.2f} MB)")
                else:
                    print(f"  ❌ No files found in GCS")
            except Exception as e:
                print(f"  ⚠️  Error checking GCS: {e}")

    # Analysis
    print("\n\n" + "=" * 80)
    print("ANALYSIS")
    print("=" * 80)

    status = session_data.get("status", "unknown")
    fanout_status = session_data.get("fanout_status", None)
    org_id_field = session_data.get("org_id", None)

    print(f"\n✓ org_id field: {org_id_field or 'MISSING'}")
    print(f"✓ status: {status}")
    print(f"✓ fanout_status: {fanout_status or 'not set'}")
    print(f"✓ subscribers: {len(subscribers)}")

    issues = []

    if not org_id_field:
        issues.append("Missing org_id field")

    if status != "complete":
        issues.append(f"Status is '{status}', not 'complete'")

    if fanout_status == "complete":
        print("\n✅ Fanout already complete")
    elif len(subscribers) <= 1:
        print("\n✅ Only 1 subscriber, fanout not needed")
    elif status != "complete":
        print(
            f"\n⚠️  Status is '{status}', fanout won't trigger until status='complete'"
        )
        print("\nPossible reasons:")
        print("  - Manager/bot job is still running")
        print("  - Manager/bot job failed")
        print("  - Manager/bot job never marked session complete")
    elif not org_id_field:
        print("\n❌ Missing org_id field - controller will skip this session!")
    else:
        print("\n⚠️  Session should be ready for fanout!")
        print("Controller should pick it up on next poll cycle.")

    if issues:
        print(f"\n❌ Found {len(issues)} issue(s):")
        for issue in issues:
            print(f"   - {issue}")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect a specific meeting session")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--org-id", required=True, help="Organization ID")
    parser.add_argument("--session-id", required=True, help="Session ID to inspect")
    parser.add_argument(
        "--firestore-database", default="(default)", help="Firestore database name"
    )
    parser.add_argument("--bucket", help="GCS bucket name (optional, to check files)")

    args = parser.parse_args()

    try:
        sys.exit(
            inspect_session(
                project_id=args.project,
                org_id=args.org_id,
                session_id=args.session_id,
                firestore_db=args.firestore_database,
                bucket_name=args.bucket,
            )
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
