#!/usr/bin/env python3
"""
Manual fanout trigger for failed/stuck meeting sessions.

This script manually triggers the fanout process for a specific meeting session.
Use this when fanout failed or never ran.

Usage:
    python scripts/manual_fanout_trigger.py \\
        --project <gcp-project-id> \\
        --org-id <organization-id> \\
        --session-id <meeting-session-id> \\
        --bucket <gcs-bucket-name>
    
Options:
    --dry-run    Show what would be done without making changes
    --force      Reset fanout status even if marked complete
"""

import argparse
import sys
from datetime import datetime, timezone


def manual_fanout(
    db,
    storage_client,
    bucket_name: str,
    org_id: str,
    session_id: str,
    dry_run: bool = False,
    force: bool = False,
) -> int:
    """Manually trigger fanout for a session."""

    print(f"\n{'='*80}")
    print("MANUAL FANOUT TRIGGER")
    print(f"Session: {session_id}")
    print(f"Organization: {org_id}")
    print(f"Bucket: {bucket_name}")
    if dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    print(f"{'='*80}\n")

    # Get session
    session_ref = (
        db.collection("organizations")
        .document(org_id)
        .collection("meeting_sessions")
        .document(session_id)
    )

    session_snap = session_ref.get()
    if not session_snap.exists:
        print(f"❌ Session not found: {session_ref.path}")
        return 1

    session_data = session_snap.to_dict() or {}
    fanout_status = session_data.get("fanout_status")

    print(f"Current fanout status: {fanout_status or 'NOT SET'}")

    if fanout_status == "complete" and not force:
        print("\n⚠️  Fanout already marked complete!")
        print("Use --force to re-run anyway, or manually check subscribers.")
        return 1

    # Get subscribers
    subscribers = list(session_ref.collection("subscribers").stream())
    print(f"\nFound {len(subscribers)} subscriber(s)")

    if len(subscribers) == 0:
        print("❌ No subscribers - nothing to fanout")
        return 1

    # First subscriber is source
    first_sub = subscribers[0]
    first_data = first_sub.to_dict() or {}
    source_user_id = first_data.get("user_id") or first_sub.id
    source_meeting_id = first_data.get("fs_meeting_id")

    if not source_user_id or not source_meeting_id:
        print(f"❌ First subscriber missing user_id or fs_meeting_id")
        return 1

    source_prefix = f"recordings/{source_user_id}/{source_meeting_id}"
    print(f"\nSource: {source_prefix}")

    # List source files
    bucket = storage_client.bucket(bucket_name)
    src_blobs = list(bucket.list_blobs(prefix=f"{source_prefix}/"))

    if not src_blobs:
        print(f"❌ No source files found at gs://{bucket_name}/{source_prefix}/")
        return 1

    print(f"Source files: {len(src_blobs)}")
    for blob in src_blobs[:10]:  # Show first 10
        size_mb = blob.size / (1024 * 1024)
        print(f"  - {blob.name} ({size_mb:.2f} MB)")
    if len(src_blobs) > 10:
        print(f"  ... and {len(src_blobs) - 10} more")

    # Try to read transcription
    transcription_text = None
    transcript_path = f"{source_prefix}/transcript.txt"
    try:
        transcript_blob = bucket.blob(transcript_path)
        if transcript_blob.exists():
            transcription_text = transcript_blob.download_as_text()
            print(f"\n✅ Found transcription ({len(transcription_text)} chars)")
    except Exception as e:
        print(f"\n⚠️  Could not read transcription: {e}")

    # Update first subscriber to complete
    print(f"\n📝 Marking first subscriber as complete...")
    if not dry_run:
        first_sub.reference.set(
            {
                "status": "complete",
                "updated_at": datetime.now(timezone.utc),
            },
            merge=True,
        )

    # Update first subscriber's meeting doc with transcription
    first_meeting_path = first_data.get("meeting_path")
    if first_meeting_path and transcription_text:
        print(f"📝 Updating first subscriber's meeting with transcription...")
        if not dry_run:
            try:
                meeting_ref = db.document(first_meeting_path)
                meeting_ref.set(
                    {
                        "transcription": transcription_text,
                        "recording_url": f"gs://{bucket_name}/{source_prefix}/recording.webm",
                        "updated_at": datetime.now(timezone.utc),
                    },
                    merge=True,
                )
            except Exception as e:
                print(f"⚠️  Failed to update meeting doc: {e}")

    # Copy to remaining subscribers
    for idx, sub in enumerate(subscribers[1:], 2):
        sub_data = sub.to_dict() or {}
        user_id = sub_data.get("user_id") or sub.id
        fs_meeting_id = sub_data.get("fs_meeting_id")
        meeting_path = sub_data.get("meeting_path")

        if not user_id or not fs_meeting_id:
            print(f"\n⚠️  Subscriber #{idx} missing user_id or fs_meeting_id, skipping")
            continue

        dst_prefix = f"recordings/{user_id}/{fs_meeting_id}"

        if dst_prefix == source_prefix:
            print(f"\n  Subscriber #{idx}: Same as source, skipping")
            continue

        print(f"\n  Subscriber #{idx}: {dst_prefix}")

        copied = 0
        skipped = 0
        errors = 0

        for src_blob in src_blobs:
            src_name = src_blob.name
            if not src_name.startswith(f"{source_prefix}/"):
                continue

            rel_path = src_name[len(source_prefix) + 1 :]
            dst_name = f"{dst_prefix}/{rel_path}"

            # Check if already exists
            dst_blob = bucket.blob(dst_name)
            if dst_blob.exists():
                skipped += 1
                continue

            # Copy
            if dry_run:
                print(f"    [DRY RUN] Would copy: {rel_path}")
                copied += 1
            else:
                try:
                    bucket.copy_blob(src_blob, bucket, dst_name)
                    copied += 1
                except Exception as e:
                    print(f"    ❌ Error copying {rel_path}: {e}")
                    errors += 1

        print(f"    Copied: {copied}, Skipped: {skipped}, Errors: {errors}")

        # Update subscriber status
        if not dry_run:
            sub.reference.set(
                {
                    "status": "copied",
                    "copied_at": datetime.now(timezone.utc),
                    "copied_count": copied,
                    "skipped_count": skipped,
                    "total_count": len(src_blobs),
                    "updated_at": datetime.now(timezone.utc),
                },
                merge=True,
            )

        # Update meeting document
        if meeting_path and not dry_run:
            try:
                meeting_ref = db.document(meeting_path)
                meeting_update = {
                    "updated_at": datetime.now(timezone.utc),
                    "recording_url": f"gs://{bucket_name}/{dst_prefix}/recording.webm",
                }
                if transcription_text:
                    meeting_update["transcription"] = transcription_text
                meeting_ref.set(meeting_update, merge=True)
                print(f"    ✅ Updated meeting document")
            except Exception as e:
                print(f"    ⚠️  Failed to update meeting doc: {e}")

    # Mark fanout complete
    print(f"\n📝 Marking fanout as complete...")
    if not dry_run:
        session_ref.set(
            {
                "fanout_status": "complete",
                "fanout_completed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
            merge=True,
        )

    print(f"\n{'='*80}")
    print("✅ FANOUT COMPLETE")
    print(f"{'='*80}\n")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manually trigger fanout for a meeting session"
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--org-id", required=True, help="Organization ID")
    parser.add_argument("--session-id", required=True, help="Meeting session ID")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument(
        "--firestore-database", default="(default)", help="Firestore database name"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-run even if marked complete"
    )

    args = parser.parse_args()

    from google.cloud import firestore, storage

    db = firestore.Client(project=args.project, database=args.firestore_database)
    storage_client = storage.Client(project=args.project)

    return manual_fanout(
        db,
        storage_client,
        args.bucket,
        args.org_id,
        args.session_id,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    sys.exit(main())
