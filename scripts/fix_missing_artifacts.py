#!/usr/bin/env python3
"""
Fix missing artifacts on meeting documents.

This script finds completed sessions and ensures all subscribers
have the artifacts field populated on their meeting documents.
"""

import os
from datetime import datetime, timezone
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

PROJECT_ID = "aw-development-7226"
ORG_ID = "advisewell"
BUCKET_NAME = "advisewell-firebase-development"


def get_db():
    creds_path = os.path.expanduser(
        "~/.config/gcloud/application_default_credentials.json"
    )
    if os.path.exists(creds_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    return firestore.Client(project=PROJECT_ID, database="(default)")


def fix_session_artifacts(db, session_id: str, dry_run: bool = True):
    """Fix artifacts for all subscribers in a session."""
    session_ref = db.document(f"organizations/{ORG_ID}/meeting_sessions/{session_id}")
    session_doc = session_ref.get()

    if not session_doc.exists:
        print(f"  Session not found: {session_id}")
        return 0

    session_data = session_doc.to_dict()
    session_artifacts = session_data.get("artifacts", {})

    if not session_artifacts:
        print(f"  No artifacts in session")
        return 0

    print(f"  Session has {len(session_artifacts)} artifacts")

    # Get source prefix from first artifact
    first_artifact = list(session_artifacts.values())[0]
    # Example: recordings/USER_ID/MEETING_ID/transcript.txt
    parts = first_artifact.split("/")
    if len(parts) >= 3:
        source_user_id = parts[1]
        source_meeting_id = parts[2]
        source_prefix = f"recordings/{source_user_id}/{source_meeting_id}"
    else:
        print(f"  Cannot parse source prefix from: {first_artifact}")
        return 0

    print(f"  Source prefix: {source_prefix}")

    # Get all subscribers
    subs = list(session_ref.collection("subscribers").stream())
    fixed = 0

    for sub in subs:
        sub_data = sub.to_dict()
        user_id = sub.id
        meeting_path = sub_data.get("meeting_path")
        fs_meeting_id = sub_data.get("fs_meeting_id")

        if not meeting_path or not fs_meeting_id:
            print(f"    Subscriber {user_id[:12]}... missing meeting info")
            continue

        # Check if meeting already has artifacts
        meeting_ref = db.document(meeting_path)
        meeting_doc = meeting_ref.get()

        if not meeting_doc.exists:
            print(f"    Meeting doc not found: {meeting_path}")
            continue

        meeting_data = meeting_doc.to_dict()

        if meeting_data.get("artifacts"):
            print(f"    {user_id[:12]}... already has artifacts ✓")
            continue

        # Build artifacts for this subscriber
        dst_prefix = f"recordings/{user_id}/{fs_meeting_id}"
        subscriber_artifacts = {}

        for artifact_key, artifact_path in session_artifacts.items():
            new_path = artifact_path.replace(source_prefix, dst_prefix)
            subscriber_artifacts[artifact_key] = new_path

        if dry_run:
            print(
                f"    Would add {len(subscriber_artifacts)} artifacts to {user_id[:12]}..."
            )
            print(f"      Example: {list(subscriber_artifacts.values())[0]}")
        else:
            meeting_ref.set(
                {
                    "artifacts": subscriber_artifacts,
                    "updated_at": datetime.now(timezone.utc),
                },
                merge=True,
            )
            print(
                f"    Added {len(subscriber_artifacts)} artifacts to {user_id[:12]}... ✓"
            )

        fixed += 1

    return fixed


def main(dry_run: bool = True):
    db = get_db()

    mode = "DRY RUN" if dry_run else "FIXING"
    print("=" * 70)
    print(f"FIX MISSING ARTIFACTS ON MEETING DOCUMENTS ({mode})")
    print("=" * 70)

    # Get completed sessions
    sessions_ref = db.collection(f"organizations/{ORG_ID}/meeting_sessions")
    completed = list(
        sessions_ref.where(filter=FieldFilter("status", "==", "complete"))
        .limit(50)
        .stream()
    )

    print(f"\nFound {len(completed)} completed sessions")

    total_fixed = 0

    for s in completed:
        session_id = s.id
        session_data = s.to_dict()
        title = session_data.get("meeting_title", "Unknown")[:40]

        print(f"\n📎 Session: {session_id[:20]}...")
        print(f"   Title: {title}")

        fixed = fix_session_artifacts(db, session_id, dry_run=dry_run)
        total_fixed += fixed

    print()
    print("=" * 70)
    if dry_run:
        print(f"DRY RUN COMPLETE: Would fix {total_fixed} meeting documents")
        print("\nRun with --apply to actually fix the documents")
    else:
        print(f"COMPLETE: Fixed {total_fixed} meeting documents")
    print("=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fix missing artifacts on meetings")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply the fixes (default is dry run)",
    )

    args = parser.parse_args()
    main(dry_run=not args.apply)
