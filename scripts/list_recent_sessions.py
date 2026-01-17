#!/usr/bin/env python3
"""
List all recent meeting sessions to help find the AU Standup meeting.
"""

import argparse
import sys
from datetime import datetime, timezone, timedelta
from google.cloud import firestore


def list_recent_sessions(project_id, firestore_db="(default)", days=2):
    """List all meeting sessions from the last N days."""

    db = firestore.Client(project=project_id, database=firestore_db)

    print("=" * 80)
    print(f"LISTING ALL MEETING SESSIONS FROM LAST {days} DAYS")
    print("=" * 80)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    all_sessions = []

    for org_doc in db.collection("organizations").stream():
        org_id = org_doc.id
        org_data = org_doc.to_dict() or {}
        org_name = org_data.get("name", org_id)

        sessions_ref = (
            db.collection("organizations")
            .document(org_id)
            .collection("meeting_sessions")
        )

        try:
            sessions = list(sessions_ref.stream())

            for session in sessions:
                session_data = session.to_dict() or {}
                created_at = session_data.get("created_at")

                # Check if recent
                if created_at:
                    if hasattr(created_at, "timestamp"):
                        session_dt = datetime.fromtimestamp(
                            created_at.timestamp(), tz=timezone.utc
                        )
                    else:
                        continue

                    if session_dt < cutoff:
                        continue

                all_sessions.append(
                    {
                        "org_id": org_id,
                        "org_name": org_name,
                        "session_id": session.id,
                        "session_data": session_data,
                        "created_at": created_at,
                    }
                )

        except Exception as e:
            print(f"Error querying org {org_name}: {e}")

    # Sort by created_at descending
    all_sessions.sort(
        key=lambda x: (
            x["created_at"].timestamp() if hasattr(x["created_at"], "timestamp") else 0
        ),
        reverse=True,
    )

    print(f"\nFound {len(all_sessions)} session(s)\n")
    print("=" * 80)

    for idx, item in enumerate(all_sessions, 1):
        session_data = item["session_data"]
        created_at = item["created_at"]

        if hasattr(created_at, "timestamp"):
            dt = datetime.fromtimestamp(created_at.timestamp(), tz=timezone.utc)
            time_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            time_str = str(created_at)

        status = session_data.get("status", "unknown")
        fanout_status = session_data.get("fanout_status", "not set")
        meeting_url = session_data.get("meeting_url", "")
        org_id_field = session_data.get("org_id", "MISSING")

        # Try to get meeting name from first subscriber
        meeting_name = "Unknown"
        session_ref = (
            db.collection("organizations")
            .document(item["org_id"])
            .collection("meeting_sessions")
            .document(item["session_id"])
        )

        subscribers = list(session_ref.collection("subscribers").limit(1).stream())
        if subscribers:
            sub_data = subscribers[0].to_dict() or {}
            meeting_path = sub_data.get("meeting_path", "")
            if meeting_path:
                try:
                    meeting_doc = db.document(meeting_path).get()
                    if meeting_doc.exists:
                        meeting_data = meeting_doc.to_dict() or {}
                        meeting_name = meeting_data.get("name", "Unknown")
                except Exception:
                    pass

        # Count subscribers
        subscribers_count = len(list(session_ref.collection("subscribers").stream()))

        print(f"\n#{idx}: {meeting_name}")
        print(f"  Session ID: {item['session_id']}")
        print(f"  Org: {item['org_name']} ({item['org_id']})")
        print(f"  Created: {time_str}")
        print(f"  Status: {status}")
        print(f"  Fanout Status: {fanout_status}")
        print(f"  org_id field: {org_id_field}")
        print(f"  Subscribers: {subscribers_count}")
        print(f"  URL: {meeting_url[:80] if meeting_url else 'N/A'}")

        # Highlight potential issues
        if status == "complete" and fanout_status != "complete":
            if org_id_field == "MISSING":
                print(f"  ⚠️  ISSUE: Missing org_id (fanout will skip)")
            elif subscribers_count > 1:
                print(
                    f"  ⚠️  ISSUE: Fanout not complete with {subscribers_count} subscribers!"
                )

        print("-" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List all recent meeting sessions")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--firestore-database", default="(default)", help="Firestore database name"
    )
    parser.add_argument(
        "--days", type=int, default=2, help="Number of days to look back"
    )

    args = parser.parse_args()

    try:
        list_recent_sessions(
            project_id=args.project,
            firestore_db=args.firestore_database,
            days=args.days,
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
