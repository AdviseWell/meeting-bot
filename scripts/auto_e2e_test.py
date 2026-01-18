#!/usr/bin/env python3
"""
Automated E2E Meeting Bot Test

For each meeting with both Matt and Clinton:
1. Shows the Teams URL so you can join
2. Queues Matt's meeting for recording (simulating schedule trigger)
3. Checks if Clinton's meeting gets consolidated (same session, added as subscriber)
4. Waits for you to press Enter before moving to the next meeting

The controller should:
- Create a session for the first queued meeting
- Consolidate Clinton's duplicate meeting into the same session
- Add Clinton as a subscriber
"""

import os
import sys
import time
import hashlib
from datetime import datetime, timezone, timedelta
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

PROJECT_ID = "aw-development-7226"
ORG_ID = "advisewell"
BUCKET_NAME = "advisewell-firebase-development"

MATT_USER_ID = "Mw6Awnh1WtN7631PrSYyGXlpozz1"
MATT_EMAIL = "matt@advisewell.co"
CLINTON_USER_ID = "qWsfR283u4Sck6fGF33dvFa3W0r2"
CLINTON_EMAIL = "clinton@advisewell.co"


def get_db():
    creds_path = os.path.expanduser(
        "~/.config/gcloud/application_default_credentials.json"
    )
    if os.path.exists(creds_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    return firestore.Client(project=PROJECT_ID, database="(default)")


def generate_session_id(org_id: str, meeting_url: str) -> str:
    combined = f"{org_id}:{meeting_url}"
    return hashlib.sha256(combined.encode()).hexdigest()


def get_shared_meetings(db):
    """Get all meetings with both users (any status)."""
    meetings_ref = db.collection(f"organizations/{ORG_ID}/meetings")

    # Get all of Matt's meetings (not filtering by status)
    matt_meetings = list(
        meetings_ref.where(filter=FieldFilter("user_id", "==", MATT_USER_ID))
        .limit(100)
        .stream()
    )

    shared = []
    for m in matt_meetings:
        data = m.to_dict()
        attendees = data.get("attendees", [])

        attendee_emails = []
        for a in attendees:
            if isinstance(a, dict):
                attendee_emails.append(a.get("email", "").lower())
            elif isinstance(a, str):
                attendee_emails.append(a.lower())

        if CLINTON_EMAIL not in attendee_emails:
            continue

        teams_url = data.get("teams_url") or data.get("join_url") or ""
        if not teams_url:
            continue

        # Find Clinton's matching meeting
        clinton_meeting = find_clinton_meeting(db, teams_url)

        shared.append(
            {
                "matt_meeting_id": m.id,
                "clinton_meeting_id": (
                    clinton_meeting["id"] if clinton_meeting else None
                ),
                "title": data.get("title", "Unknown"),
                "teams_url": teams_url,
            }
        )

    return shared


def find_clinton_meeting(db, teams_url: str):
    """Find Clinton's meeting with the same Teams URL."""
    meetings_ref = db.collection(f"organizations/{ORG_ID}/meetings")

    # Try teams_url field
    matches = list(
        meetings_ref.where(filter=FieldFilter("user_id", "==", CLINTON_USER_ID))
        .where(filter=FieldFilter("teams_url", "==", teams_url))
        .limit(1)
        .stream()
    )

    if matches:
        return {"id": matches[0].id, "data": matches[0].to_dict()}

    # Try join_url field
    matches = list(
        meetings_ref.where(filter=FieldFilter("user_id", "==", CLINTON_USER_ID))
        .where(filter=FieldFilter("join_url", "==", teams_url))
        .limit(1)
        .stream()
    )

    if matches:
        return {"id": matches[0].id, "data": matches[0].to_dict()}

    return None


def queue_meeting_for_recording(db, meeting_id: str, user_name: str):
    """Queue a meeting by setting status to 'queued' and start to soon."""
    meeting_ref = db.document(f"organizations/{ORG_ID}/meetings/{meeting_id}")

    # Set 'start' to 2 minutes in the future to match controller's scan window
    # Controller scans for meetings with 'start' field 1.5-2.5 minutes from now
    start_time = datetime.now(timezone.utc) + timedelta(minutes=2)

    meeting_ref.set(
        {
            "status": "queued",
            "start": start_time,  # Controller queries by 'start' not 'start_time'
            "queued_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        },
        merge=True,
    )
    print(f"   ✅ Queued {user_name}'s meeting: {meeting_id}")
    print(f"      Start set to: {start_time.strftime('%H:%M:%S')} UTC")


def check_session(db, session_id: str):
    """Check session status and subscribers."""
    session_ref = db.document(f"organizations/{ORG_ID}/meeting_sessions/{session_id}")
    session_doc = session_ref.get()

    if not session_doc.exists:
        return None

    data = session_doc.to_dict()
    subs = list(session_ref.collection("subscribers").stream())
    sub_info = {}
    for s in subs:
        sub_data = s.to_dict()
        sub_info[s.id] = {
            "fs_meeting_id": sub_data.get("fs_meeting_id"),
            "status": sub_data.get("status"),
            "added_via": sub_data.get("added_via"),
        }

    return {
        "status": data.get("status"),
        "fanout_status": data.get("fanout_status"),
        "subscribers": sub_info,
        "has_matt": MATT_USER_ID in sub_info,
        "has_clinton": CLINTON_USER_ID in sub_info,
    }


def verify_meeting(db, meeting_id: str):
    """Check meeting transcription and artifacts."""
    meeting_ref = db.document(f"organizations/{ORG_ID}/meetings/{meeting_id}")
    doc = meeting_ref.get()

    if not doc.exists:
        return None

    data = doc.to_dict()
    return {
        "has_transcription": bool(data.get("transcription")),
        "transcription_len": len(data.get("transcription", "")),
        "has_artifacts": bool(data.get("artifacts")),
        "artifact_count": len(data.get("artifacts", {})),
        "has_recording_url": bool(data.get("recording_url")),
    }


def show_kubernetes_logs(session_id: str):
    """Fetch and display kubernetes logs from controller and manager pods."""
    import subprocess

    # Short session ID for log filtering
    short_session = session_id[:24]

    print()
    print("-" * 80)
    print("🔍 CONTROLLER LOGS (last 100 lines):")
    print("-" * 80)
    try:
        result = subprocess.run(
            [
                "kubectl",
                "logs",
                "-l",
                "app=meeting-bot-controller",
                "--tail=100",
                "--all-containers=true",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.stdout:
            # Filter for relevant lines
            lines = result.stdout.split("\n")
            relevant = [
                l
                for l in lines
                if short_session in l
                or "error" in l.lower()
                or "exception" in l.lower()
                or "fanout" in l.lower()
                or "artifact" in l.lower()
            ]
            if relevant:
                print("\n".join(relevant[-50:]))  # Last 50 relevant lines
            else:
                print("(No lines matching session ID or error keywords)")
                print("Full log tail:")
                print("\n".join(lines[-30:]))
        if result.stderr:
            print(f"stderr: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("(Timeout fetching controller logs)")
    except Exception as e:
        print(f"(Error fetching controller logs: {e})")

    print()
    print("-" * 80)
    print("🔍 MANAGER LOGS (last 100 lines):")
    print("-" * 80)
    try:
        result = subprocess.run(
            [
                "kubectl",
                "logs",
                "-l",
                "app=meeting-bot-manager",
                "--tail=100",
                "--all-containers=true",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.stdout:
            lines = result.stdout.split("\n")
            relevant = [
                l
                for l in lines
                if short_session in l
                or "error" in l.lower()
                or "exception" in l.lower()
                or "fanout" in l.lower()
                or "artifact" in l.lower()
            ]
            if relevant:
                print("\n".join(relevant[-50:]))
            else:
                print("(No lines matching session ID or error keywords)")
                print("Full log tail:")
                print("\n".join(lines[-30:]))
        if result.stderr:
            print(f"stderr: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("(Timeout fetching manager logs)")
    except Exception as e:
        print(f"(Error fetching manager logs: {e})")

    print()
    print("-" * 80)


def run_test(db, meeting: dict, index: int, total: int):
    """Run test for a single meeting."""
    print()
    print("=" * 80)
    print(f"TEST {index}/{total}: {meeting['title']}")
    print("=" * 80)
    print()

    # Show the Teams URL prominently
    print("📋 TEAMS MEETING URL (join this in your browser):")
    print()
    print(f"   {meeting['teams_url']}")
    print()
    print("-" * 80)

    session_id = generate_session_id(ORG_ID, meeting["teams_url"])
    print(f"Session ID: {session_id[:24]}...")
    print(f"Matt's meeting: {meeting['matt_meeting_id']}")
    print(f"Clinton's meeting: {meeting['clinton_meeting_id'] or 'NOT FOUND'}")
    print()

    # Check if session already exists
    session = check_session(db, session_id)
    if session:
        print(f"⚠️  Session already exists with status: {session['status']}")
        if session["status"] == "complete":
            print("   Session already completed - will retest anyway")
    print()

    # Queue Matt's meeting
    print("🚀 STEP 1: Queueing Matt's meeting...")
    queue_meeting_for_recording(db, meeting["matt_meeting_id"], "Matt")

    # Queue Clinton's meeting too (to test consolidation)
    if meeting["clinton_meeting_id"]:
        print()
        print("🚀 STEP 2: Queueing Clinton's meeting...")
        queue_meeting_for_recording(db, meeting["clinton_meeting_id"], "Clinton")
    else:
        print()
        print("⚠️  Clinton's meeting not found - only Matt will be tested")

    print()
    print("-" * 80)
    print("🎯 EXPECTED BEHAVIOR:")
    print("   1. Controller creates ONE session for both meetings")
    print("   2. Both Matt and Clinton are added as subscribers")
    print("   3. Bot joins and records the meeting")
    print("   4. Fanout copies transcription/artifacts to BOTH users")
    print("-" * 80)
    print()

    # Wait a moment for controller to process
    print("⏳ Waiting 5 seconds for controller to process...")
    time.sleep(5)

    # Check session status
    print()
    print("📊 SESSION STATUS:")
    session = check_session(db, session_id)
    if session:
        matt_icon = "✅" if session["has_matt"] else "❌"
        clinton_icon = "✅" if session["has_clinton"] else "❌"
        print(f"   Status: {session['status']}")
        print(f"   Matt subscribed: {matt_icon}")
        print(f"   Clinton subscribed: {clinton_icon}")

        if session["has_clinton"]:
            clinton_info = session["subscribers"].get(CLINTON_USER_ID, {})
            added_via = clinton_info.get("added_via", "unknown")
            print(f"   Clinton added via: {added_via}")

            if added_via == "merge_consolidation":
                print()
                print("   ✅ CONSOLIDATION WORKING! Clinton was merged correctly.")
    else:
        print("   ❌ Session not created yet")
        print("   (Controller may need more time, or there's an issue)")

    print()
    print("=" * 80)
    print("👆 JOIN THE MEETING NOW using the URL above")
    print("   The bot should join shortly after you do.")
    print("   Record some audio, then end the meeting.")
    print("=" * 80)

    print()
    print("Options:")
    print("  [Enter] - Done, check results and continue to next meeting")
    print("  [s]     - Skip to next meeting without checking")
    print("  [q]     - Quit the script")
    print()
    choice = input("Choice: ").strip().lower()

    if choice == "q":
        print("\nQuitting...")
        sys.exit(0)

    if choice == "s":
        print("\nSkipping to next meeting...")
        return

    # Final verification
    print()
    print("📊 FINAL VERIFICATION:")
    session = check_session(db, session_id)

    has_error = False
    if session:
        print(f"   Session status: {session['status']}")
        print(f"   Fanout status: {session['fanout_status']}")

        for user_id, info in session["subscribers"].items():
            if user_id == MATT_USER_ID:
                name = "Matt"
            elif user_id == CLINTON_USER_ID:
                name = "Clinton"
            else:
                name = user_id[:12]

            meeting_id = info.get("fs_meeting_id")
            if meeting_id:
                result = verify_meeting(db, meeting_id)
                if result:
                    trans = "✅" if result["has_transcription"] else "❌"
                    arts = "✅" if result["has_artifacts"] else "❌"
                    print(f"\n   {name}:")
                    print(f"      Transcription: {trans}", end="")
                    if result["has_transcription"]:
                        print(f" ({result['transcription_len']} chars)")
                    else:
                        print()
                        has_error = True
                    print(f"      Artifacts: {arts}", end="")
                    if result["has_artifacts"]:
                        print(f" ({result['artifact_count']} files)")
                    else:
                        print()
                        has_error = True
    else:
        print("   ❌ No session found")
        has_error = True

    # If there was an error, show kubernetes logs and stop
    if has_error:
        print()
        print("=" * 80)
        print("❌ ERROR DETECTED - Fetching kubernetes logs for diagnosis...")
        print("=" * 80)
        show_kubernetes_logs(session_id)
        print()
        print("Script stopped due to error. Fix the issue and re-run.")
        sys.exit(1)


def main():
    db = get_db()

    print("=" * 80)
    print("AUTOMATED E2E MEETING BOT TEST")
    print("=" * 80)
    print()
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print()
    print("This script will:")
    print("  1. Find all scheduled meetings with both Matt and Clinton")
    print("  2. Queue each meeting for recording")
    print("  3. Show you the Teams URL to join")
    print("  4. Verify consolidation and fanout work correctly")
    print()

    meetings = get_shared_meetings(db)
    print(f"Found {len(meetings)} meetings to test")

    if not meetings:
        print("\nNo meetings to test!")
        print("Make sure there are scheduled meetings with both users as attendees.")
        return

    print()
    input("Press Enter to start testing...")

    for i, meeting in enumerate(meetings, 1):
        run_test(db, meeting, i, len(meetings))

    print()
    print("=" * 80)
    print("ALL TESTS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
        sys.exit(0)
