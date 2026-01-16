#!/usr/bin/env python3
"""
End-to-End Meeting Bot Test Script

This script helps test the meeting bot flow:
1. Creates test meeting entries for both users
2. Creates a session with both as subscribers
3. Monitors the bot job
4. Verifies fanout delivers to both users

Usage:
    python scripts/e2e_test.py --teams-url "https://teams.microsoft.com/..."
"""

import os
import sys
import time
import hashlib
import argparse
from datetime import datetime, timezone, timedelta
from google.cloud import firestore, storage
from google.cloud.firestore_v1.base_query import FieldFilter

# Configuration
PROJECT_ID = "aw-development-7226"
ORG_ID = "advisewell"
BUCKET_NAME = "advisewell-firebase-development"

MATT_USER_ID = "Mw6Awnh1WtN7631PrSYyGXlpozz1"
MATT_EMAIL = "matt@advisewell.co"
CLINTON_USER_ID = "qWsfR283u4Sck6fGF33dvFa3W0r2"
CLINTON_EMAIL = "clinton@advisewell.co"


def get_db():
    """Get Firestore client."""
    creds_path = os.path.expanduser(
        "~/.config/gcloud/application_default_credentials.json"
    )
    if os.path.exists(creds_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    return firestore.Client(project=PROJECT_ID, database="(default)")


def generate_session_id(org_id: str, meeting_url: str) -> str:
    """Generate session ID from org and URL."""
    combined = f"{org_id}:{meeting_url}"
    return hashlib.sha256(combined.encode()).hexdigest()


def find_meetings_by_url(db, url: str):
    """Find all meetings matching a URL."""
    meetings_ref = db.collection(f"organizations/{ORG_ID}/meetings")
    
    # Search by different URL fields
    results = []
    for field in ["join_url", "teams_url", "meeting_url"]:
        try:
            matches = list(
                meetings_ref.where(filter=FieldFilter(field, "==", url))
                .limit(10)
                .stream()
            )
            for m in matches:
                if m.id not in [r["id"] for r in results]:
                    results.append({"id": m.id, "data": m.to_dict()})
        except Exception:
            pass
    
    return results


def check_session(db, session_id: str):
    """Check session status and subscribers."""
    session_ref = db.document(
        f"organizations/{ORG_ID}/meeting_sessions/{session_id}"
    )
    session_doc = session_ref.get()
    
    if not session_doc.exists:
        return None
    
    session_data = session_doc.to_dict()
    
    # Get subscribers
    subs_ref = session_ref.collection("subscribers")
    subscribers = []
    for sub in subs_ref.stream():
        sub_data = sub.to_dict()
        subscribers.append({
            "user_id": sub.id,
            "fs_meeting_id": sub_data.get("fs_meeting_id"),
            "status": sub_data.get("status"),
        })
    
    return {
        "id": session_id,
        "status": session_data.get("status"),
        "fanout_status": session_data.get("fanout_status"),
        "created_at": session_data.get("created_at"),
        "artifacts": session_data.get("artifacts", {}),
        "subscribers": subscribers,
    }


def verify_transcription(db, user_id: str, meeting_id: str):
    """Check if a meeting has transcription."""
    meeting_ref = db.document(
        f"organizations/{ORG_ID}/meetings/{meeting_id}"
    )
    meeting_doc = meeting_ref.get()
    
    if not meeting_doc.exists:
        return {"exists": False}
    
    data = meeting_doc.to_dict()
    return {
        "exists": True,
        "has_transcription": bool(data.get("transcription")),
        "transcription_length": len(data.get("transcription", "")),
        "has_artifacts": bool(data.get("artifacts")),
        "status": data.get("status"),
    }


def monitor_session(db, session_id: str, timeout_minutes: int = 30):
    """Monitor a session until complete or timeout."""
    print(f"\n🔄 Monitoring session {session_id[:16]}...")
    print("   Press Ctrl+C to stop monitoring\n")
    
    start_time = datetime.now(timezone.utc)
    last_status = None
    
    try:
        while True:
            session = check_session(db, session_id)
            
            if not session:
                print("   ❌ Session not found!")
                return False
            
            status = session["status"]
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            if status != last_status:
                print(f"   [{elapsed:.0f}s] Status: {status}")
                print(f"         Subscribers: {len(session['subscribers'])}")
                for sub in session["subscribers"]:
                    print(f"           - {sub['user_id'][:20]}...")
                last_status = status
            
            if status == "complete":
                print(f"\n✅ Session completed!")
                print(f"   Fanout status: {session['fanout_status']}")
                print(f"   Artifacts: {list(session['artifacts'].keys())}")
                return True
            
            if status == "failed":
                print(f"\n❌ Session failed!")
                return False
            
            if elapsed > timeout_minutes * 60:
                print(f"\n⏰ Timeout after {timeout_minutes} minutes")
                return False
            
            time.sleep(10)
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Monitoring stopped by user")
        return None


def run_e2e_test(teams_url: str):
    """Run the end-to-end test."""
    db = get_db()
    
    print("=" * 70)
    print("END-TO-END MEETING BOT TEST")
    print("=" * 70)
    print(f"\nTeams URL: {teams_url[:60]}...")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    # Step 1: Find meetings for this URL
    print("\n" + "=" * 70)
    print("STEP 1: Finding meetings with this URL")
    print("=" * 70)
    
    meetings = find_meetings_by_url(db, teams_url)
    print(f"\nFound {len(meetings)} meeting(s)")
    
    matt_meeting = None
    clinton_meeting = None
    
    for m in meetings:
        user_id = m["data"].get("user_id")
        print(f"\n  Meeting: {m['id']}")
        print(f"    User: {user_id[:20]}...")
        print(f"    Title: {m['data'].get('title', 'Unknown')}")
        print(f"    Session ID: {m['data'].get('meeting_session_id', 'None')}")
        
        if user_id == MATT_USER_ID:
            matt_meeting = m
        elif user_id == CLINTON_USER_ID:
            clinton_meeting = m
    
    # Step 2: Check session
    print("\n" + "=" * 70)
    print("STEP 2: Checking session and subscribers")
    print("=" * 70)
    
    session_id = generate_session_id(ORG_ID, teams_url)
    print(f"\nExpected session ID: {session_id[:20]}...")
    
    session = check_session(db, session_id)
    
    if session:
        print(f"\n✅ Session found!")
        print(f"   Status: {session['status']}")
        print(f"   Fanout: {session['fanout_status']}")
        print(f"   Subscribers: {len(session['subscribers'])}")
        
        has_matt = any(s["user_id"] == MATT_USER_ID for s in session["subscribers"])
        has_clinton = any(
            s["user_id"] == CLINTON_USER_ID for s in session["subscribers"]
        )
        
        print(f"\n   Matt subscribed: {'✅' if has_matt else '❌'}")
        print(f"   Clinton subscribed: {'✅' if has_clinton else '❌'}")
        
        if not has_matt or not has_clinton:
            print("\n⚠️ Not all users are subscribed!")
            print("   This may indicate a consolidation issue.")
    else:
        print("\n❌ Session not found!")
        print("   The meeting may not have been queued for recording yet.")
        return
    
    # Step 3: Monitor if not complete
    if session["status"] not in ["complete", "failed"]:
        print("\n" + "=" * 70)
        print("STEP 3: Monitoring session")
        print("=" * 70)
        
        result = monitor_session(db, session_id)
        
        if result:
            session = check_session(db, session_id)
    
    # Step 4: Verify transcriptions
    print("\n" + "=" * 70)
    print("STEP 4: Verifying transcriptions delivered to both users")
    print("=" * 70)
    
    if matt_meeting:
        matt_result = verify_transcription(
            db, MATT_USER_ID, matt_meeting["id"]
        )
        print(f"\nMatt's meeting ({matt_meeting['id']}):")
        print(f"   Has transcription: {'✅' if matt_result['has_transcription'] else '❌'}")
        if matt_result["has_transcription"]:
            print(f"   Transcription length: {matt_result['transcription_length']} chars")
    
    if clinton_meeting:
        clinton_result = verify_transcription(
            db, CLINTON_USER_ID, clinton_meeting["id"]
        )
        print(f"\nClinton's meeting ({clinton_meeting['id']}):")
        print(f"   Has transcription: {'✅' if clinton_result['has_transcription'] else '❌'}")
        if clinton_result["has_transcription"]:
            print(f"   Transcription length: {clinton_result['transcription_length']} chars")
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    all_good = True
    
    if len(meetings) < 2:
        print("❌ Both users should have the meeting synced")
        all_good = False
    else:
        print("✅ Both users have the meeting synced")
    
    if session and len(session["subscribers"]) >= 2:
        print("✅ Both users are session subscribers")
    else:
        print("❌ Not all users are session subscribers")
        all_good = False
    
    if session and session["status"] == "complete":
        print("✅ Session completed successfully")
    else:
        print("❌ Session did not complete")
        all_good = False
    
    if matt_meeting and clinton_meeting:
        matt_has = verify_transcription(db, MATT_USER_ID, matt_meeting["id"])
        clinton_has = verify_transcription(db, CLINTON_USER_ID, clinton_meeting["id"])
        
        if matt_has["has_transcription"] and clinton_has["has_transcription"]:
            print("✅ Both users received transcriptions (fanout working)")
        else:
            print("❌ Transcription fanout failed")
            all_good = False
    
    print()
    if all_good:
        print("🎉 ALL TESTS PASSED!")
    else:
        print("⚠️ SOME TESTS FAILED - See details above")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2E Meeting Bot Test")
    parser.add_argument(
        "--teams-url",
        required=True,
        help="The Teams meeting URL to test",
    )
    parser.add_argument(
        "--monitor-only",
        action="store_true",
        help="Only monitor, don't run full test",
    )
    
    args = parser.parse_args()
    
    run_e2e_test(args.teams_url)
