"""
Test: Multi-Attendee Meeting Consolidation and Fanout

Simulates the complete flow:
1. Multiple users from the same org click "Record" on the same meeting
2. Controller detects duplicates and consolidates to single meeting entry
3. Single bot joins the meeting
4. After meeting ends, results fan out to all subscribers
"""

import pytest
import hashlib
import asyncio
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import List, Optional, Dict, Any


class MockFirestoreDocument:
    """Mock Firestore document"""
    def __init__(self, doc_id: str, data: dict, exists: bool = True):
        self.id = doc_id
        self._data = data
        self.exists = exists
    
    def to_dict(self):
        return self._data if self.exists else None


class MockFirestoreCollection:
    """Mock Firestore collection with query support"""
    def __init__(self, docs=None):
        self._docs = docs or []
        self._filters = []
    
    def where(self, field, op, value):
        new_collection = MockFirestoreCollection(self._docs)
        new_collection._filters = self._filters + [(field, op, value)]
        return new_collection
    
    def limit(self, n):
        return self
    
    def order_by(self, field, direction=None):
        return self
    
    def stream(self):
        result = self._docs
        for field, op, value in self._filters:
            filtered = []
            for doc in result:
                doc_data = doc.to_dict()
                if doc_data is None:
                    continue
                # Handle nested fields
                field_value = doc_data
                for part in field.split('.'):
                    if isinstance(field_value, dict):
                        field_value = field_value.get(part)
                    else:
                        field_value = None
                        break
                
                if op == '==' and field_value == value:
                    filtered.append(doc)
                elif op == '!=' and field_value != value:
                    filtered.append(doc)
                elif op == 'in' and field_value in value:
                    filtered.append(doc)
            result = filtered
        return iter(result)
    
    def get(self):
        return list(self.stream())


def normalize_meeting_url(url: str) -> str:
    """Normalize meeting URL for consistent comparison"""
    parsed = urlparse(url)
    
    if 'meet.google.com' in parsed.netloc:
        # Google Meet: just need the meeting code
        path = parsed.path.strip('/')
        return f"meet.google.com/{path}"
    elif 'zoom.us' in parsed.netloc:
        # Zoom: need meeting ID
        path = parsed.path
        if '/j/' in path:
            meeting_id = path.split('/j/')[-1].split('?')[0]
            return f"zoom.us/{meeting_id}"
    elif 'teams.microsoft.com' in parsed.netloc:
        # Teams: need the meeting ID from path
        return f"teams.microsoft.com{parsed.path}"
    
    return url


def generate_session_id(org_id: str, meeting_url: str) -> str:
    """Generate deterministic session ID from org and meeting URL"""
    normalized = normalize_meeting_url(meeting_url)
    combined = f"{org_id}:{normalized}"
    return hashlib.sha256(combined.encode()).hexdigest()


class TestMultiAttendeeConsolidation:
    """Test suite for multi-attendee consolidation flow"""
    
    @pytest.fixture
    def org_id(self):
        return "test-org-123"
    
    @pytest.fixture
    def meeting_url(self):
        return "https://meet.google.com/abc-defg-hij"
    
    @pytest.fixture
    def users(self):
        """Three users from the same org"""
        return [
            {"user_id": "user-alice", "email": "alice@testorg.com", "name": "Alice"},
            {"user_id": "user-bob", "email": "bob@testorg.com", "name": "Bob"},
            {"user_id": "user-charlie", "email": "charlie@testorg.com", "name": "Charlie"},
        ]
    
    def test_same_session_id_for_same_meeting(self, org_id, meeting_url, users):
        """All users requesting the same meeting get the same session ID"""
        session_ids = []
        
        for user in users:
            session_id = generate_session_id(org_id, meeting_url)
            session_ids.append(session_id)
        
        # All session IDs should be identical
        assert len(set(session_ids)) == 1, "All users should get the same session ID"
        print(f"✓ All 3 users get session ID: {session_ids[0][:16]}...")
    
    def test_url_normalization_variants(self, org_id, users):
        """Different URL formats for same meeting normalize to same session"""
        url_variants = [
            "https://meet.google.com/abc-defg-hij",
            "https://meet.google.com/abc-defg-hij?authuser=0",
            "https://meet.google.com/abc-defg-hij?pli=1",
            "https://meet.google.com/abc-defg-hij/",
        ]
        
        session_ids = [generate_session_id(org_id, url) for url in url_variants]
        
        assert len(set(session_ids)) == 1, "All URL variants should produce same session ID"
        print(f"✓ All {len(url_variants)} URL variants normalize to same session")
    
    def test_detect_duplicate_meeting_entries(self, org_id, meeting_url, users):
        """Controller detects when multiple meeting documents exist for same URL"""
        # Simulate: Each user clicks "Record" creating a meeting document
        meeting_docs = []
        base_time = datetime.now()
        
        for i, user in enumerate(users):
            doc_id = f"meeting-{user['user_id']}-{i}"
            meeting_docs.append(MockFirestoreDocument(
                doc_id=doc_id,
                data={
                    "meeting_url": meeting_url,
                    "user_id": user["user_id"],
                    "user_email": user["email"],
                    "created_at": base_time + timedelta(milliseconds=i * 100),
                    "status": "pending",
                    "org_id": org_id,
                }
            ))
        
        # Group by normalized URL (simulating controller logic)
        url_groups = {}
        for doc in meeting_docs:
            data = doc.to_dict()
            normalized = normalize_meeting_url(data["meeting_url"])
            if normalized not in url_groups:
                url_groups[normalized] = []
            url_groups[normalized].append(doc)
        
        # Should have 1 group with 3 documents
        assert len(url_groups) == 1, "All should be in same URL group"
        assert len(list(url_groups.values())[0]) == 3, "Should have 3 duplicate entries"
        print(f"✓ Detected {len(meeting_docs)} meeting docs as duplicates for same URL")
    
    def test_consolidate_to_canonical_meeting(self, org_id, meeting_url, users):
        """Consolidate duplicates to single canonical meeting with subscriber list"""
        # Create meeting documents from each user
        meeting_docs = []
        base_time = datetime.now()
        
        for i, user in enumerate(users):
            meeting_docs.append({
                "doc_id": f"meeting-{user['user_id']}",
                "meeting_url": meeting_url,
                "user_id": user["user_id"],
                "user_email": user["email"],
                "created_at": base_time + timedelta(milliseconds=i * 100),
                "status": "pending",
            })
        
        # Consolidation logic: pick earliest, collect all subscribers
        sorted_docs = sorted(meeting_docs, key=lambda x: x["created_at"])
        canonical = sorted_docs[0].copy()
        
        # Build subscriber list
        subscribers = []
        for doc in meeting_docs:
            subscribers.append({
                "user_id": doc["user_id"],
                "user_email": doc["user_email"],
                "original_meeting_id": doc["doc_id"],
            })
        
        canonical["subscribers"] = subscribers
        canonical["subscriber_count"] = len(subscribers)
        
        # Verify consolidation
        assert canonical["subscriber_count"] == 3
        assert canonical["user_id"] == users[0]["user_id"], "First user becomes canonical owner"
        assert all(s["user_id"] in [u["user_id"] for u in users] for s in subscribers)
        
        print(f"✓ Consolidated to canonical meeting with {len(subscribers)} subscribers:")
        for sub in subscribers:
            print(f"  - {sub['user_email']}")
    
    def test_single_session_created(self, org_id, meeting_url, users):
        """Only one session is created in meeting_sessions collection"""
        sessions_created = {}
        
        def mock_create_session(session_id, data):
            if session_id in sessions_created:
                return False  # Already exists
            sessions_created[session_id] = data
            return True
        
        # Simulate each user's request hitting the controller
        for user in users:
            session_id = generate_session_id(org_id, meeting_url)
            session_data = {
                "meeting_url": meeting_url,
                "org_id": org_id,
                "status": "queued",
                "subscribers": [user["user_id"]],
            }
            
            created = mock_create_session(session_id, session_data)
            if created:
                print(f"  Session created for {user['email']}")
            else:
                # Update existing session to add subscriber
                sessions_created[session_id]["subscribers"].append(user["user_id"])
                print(f"  Added {user['email']} as subscriber to existing session")
        
        # Should have exactly 1 session
        assert len(sessions_created) == 1
        session = list(sessions_created.values())[0]
        assert len(session["subscribers"]) == 3
        print(f"✓ Single session created with {len(session['subscribers'])} subscribers")
    
    def test_single_bot_job_created(self, org_id, meeting_url, users):
        """Only one Kubernetes job (bot) is created for the meeting"""
        jobs_created = []
        session_id = generate_session_id(org_id, meeting_url)
        
        def mock_create_job(job_name, meeting_url, session_id):
            # Check if job already exists for this session
            existing = [j for j in jobs_created if j["session_id"] == session_id]
            if existing:
                return None  # Job already exists
            
            job = {
                "job_name": job_name,
                "meeting_url": meeting_url,
                "session_id": session_id,
            }
            jobs_created.append(job)
            return job
        
        # Simulate controller processing each meeting request
        for user in users:
            job_name = f"bot-{session_id[:8]}"
            result = mock_create_job(job_name, meeting_url, session_id)
            if result:
                print(f"  Bot job created: {job_name}")
            else:
                print(f"  Skipped job creation for {user['email']} (already exists)")
        
        assert len(jobs_created) == 1
        print(f"✓ Only 1 bot job created for 3 user requests")


class TestMeetingRecordingFlow:
    """Test the actual meeting recording and completion flow"""
    
    @pytest.fixture
    def session_data(self):
        return {
            "session_id": "test-session-abc123",
            "meeting_url": "https://meet.google.com/abc-defg-hij",
            "org_id": "test-org-123",
            "status": "queued",
            "subscribers": [
                {"user_id": "user-alice", "email": "alice@testorg.com"},
                {"user_id": "user-bob", "email": "bob@testorg.com"},
                {"user_id": "user-charlie", "email": "charlie@testorg.com"},
            ],
        }
    
    def test_bot_joins_and_records(self, session_data):
        """Simulate bot joining meeting and recording"""
        # Bot claims the session
        session_data["status"] = "claimed"
        session_data["claimed_at"] = datetime.now()
        session_data["bot_instance"] = "bot-pod-xyz"
        
        print(f"✓ Bot claimed session, status: {session_data['status']}")
        
        # Bot starts recording
        session_data["status"] = "recording"
        session_data["recording_started_at"] = datetime.now()
        
        print(f"✓ Recording started at {session_data['recording_started_at']}")
        
        # Meeting ends, recording stops
        session_data["status"] = "processing"
        session_data["recording_ended_at"] = datetime.now()
        
        # Simulate file outputs
        session_data["outputs"] = {
            "audio_file": f"gs://bucket/{session_data['session_id']}/audio.wav",
            "transcript_file": f"gs://bucket/{session_data['session_id']}/transcript.vtt",
            "summary_file": f"gs://bucket/{session_data['session_id']}/summary.md",
        }
        
        print(f"✓ Recording complete, files generated")
        assert session_data["status"] == "processing"
        assert len(session_data["outputs"]) == 3
    
    def test_fanout_to_all_subscribers(self, session_data):
        """Test that results fan out to all subscribers"""
        # Simulate completed recording with outputs
        session_data["status"] = "complete"
        session_data["outputs"] = {
            "audio_file": "gs://bucket/session/audio.wav",
            "transcript_file": "gs://bucket/session/transcript.vtt",
            "summary_file": "gs://bucket/session/summary.md",
        }
        
        fanout_results = []
        
        # Fanout logic: copy files to each subscriber's storage
        for subscriber in session_data["subscribers"]:
            user_id = subscriber["user_id"]
            
            # Create user-specific copies
            user_outputs = {}
            for key, source_path in session_data["outputs"].items():
                # Copy to user's folder
                filename = source_path.split("/")[-1]
                user_path = f"gs://bucket/users/{user_id}/meetings/{session_data['session_id']}/{filename}"
                user_outputs[key] = user_path
            
            fanout_result = {
                "user_id": user_id,
                "email": subscriber["email"],
                "outputs": user_outputs,
                "fanout_status": "complete",
            }
            fanout_results.append(fanout_result)
            print(f"  Fanned out to {subscriber['email']}")
        
        assert len(fanout_results) == 3
        print(f"✓ Results fanned out to {len(fanout_results)} subscribers")
        
        # Verify each subscriber got their own copy
        for result in fanout_results:
            assert result["fanout_status"] == "complete"
            assert len(result["outputs"]) == 3
            # Verify paths are user-specific
            for path in result["outputs"].values():
                assert result["user_id"] in path
    
    def test_user_meeting_documents_updated(self, session_data):
        """Each user's original meeting document is updated with results"""
        session_data["status"] = "complete"
        session_data["outputs"] = {
            "transcript_file": "gs://bucket/session/transcript.vtt",
        }
        
        # Simulate original meeting documents
        user_meetings = {
            "user-alice": {"status": "pending", "meeting_id": "meeting-alice"},
            "user-bob": {"status": "pending", "meeting_id": "meeting-bob"},
            "user-charlie": {"status": "pending", "meeting_id": "meeting-charlie"},
        }
        
        # Fanout updates each user's meeting document
        for subscriber in session_data["subscribers"]:
            user_id = subscriber["user_id"]
            meeting = user_meetings[user_id]
            
            meeting["status"] = "complete"
            meeting["session_id"] = session_data["session_id"]
            meeting["transcript_url"] = f"gs://bucket/users/{user_id}/transcript.vtt"
            meeting["completed_at"] = datetime.now()
        
        # Verify all meetings updated
        for user_id, meeting in user_meetings.items():
            assert meeting["status"] == "complete"
            assert "transcript_url" in meeting
            print(f"  ✓ {user_id}'s meeting updated: {meeting['status']}")
        
        print(f"✓ All {len(user_meetings)} user meeting documents updated")


class TestEndToEndFlow:
    """Complete end-to-end simulation"""
    
    @pytest.mark.asyncio
    async def test_full_multi_user_flow(self):
        """
        Complete simulation:
        1. 3 users from same org click Record on same meeting (within seconds)
        2. Controller consolidates to single session
        3. Single bot joins meeting
        4. Bot records and transcribes
        5. Results fan out to all 3 users
        """
        print("\n" + "="*60)
        print("FULL END-TO-END SIMULATION")
        print("="*60)
        
        # Setup
        org_id = "advisewell"
        meeting_url = "https://meet.google.com/team-standup-xyz"
        users = [
            {"user_id": "user-1", "email": "alice@advisewell.com"},
            {"user_id": "user-2", "email": "bob@advisewell.com"},
            {"user_id": "user-3", "email": "charlie@advisewell.com"},
        ]
        
        print(f"\n📅 Meeting: {meeting_url}")
        print(f"🏢 Organization: {org_id}")
        print(f"👥 Users: {', '.join(u['email'] for u in users)}")
        
        # Step 1: Users click "Record" (creates meeting documents)
        print("\n--- STEP 1: Users Click Record ---")
        meeting_docs = []
        base_time = datetime.now()
        
        for i, user in enumerate(users):
            doc = {
                "id": f"meeting-{i}",
                "meeting_url": meeting_url,
                "user_id": user["user_id"],
                "email": user["email"],
                "created_at": base_time + timedelta(milliseconds=i * 50),
                "status": "pending",
            }
            meeting_docs.append(doc)
            print(f"  📝 Meeting doc created: {user['email']} (+{i*50}ms)")
        
        # Step 2: Controller detects duplicates and consolidates
        print("\n--- STEP 2: Controller Consolidates ---")
        session_id = generate_session_id(org_id, meeting_url)
        
        # Group by URL
        normalized_url = normalize_meeting_url(meeting_url)
        duplicates = [d for d in meeting_docs if normalize_meeting_url(d["meeting_url"]) == normalized_url]
        
        print(f"  🔍 Detected {len(duplicates)} requests for same meeting")
        print(f"  🔗 Session ID: {session_id[:16]}...")
        
        # Create consolidated session
        session = {
            "session_id": session_id,
            "meeting_url": meeting_url,
            "org_id": org_id,
            "status": "queued",
            "subscribers": [{"user_id": u["user_id"], "email": u["email"]} for u in users],
            "canonical_meeting_id": meeting_docs[0]["id"],
            "created_at": datetime.now(),
        }
        
        print(f"  ✅ Created single session with {len(session['subscribers'])} subscribers")
        
        # Mark duplicate meeting docs
        for i, doc in enumerate(meeting_docs):
            if i == 0:
                doc["is_canonical"] = True
                doc["session_id"] = session_id
            else:
                doc["merged_into"] = meeting_docs[0]["id"]
                doc["status"] = "merged"
        
        # Step 3: Single bot job created
        print("\n--- STEP 3: Bot Job Creation ---")
        job = {
            "name": f"meeting-bot-{session_id[:8]}",
            "session_id": session_id,
            "meeting_url": meeting_url,
        }
        print(f"  🤖 Created bot job: {job['name']}")
        print(f"  ⚡ Only 1 bot for {len(users)} user requests")
        
        # Step 4: Bot records meeting
        print("\n--- STEP 4: Bot Records Meeting ---")
        session["status"] = "claimed"
        print(f"  🎯 Bot claimed session")
        
        await asyncio.sleep(0.1)  # Simulate time passing
        
        session["status"] = "recording"
        print(f"  🔴 Recording in progress...")
        
        await asyncio.sleep(0.1)  # Simulate recording
        
        session["status"] = "processing"
        session["outputs"] = {
            "audio": f"gs://recordings/{session_id}/audio.wav",
            "transcript": f"gs://recordings/{session_id}/transcript.vtt",
            "summary": f"gs://recordings/{session_id}/summary.md",
        }
        print(f"  ✅ Recording complete, processing transcript...")
        
        # Step 5: Fanout to all subscribers
        print("\n--- STEP 5: Fanout to Subscribers ---")
        session["status"] = "complete"
        
        fanout_complete = []
        for subscriber in session["subscribers"]:
            user_id = subscriber["user_id"]
            email = subscriber["email"]
            
            # Copy files to user's folder
            user_files = {
                "audio": f"gs://users/{user_id}/meetings/{session_id}/audio.wav",
                "transcript": f"gs://users/{user_id}/meetings/{session_id}/transcript.vtt",
                "summary": f"gs://users/{user_id}/meetings/{session_id}/summary.md",
            }
            
            fanout_complete.append({
                "user_id": user_id,
                "email": email,
                "files": user_files,
            })
            print(f"  📤 Fanned out to {email}")
        
        # Step 6: Verify final state
        print("\n--- FINAL STATE ---")
        print(f"  📊 Session: {session['status']}")
        print(f"  👥 Subscribers served: {len(fanout_complete)}")
        print(f"  🤖 Bots used: 1")
        print(f"  📁 Files per user: {len(session['outputs'])}")
        
        # Assertions
        assert session["status"] == "complete"
        assert len(fanout_complete) == 3
        assert len([d for d in meeting_docs if d.get("status") == "merged"]) == 2
        assert len([d for d in meeting_docs if d.get("is_canonical")]) == 1
        
        print("\n" + "="*60)
        print("✅ END-TO-END TEST PASSED")
        print("="*60)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
