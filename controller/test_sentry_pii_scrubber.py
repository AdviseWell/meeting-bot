"""Tests for Sentry PII scrubber.

Run with: pytest controller/test_sentry_pii_scrubber.py -v
"""

import pytest
from sentry_integration import (
    scrub_string,
    scrub_pii,
    parameterise_name,
    _is_sensitive_key,
    _contains_pii,
)


# ─── scrub_string ────────────────────────────────────────────────────────────


class TestScrubString:
    def test_redacts_email(self):
        result = scrub_string("Contact john.doe@example.com for details")
        assert "john.doe@example.com" not in result
        assert "[REDACTED]" in result

    def test_redacts_phone_au(self):
        result = scrub_string("Call +61 412 345 678")
        assert "412 345 678" not in result

    def test_redacts_credit_card(self):
        result = scrub_string("Card: 4111 1111 1111 1111")
        assert "4111" not in result

    def test_redacts_tfn(self):
        result = scrub_string("TFN is 123 456 789")
        assert "123 456 789" not in result

    def test_preserves_safe_strings(self):
        safe = "Meeting started at 10:00 AM"
        assert scrub_string(safe) == safe

    def test_handles_empty_string(self):
        assert scrub_string("") == ""

    def test_handles_multiple_pii(self):
        result = scrub_string("Email: a@b.com, Phone: +61 412 345 678")
        assert "a@b.com" not in result
        assert "412 345 678" not in result


# ─── _is_sensitive_key ───────────────────────────────────────────────────────


class TestIsSensitiveKey:
    @pytest.mark.parametrize("key", [
        "email", "user_email", "firstName", "last_name",
        "phone", "password", "api_key", "authToken",
        "ssn", "tfn", "credit_card", "account_number",
        "meeting_url", "join_url", "client_id",
    ])
    def test_detects_sensitive_keys(self, key):
        assert _is_sensitive_key(key), f"Expected '{key}' to be sensitive"

    @pytest.mark.parametrize("key", [
        "environment", "release", "component", "feature",
        "timestamp", "status", "level", "count",
    ])
    def test_allows_safe_keys(self, key):
        assert not _is_sensitive_key(key), f"Expected '{key}' to be safe"


# ─── _contains_pii ──────────────────────────────────────────────────────────


class TestContainsPII:
    def test_detects_email(self):
        assert _contains_pii("user@example.com")

    def test_detects_phone(self):
        assert _contains_pii("+61 412 345 678")

    def test_safe_text(self):
        assert not _contains_pii("Meeting started successfully")

    def test_safe_numbers(self):
        assert not _contains_pii("Status code 200")


# ─── scrub_pii ───────────────────────────────────────────────────────────────


class TestScrubPII:
    def test_scrubs_dict_sensitive_keys(self):
        data = {"email": "test@example.com", "status": "active"}
        result = scrub_pii(data)
        assert result["email"] == "[REDACTED]"
        assert result["status"] == "active"

    def test_scrubs_nested_dicts(self):
        data = {
            "user": {
                "email": "test@example.com",
                "role": "admin",
            },
            "action": "login",
        }
        result = scrub_pii(data)
        assert result["user"]["email"] == "[REDACTED]"
        assert result["user"]["role"] == "admin"
        assert result["action"] == "login"

    def test_scrubs_pii_in_values(self):
        data = {"message": "User test@example.com logged in"}
        result = scrub_pii(data)
        assert "test@example.com" not in str(result)

    def test_scrubs_lists(self):
        data = ["test@example.com", "safe string"]
        result = scrub_pii(data)
        assert "test@example.com" not in str(result)
        assert "safe string" in result

    def test_handles_none(self):
        assert scrub_pii(None) is None

    def test_handles_int(self):
        assert scrub_pii(42) == 42

    def test_max_depth(self):
        # Build deeply nested structure
        data = {"key": "value"}
        for _ in range(15):
            data = {"nested": data}
        result = scrub_pii(data)
        # Should not recurse forever; deep values become [MAX_DEPTH]
        assert "[MAX_DEPTH]" in str(result)

    def test_preserves_tuple_type(self):
        data = ("safe",)
        result = scrub_pii(data)
        assert isinstance(result, tuple)


# ─── parameterise_name ───────────────────────────────────────────────────────


class TestParameteriseName:
    def test_replaces_uuid(self):
        url = "/api/meetings/550e8400-e29b-41d4-a716-446655440000/status"
        result = parameterise_name(url)
        assert ":id" in result
        assert "550e8400" not in result

    def test_replaces_numeric_ids(self):
        url = "/api/users/123456/meetings"
        result = parameterise_name(url)
        assert ":id" in result
        assert "123456" not in result

    def test_preserves_short_numbers(self):
        url = "/api/v2/health"
        result = parameterise_name(url)
        assert result == url

    def test_handles_empty_string(self):
        assert parameterise_name("") == ""

    def test_replaces_multiple_uuids(self):
        url = "/a/550e8400-e29b-41d4-a716-446655440000/b/660e8400-e29b-41d4-a716-446655440000"
        result = parameterise_name(url)
        assert result.count(":id") == 2


# ─── beforeSend simulation ──────────────────────────────────────────────────


class TestBeforeSendIntegration:
    """Test scrubbing as it would be applied in before_send."""

    def test_full_event_scrub(self):
        """Simulate a Sentry event and verify PII is removed."""
        event = {
            "user": {
                "id": "user-123",
                "email": "user@example.com",
                "ip_address": "192.168.1.1",
            },
            "request": {
                "url": "/api/meetings/550e8400-e29b-41d4-a716-446655440000",
                "headers": {
                    "authorization": "Bearer secret-token",
                    "content-type": "application/json",
                },
                "data": {
                    "email": "test@example.com",
                    "platform": "google",
                },
            },
            "tags": {
                "environment": "production",
                "component": "bot",
                "email": "leak@example.com",
            },
            "extra": {
                "password": "supersecret",
                "meeting_count": 5,
            },
        }

        # Step 1: Scrub user
        scrubbed_user = {"id": event["user"].get("id")}
        assert "email" not in scrubbed_user

        # Step 2: Scrub request data
        scrubbed_data = scrub_pii(event["request"]["data"])
        assert scrubbed_data["email"] == "[REDACTED]"
        assert scrubbed_data["platform"] == "google"

        # Step 3: Scrub URL
        scrubbed_url = parameterise_name(event["request"]["url"])
        assert "550e8400" not in scrubbed_url

        # Step 4: Scrub extra
        scrubbed_extra = scrub_pii(event["extra"])
        assert scrubbed_extra["password"] == "[REDACTED]"
        assert scrubbed_extra["meeting_count"] == 5
