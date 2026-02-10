"""
Sentry integration for Meeting Bot Python components (controller & manager).

Mirrors the PII scrubbing patterns from the TypeScript bot and the
AdviseWell backend Python implementation.

Usage:
    from sentry_integration import initialise_sentry, capture_error_safe, flush_sentry

    initialise_sentry(component="controller")  # or "manager"
"""

import os
import re
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ─── PII Patterns ────────────────────────────────────────────────────────────

SENSITIVE_KEY_PATTERNS = [
    re.compile(r"email", re.I),
    re.compile(r"phone", re.I),
    re.compile(r"mobile", re.I),
    re.compile(r"address", re.I),
    re.compile(r"name", re.I),
    re.compile(r"first.?name", re.I),
    re.compile(r"last.?name", re.I),
    re.compile(r"full.?name", re.I),
    re.compile(r"display.?name", re.I),
    re.compile(r"given.?name", re.I),
    re.compile(r"family.?name", re.I),
    re.compile(r"ssn", re.I),
    re.compile(r"social.?security", re.I),
    re.compile(r"tfn", re.I),
    re.compile(r"tax.?file", re.I),
    re.compile(r"bsb", re.I),
    re.compile(r"abn", re.I),
    re.compile(r"acn", re.I),
    re.compile(r"medicare", re.I),
    re.compile(r"passport", re.I),
    re.compile(r"license", re.I),
    re.compile(r"birth.?date", re.I),
    re.compile(r"date.of.birth", re.I),
    re.compile(r"dob", re.I),
    re.compile(r"credit.?card", re.I),
    re.compile(r"card.?number", re.I),
    re.compile(r"account.?number", re.I),
    re.compile(r"bank.?account", re.I),
    re.compile(r"routing.?number", re.I),
    re.compile(r"password", re.I),
    re.compile(r"secret", re.I),
    re.compile(r"token", re.I),
    re.compile(r"api.?key", re.I),
    re.compile(r"auth", re.I),
    re.compile(r"credential", re.I),
    re.compile(r"private.?key", re.I),
    re.compile(r"session", re.I),
    re.compile(r"cookie", re.I),
    re.compile(r"access.?token", re.I),
    re.compile(r"refresh.?token", re.I),
    re.compile(r"bearer", re.I),
    re.compile(r"jwt", re.I),
    re.compile(r"salary", re.I),
    re.compile(r"income", re.I),
    re.compile(r"superannuation", re.I),
    re.compile(r"client.?id", re.I),
    re.compile(r"patient", re.I),
    re.compile(r"meeting.?url", re.I),
    re.compile(r"join.?url", re.I),
]

PII_VALUE_PATTERNS = [
    re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),  # Email
    re.compile(r"\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}"),  # Phone
    re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{3}\b"),  # AU TFN
    re.compile(r"\b\d{4}[-.\s]?\d{4}[-.\s]?\d{4}[-.\s]?\d{4}\b"),  # Credit card
    re.compile(r"\b\d{2}[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{3}\b"),  # ABN
    re.compile(r"\b\d{3}[-.\s]?\d{3}\b"),  # BSB
]

# UUID pattern for parameterising
UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)
FIREBASE_ID_PATTERN = re.compile(r"[a-zA-Z0-9]{20,28}")
NUMERIC_ID_PATTERN = re.compile(r"/\d{5,}/")

# Allowed tags that are safe to send
ALLOWED_TAGS = frozenset([
    "environment", "release", "version", "meeting_platform",
    "job_id", "component", "feature", "action", "source",
])


def _is_sensitive_key(key: str) -> bool:
    """Check if a key name suggests sensitive data."""
    return any(p.search(key) for p in SENSITIVE_KEY_PATTERNS)


def _contains_pii(value: str) -> bool:
    """Check if a string value contains PII patterns."""
    return any(p.search(value) for p in PII_VALUE_PATTERNS)


def scrub_string(value: str) -> str:
    """Remove PII from a string value."""
    result = value
    for pattern in PII_VALUE_PATTERNS:
        result = pattern.sub("[REDACTED]", result)
    return result


def scrub_pii(data: Any, depth: int = 0) -> Any:
    """Recursively scrub PII from data structures."""
    if depth > 10:
        return "[MAX_DEPTH]"

    if isinstance(data, str):
        return scrub_string(data) if _contains_pii(data) else data

    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if _is_sensitive_key(str(key)):
                result[key] = "[REDACTED]"
            else:
                result[key] = scrub_pii(value, depth + 1)
        return result

    if isinstance(data, (list, tuple)):
        return type(data)(scrub_pii(item, depth + 1) for item in data)

    return data


def parameterise_name(value: str) -> str:
    """Replace IDs in strings with parameterised placeholders."""
    result = UUID_PATTERN.sub(":id", value)
    result = NUMERIC_ID_PATTERN.sub("/:id/", result)
    return result


# ─── Sentry Integration ─────────────────────────────────────────────────────

_initialised = False


def initialise_sentry(
    component: str = "controller",
    dsn: Optional[str] = None,
    environment: Optional[str] = None,
    release: Optional[str] = None,
    traces_sample_rate: Optional[float] = None,
) -> None:
    """
    Initialise Sentry for a Python component.

    Safe to call multiple times — only initialises once.
    Disabled if no DSN is provided (safe for local dev).
    """
    global _initialised
    if _initialised:
        return

    dsn = dsn or os.environ.get("SENTRY_DSN", "")
    if not dsn:
        logger.info("[Sentry] Disabled — no DSN configured")
        return

    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
    except ImportError:
        logger.warning("[Sentry] sentry-sdk not installed — skipping initialisation")
        return

    environment = (
        environment
        or os.environ.get("SENTRY_ENVIRONMENT")
        or os.environ.get("NODE_ENV", "development")
    )
    release = release or os.environ.get("SENTRY_RELEASE")

    if traces_sample_rate is None:
        env_rate = os.environ.get("SENTRY_TRACES_SAMPLE_RATE")
        if env_rate:
            traces_sample_rate = float(env_rate)
        else:
            traces_sample_rate = 0.1 if environment == "production" else 1.0

    def before_send(event: Dict[str, Any], hint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Strip PII before sending events to Sentry."""
        # Scrub user data
        if "user" in event:
            user_id = event["user"].get("id")
            event["user"] = {"id": user_id} if user_id else {}

        # Scrub request data
        if "request" in event:
            req = event["request"]
            req.pop("cookies", None)
            if "headers" in req:
                req["headers"] = {
                    k: v for k, v in req["headers"].items()
                    if k.lower() in ("content-type", "accept", "user-agent")
                }
            if "query_string" in req:
                req["query_string"] = "[REDACTED]"
            if "data" in req:
                req["data"] = scrub_pii(req["data"])
            if "url" in req:
                req["url"] = parameterise_name(req["url"])

        # Scrub breadcrumbs
        if "breadcrumbs" in event:
            for bc in event.get("breadcrumbs", {}).get("values", []):
                if "message" in bc and bc["message"]:
                    bc["message"] = scrub_string(bc["message"])
                if "data" in bc and bc["data"]:
                    bc["data"] = scrub_pii(bc["data"])

        # Scrub extra context
        if "extra" in event:
            event["extra"] = scrub_pii(event["extra"])

        # Filter tags
        if "tags" in event:
            event["tags"] = {
                k: scrub_string(str(v)) if isinstance(v, str) else v
                for k, v in event["tags"].items()
                if k in ALLOWED_TAGS
            }

        # Scrub exception values
        if "exception" in event:
            for ex in event.get("exception", {}).get("values", []):
                if "value" in ex and ex["value"]:
                    ex["value"] = scrub_string(ex["value"])

        return event

    def before_send_transaction(event: Dict[str, Any], hint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Strip PII from transaction/performance data."""
        if "transaction" in event:
            event["transaction"] = parameterise_name(event["transaction"])

        if "spans" in event:
            for span in event["spans"]:
                if "data" in span:
                    span["data"] = scrub_pii(span["data"])
                if "description" in span:
                    span["description"] = parameterise_name(span["description"])

        return event

    sentry_logging = LoggingIntegration(
        level=logging.INFO,
        event_level=logging.ERROR,
    )

    sentry_sdk.init(
        dsn=dsn,
        send_default_pii=False,
        environment=environment,
        release=release,
        before_send=before_send,
        before_send_transaction=before_send_transaction,
        sample_rate=1.0,
        traces_sample_rate=traces_sample_rate,
        max_breadcrumbs=50,
        integrations=[sentry_logging],
        ignore_errors=[
            "KeyboardInterrupt",
            "SystemExit",
        ],
    )

    sentry_sdk.set_tag("component", component)

    _initialised = True
    logger.info(f"[Sentry] Initialised — component={component}, env={environment}")


def capture_error_safe(
    error: Exception,
    component: Optional[str] = None,
    meeting_platform: Optional[str] = None,
    job_id: Optional[str] = None,
    feature: Optional[str] = None,
    action: Optional[str] = None,
) -> None:
    """Capture an error with safe context tags only."""
    if not _initialised:
        return

    import sentry_sdk

    with sentry_sdk.push_scope() as scope:
        if component:
            scope.set_tag("component", component)
        if meeting_platform:
            scope.set_tag("meeting_platform", meeting_platform)
        if job_id:
            scope.set_tag("job_id", parameterise_name(job_id))
        if feature:
            scope.set_tag("feature", feature)
        if action:
            scope.set_tag("action", action)
        sentry_sdk.capture_exception(error)


def capture_message_safe(
    message: str,
    level: str = "info",
    component: Optional[str] = None,
    feature: Optional[str] = None,
) -> None:
    """Capture a message with PII scrubbing."""
    if not _initialised:
        return

    import sentry_sdk

    scrubbed = scrub_string(message)
    with sentry_sdk.push_scope() as scope:
        scope.set_level(level)
        if component:
            scope.set_tag("component", component)
        if feature:
            scope.set_tag("feature", feature)
        sentry_sdk.capture_message(scrubbed)


def add_breadcrumb(
    message: str,
    category: str,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """Add a breadcrumb with PII scrubbing."""
    if not _initialised:
        return

    import sentry_sdk

    sentry_sdk.add_breadcrumb(
        message=scrub_string(message),
        category=category,
        data=scrub_pii(data) if data else None,
        level="info",
    )


def flush_sentry(timeout: float = 2.0) -> None:
    """Flush pending events before exit."""
    if not _initialised:
        return

    try:
        import sentry_sdk
        sentry_sdk.flush(timeout)
        logger.info("[Sentry] Flushed pending events")
    except Exception as e:
        logger.error(f"[Sentry] Failed to flush: {e}")
