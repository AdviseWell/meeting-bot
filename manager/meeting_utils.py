"""
Utility functions for meeting message processing
"""
import os
import re
import uuid
from typing import Optional, Dict, Tuple


def detect_platform(url: str) -> Optional[str]:
    """
    Detect the meeting platform from a URL
    
    Args:
        url: The meeting URL
        
    Returns:
        Platform name ('google', 'microsoft', 'zoom') or None if not detected
    """
    if 'teams.microsoft.com' in url:
        return 'microsoft'
    if 'meet.google.com' in url:
        return 'google'
    if 'zoom.us' in url:
        return 'zoom'
    return None


def extract_meeting_id(url: str) -> Optional[str]:
    """
    Extract a meeting ID from a meeting URL
    
    Args:
        url: The meeting URL
        
    Returns:
        Meeting ID string or None if not extractable
    """
    # For Microsoft Teams: extract meeting code from URL
    teams_match = re.search(r'meet/(\d+)', url)
    if teams_match:
        return f'teams-{teams_match.group(1)}'
    
    # For Zoom: extract meeting ID
    zoom_match = re.search(r'zoom\.us/j/(\d+)', url)
    if zoom_match:
        return f'zoom-{zoom_match.group(1)}'
    
    # For Google Meet: extract meeting code
    meet_match = re.search(r'meet\.google\.com/([a-z-]+)', url)
    if meet_match:
        return f'meet-{meet_match.group(1)}'
    
    return None


def get_bearer_token(platform: str) -> str:
    """
    Get the bearer token for a specific platform from environment variables
    
    Args:
        platform: Platform name ('microsoft', 'google', 'zoom')
        
    Returns:
        Bearer token string (empty if not configured)
    """
    token_map = {
        'microsoft': os.getenv('TEAMS_BEARER_TOKEN', 'DEFAULT_TOKEN'),
        'google': os.getenv('GOOGLE_MEET_BEARER_TOKEN', 'DEFAULT_TOKEN'),
        'zoom': os.getenv('ZOOM_BEARER_TOKEN', 'DEFAULT_TOKEN'),
    }
    return token_map.get(platform, '')


def generate_uuid() -> str:
    """
    Generate a UUID for userId or botId
    
    Returns:
        UUID string
    """
    return str(uuid.uuid4())


def auto_generate_missing_fields(
    url: str,
    bearer_token: Optional[str] = None,
    user_id: Optional[str] = None,
    bot_id: Optional[str] = None,
    event_id: Optional[str] = None
) -> Dict[str, str]:
    """
    Auto-generate missing fields for meeting join params
    
    Args:
        url: Meeting URL (required)
        bearer_token: Bearer token (optional, will be auto-generated from env)
        user_id: User ID (optional, will be auto-generated UUID)
        bot_id: Bot ID (optional, will be auto-generated UUID or use event_id)
        event_id: Event ID (optional, used as fallback for bot_id)
        
    Returns:
        Dictionary with all fields populated
    """
    platform = detect_platform(url)
    if not platform:
        raise ValueError(f"Cannot detect meeting platform from URL: {url}")
    
    meeting_id = extract_meeting_id(url)
    if not meeting_id:
        raise ValueError(f"Cannot extract meeting ID from URL: {url}")
    
    # Use provided values or auto-generate
    final_bearer_token = bearer_token if bearer_token else get_bearer_token(platform)
    final_user_id = user_id if user_id else generate_uuid()
    final_bot_id = bot_id if bot_id else (event_id if event_id else generate_uuid())
    
    return {
        'meeting_id': meeting_id,
        'platform': platform,
        'bearer_token': final_bearer_token,
        'user_id': final_user_id,
        'bot_id': final_bot_id,
    }
