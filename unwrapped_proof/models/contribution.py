"""Domain models for handling Spotify listening data contributions"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

@dataclass
class ListeningStats:
    """Statistics about user's listening history"""
    total_minutes: int
    track_count: int
    unique_artists: List[str]
    activity_period_days: int
    first_listen_date: Optional[datetime]
    last_listen_date: Optional[datetime]

@dataclass
class Track:
    """Anonymized track listening data"""
    track_id: str
    artist_id: str
    duration_ms: int
    listened_at: datetime

@dataclass
class ContributionData:
    """Complete contribution data package"""
    account_id_hash: str
    stats: ListeningStats
    tracks: List[Track]
    raw_data: Dict[str, Any]

@dataclass
class ExistingContribution:
    """Information about an existing user contribution"""
    times_rewarded: int
    track_count: int
    total_minutes: int
    activity_period_days: int
    unique_artists: int
    latest_score: float