"""Spotify API integration service"""
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import time

import requests

from unwrapped_proof.models.contribution import ContributionData, ListeningStats, Track

logger = logging.getLogger(__name__)

class SpotifyAPI:
    """Handles all Spotify API interactions with consistent formatting"""

    def __init__(self, token: str, base_url: str = "https://api.spotify.com/v1"):
        """
        Initialize with Spotify access token
        """
        self.token = token
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        })

    def get_user_info(self) -> Dict[str, Any]:
        """Get basic user profile information"""
        return self._make_request('me')

    def get_recently_played(self, limit: int = 50, before: Optional[int] = None) -> List[Dict]:
        """
        Get recently played tracks with pagination

        Args:
            limit: Number of tracks to fetch (max 50)
            before: Unix timestamp in milliseconds for pagination
        """
        endpoint = f'me/player/recently-played?limit={limit}'
        if before:
            endpoint += f'&before={before}'
        return self._make_request(endpoint).get('items', [])

    def get_top_tracks(self, time_range: str = 'medium_term', limit: int = 50) -> List[Dict]:
        """
        Get user's top tracks

        Args:
            time_range: short_term (4 weeks), medium_term (6 months), or long_term (years)
            limit: Number of tracks to fetch (max 50)
        """
        endpoint = f'me/top/tracks?time_range={time_range}&limit={limit}'
        return self._make_request(endpoint).get('items', [])

    def get_top_artists(self, time_range: str = 'medium_term', limit: int = 50) -> List[Dict]:
        """Get user's top artists"""
        endpoint = f'me/top/artists?time_range={time_range}&limit={limit}'
        return self._make_request(endpoint).get('items', [])

    def _make_request(self, endpoint: str, retries: int = 3) -> Dict:
        """Make authenticated request to Spotify API with retries"""
        url = f'{self.base_url}/{endpoint}'

        for attempt in range(retries):
            try:
                response = self.session.get(url)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if response.status_code == 429:  # Rate limit hit
                    retry_after = int(response.headers.get('Retry-After', 1))
                    logger.warning(f"Rate limit hit, retrying after {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                elif attempt == retries - 1:  # Last attempt
                    logger.error(f"Spotify API error: {str(e)}")
                    raise
                time.sleep(1 * (attempt + 1))  # Exponential backoff

    def parse_spotify_datetime(self, datetime_str: str) -> datetime:
        """Parse Spotify datetime string to timezone-aware datetime object"""
        # Remove 'Z' and add UTC timezone
        if datetime_str.endswith('Z'):
            datetime_str = datetime_str[:-1]
        dt = datetime.fromisoformat(datetime_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def fetch_all_listening_history(self) -> List[Dict]:
        """
        Fetch complete listening history within limits
        Combines recently played tracks and top tracks
        """
        all_tracks = []
        seen_track_ids = set()

        # Get recently played tracks with pagination
        before = int(time.time() * 1000)  # Current time in ms
        for _ in range(5):  # Fetch last ~250 tracks maximum
            tracks = self.get_recently_played(limit=50, before=before)
            if not tracks:
                break

            for track in tracks:
                track_id = track['track']['id']
                if track_id not in seen_track_ids:
                    all_tracks.append(track)
                    seen_track_ids.add(track_id)

            # Update pagination cursor
            before = int(datetime.strptime(tracks[-1]['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)

        # Add top tracks from different time ranges
        now = datetime.now(timezone.utc)
        for time_range in ['short_term', 'medium_term', 'long_term']:
            top_tracks = self.get_top_tracks(time_range=time_range)
            for track in top_tracks:
                if track['id'] not in seen_track_ids:
                    # Create synthetic play entry for top tracks
                    all_tracks.append({
                        'track': track,
                        'played_at': (now - timedelta(days=30)).isoformat()
                    })
                    seen_track_ids.add(track['id'])

        return all_tracks

    def get_formatted_history(self) -> ContributionData:
        """Get formatted listening history with anonymized user data"""
        # Get user info and hash ID
        user = self.get_user_info()
        account_id_hash = hashlib.sha256(user['id'].encode()).hexdigest()

        # Fetch all available listening history
        all_tracks = self.fetch_all_listening_history()

        # Format into Track objects
        formatted_tracks = []
        unique_artists = set()
        total_duration_ms = 0
        earliest_listen = datetime.now(timezone.utc)
        latest_listen = datetime.fromtimestamp(0, timezone.utc)

        for entry in all_tracks:
            track = entry['track']
            listened_at = self.parse_spotify_datetime(entry['played_at'])

            # Update artist tracking
            for artist in track['artists']:
                unique_artists.add(artist['id'])

            # Update listen time bounds
            earliest_listen = min(earliest_listen, listened_at)
            latest_listen = max(latest_listen, listened_at)

            # Accumulate duration
            total_duration_ms += track['duration_ms']

            # Create Track object
            formatted_tracks.append(Track(
                track_id=track['id'],
                artist_id=track['artists'][0]['id'],  # Primary artist
                duration_ms=track['duration_ms'],
                listened_at=listened_at
            ))

        # Calculate listening stats
        stats = ListeningStats(
            total_minutes=total_duration_ms // 60000,  # Convert ms to minutes
            track_count=len(formatted_tracks),
            unique_artists=list(unique_artists),
            activity_period_days=(latest_listen - earliest_listen).days + 1,
            first_listen_date=earliest_listen,
            last_listen_date=latest_listen
        )

        # Create raw data structure
        raw_data = {
            'user': {
                'id_hash': account_id_hash,
                'country': user.get('country'),
                'product': user.get('product')  # premium/free
            },
            'stats': {
                'total_minutes': stats.total_minutes,
                'track_count': stats.track_count,
                'unique_artists_count': len(stats.unique_artists),
                'activity_period_days': stats.activity_period_days,
                'first_listen': stats.first_listen_date.isoformat(),
                'last_listen': stats.last_listen_date.isoformat()
            },
            'tracks': [
                {
                    'track_id': track.track_id,
                    'artist_id': track.artist_id,
                    'duration_ms': track.duration_ms,
                    'listened_at': track.listened_at.isoformat()
                }
                for track in formatted_tracks
            ]
        }

        return ContributionData(
            account_id_hash=account_id_hash,
            stats=stats,
            tracks=formatted_tracks,
            raw_data=raw_data
        )