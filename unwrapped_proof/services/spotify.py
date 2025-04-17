"""Spotify API integration service"""
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import time
import json

import requests

from unwrapped_proof.models.contribution import ContributionData, ListeningStats, Track

logger = logging.getLogger(__name__)

# --- Constants for Fetching Control ---
# Max requests for paginated endpoints (recently_played)
MAX_RECENT_PAGES = 15 # Fetch up to 15 * 50 = 750 tracks per run
# Time limit in seconds to stop fetching before hitting the 1-minute execution cap
FETCH_TIME_LIMIT_SECONDS = 50 # Reduced safety margin
# Base delay in seconds for retries on rate limit
RATE_LIMIT_RETRY_BASE_DELAY = 2
# Small delay between pagination requests to be gentle on the API
PAGINATION_DELAY_SECONDS = 0.1
# Max age (in days) for a stored cursor to be considered valid for continuation
MAX_CURSOR_AGE_DAYS = 7
# ------------------------------------

class SpotifyAPI:
    """Handles all Spotify API interactions with consistent formatting"""

    def __init__(self, token: str, base_url: str = "https://api.spotify.com/v1"):
        """
        Initialize with Spotify access token
        """
        if not token:
            raise ValueError("Spotify token cannot be empty")
        self.token = token
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        })
        self.start_time = time.time() # Record start time for time limiting

    def _time_check(self) -> bool:
        """Checks if the elapsed time is within the safe limit."""
        elapsed = time.time() - self.start_time
        if elapsed >= FETCH_TIME_LIMIT_SECONDS:
            logger.warning(f"Approaching time limit ({elapsed:.2f}s >= {FETCH_TIME_LIMIT_SECONDS}s). Halting further Spotify API calls.")
            return False
        return True

    def get_user_info(self) -> Dict[str, Any]:
        """Get basic user profile information"""
        if not self._time_check(): return {} # Return empty if time limit exceeded
        logger.info("Fetching user info...")
        user_info = self._make_request('me')
        # Check if user_info is a dict and has 'id' before logging
        if isinstance(user_info, dict) and 'id' in user_info:
            logger.info(f"User info fetched successfully for user ID: {user_info.get('id')}")
        else:
            logger.error(f"Invalid user info response received: {user_info}")
            # Decide whether to raise an error or return empty dict based on severity
            raise ValueError("Failed to fetch valid user info from Spotify.")
        return user_info


    def get_recently_played(self, limit: int = 50, before: Optional[int] = None) -> List[Dict]:
        """
        Get recently played tracks with pagination

        Args:
            limit: Number of tracks to fetch (max 50 per Spotify API docs)
            before: Unix timestamp in milliseconds for pagination
        """
        if not self._time_check(): return [] # Return empty if time limit exceeded

        endpoint = f'me/player/recently-played?limit={limit}'
        # Only add 'before' if it's provided (not None)
        if before is not None:
            endpoint += f'&before={before}'

        response_data = self._make_request(endpoint)
        # Ensure response_data is a dictionary and has 'items' key which is a list
        if isinstance(response_data, dict) and isinstance(response_data.get('items'), list):
            return response_data['items']
        else:
            logger.warning(f"Unexpected response format for recently played: {response_data}")
            return [] # Return empty list if format is wrong

    def get_top_tracks(self, time_range: str = 'medium_term', limit: int = 50) -> List[Dict]:
        """
        Get user's top tracks

        Args:
            time_range: short_term (4 weeks), medium_term (6 months), or long_term (years)
            limit: Number of tracks to fetch (Spotify API max is 50).
        """
        if not self._time_check(): return [] # Return empty if time limit exceeded
        # Enforce Spotify API limit
        actual_limit = min(limit, 50)
        logger.info(f"Fetching top tracks (range: {time_range}, limit: {actual_limit})...")
        endpoint = f'me/top/tracks?time_range={time_range}&limit={actual_limit}'
        response_data = self._make_request(endpoint)
        # Validate response structure
        if isinstance(response_data, dict) and isinstance(response_data.get('items'), list):
            return response_data['items']
        else:
            logger.warning(f"Unexpected response format for top tracks: {response_data}")
            return []

    def get_top_artists(self, time_range: str = 'medium_term', limit: int = 50) -> List[Dict]:
        """Get user's top artists

        Args:
            time_range: short_term (4 weeks), medium_term (6 months), or long_term (years)
            limit: Number of artists to fetch (Spotify API max is 50).
        """
        if not self._time_check(): return [] # Return empty if time limit exceeded
        # Enforce Spotify API limit
        actual_limit = min(limit, 50)
        logger.info(f"Fetching top artists (range: {time_range}, limit: {actual_limit})...")
        endpoint = f'me/top/artists?time_range={time_range}&limit={actual_limit}'
        response_data = self._make_request(endpoint)
        # Validate response structure
        if isinstance(response_data, dict) and isinstance(response_data.get('items'), list):
            return response_data['items']
        else:
            logger.warning(f"Unexpected response format for top artists: {response_data}")
            return []


    def _make_request(self, endpoint: str, retries: int = 3) -> Dict:
        """Make authenticated request to Spotify API with retries and time check"""
        url = f'{self.base_url}/{endpoint}'
        attempt = 0
        last_exception = None

        while attempt < retries:
            if not self._time_check():
                # If time limit is hit during retries, raise the last error or a timeout error
                logger.error(f"Fetching stopped due to time limit during retry for {url}.")
                raise last_exception or requests.exceptions.Timeout("Fetching stopped due to time limit during retry.")

            attempt += 1
            try:
                logger.debug(f"Attempt {attempt}/{retries}: Making request to {url}")
                response = self.session.get(url, timeout=15) # Add timeout to requests
                response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
                logger.debug(f"Request successful (Status: {response.status_code}) to {url}")
                # Check if response body is valid JSON
                try:
                    json_response = response.json()
                    # Ensure it's a dictionary before returning
                    return json_response if isinstance(json_response, dict) else {}
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON response from {url}. Status: {response.status_code}. Response text: {response.text[:200]}")
                    # If it was a success status code but bad JSON, return empty dict. If it was error status, raise_for_status already handled it.
                    return {}

            except requests.exceptions.HTTPError as e:
                last_exception = e
                response = e.response
                logger.warning(f"HTTP Error on attempt {attempt} for {url}: {e}")
                # Handle specific status codes
                if response.status_code == 401: # Unauthorized
                    logger.error(f"Spotify token is invalid or expired (401) for {url}. Cannot proceed.")
                    raise # Fail immediately
                elif response.status_code == 403: # Forbidden
                    logger.error(f"Forbidden access (403) to Spotify endpoint {url}. Check scopes/permissions.")
                    raise # Fail immediately
                elif response.status_code == 429:  # Rate limit hit
                    retry_after = int(response.headers.get('Retry-After', RATE_LIMIT_RETRY_BASE_DELAY * (2 ** (attempt - 1))))
                    # Ensure retry_after is within reasonable bounds
                    retry_after = max(1, min(retry_after, 60)) # Wait between 1 and 60 seconds
                    logger.warning(f"Rate limit hit (429) for {url}. Retrying after {retry_after} seconds...")
                    if not self._time_check(): # Check time *before* sleeping
                        logger.error(f"Fetching stopped due to time limit before rate limit (429) retry for {url}.")
                        raise last_exception or requests.exceptions.Timeout("Fetching stopped due to time limit before rate limit retry.")
                    time.sleep(retry_after)
                    continue # Go to next attempt
                elif response.status_code >= 500: # Server error
                    # Retry server errors with backoff
                    logger.warning(f"Spotify server error ({response.status_code}) for {url}. Retrying...")
                    # Continue to sleep and retry logic below
                else:
                    # For other 4xx errors, don't retry, raise immediately
                    logger.error(f"Client error ({response.status_code}) for {url}. Aborting request.")
                    raise

            except requests.exceptions.RequestException as e: # Includes connection errors, timeouts, etc.
                last_exception = e
                logger.warning(f"Request Error on attempt {attempt} for {url}: {e}. Retrying...")
                # Continue to sleep and retry logic below

            # Exponential backoff before next retry for non-429 errors handled above or RequestExceptions
            if attempt < retries:
                # Check time before sleeping
                if not self._time_check():
                    logger.error(f"Fetching stopped due to time limit before retry sleep for {url}.")
                    raise last_exception or requests.exceptions.Timeout("Fetching stopped due to time limit before retry sleep.")
                sleep_time = RATE_LIMIT_RETRY_BASE_DELAY * (1.5 ** (attempt - 1)) + (0.5 * attempt) # Adjusted backoff
                logger.info(f"Waiting {sleep_time:.2f}s before next retry for {url}...")
                time.sleep(sleep_time)

        # If loop finishes without returning, raise the last exception
        logger.error(f"Request failed after {retries} attempts for {url}.")
        raise last_exception or requests.exceptions.RetryError(f"Request failed after {retries} attempts for {url}")


    def parse_spotify_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Parse Spotify datetime string to timezone-aware datetime object"""
        if not datetime_str:
            return None
        try:
            # Handle both 'Z' and '+00:00' formats, and potentially naive timestamps
            if isinstance(datetime_str, str):
                if datetime_str.endswith('Z'):
                    dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                else:
                    # Attempt direct parsing, assumes ISO format possibly with offset
                    dt = datetime.fromisoformat(datetime_str)
            elif isinstance(datetime_str, datetime):
                # Already a datetime object
                dt = datetime_str
            else:
                logger.warning(f"Unexpected type for datetime string: {type(datetime_str)}")
                return None

            # If parsed datetime is naive, assume UTC
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse datetime value: {datetime_str}. Error: {e}")
            return None # Return None if parsing fails

    # Modified function signature and return type
    def fetch_all_listening_history(self, start_cursor: Optional[int] = None) -> Tuple[List[Dict], Dict[str, Any], Optional[int]]:
        """
        Fetch listening history iteratively, respecting time limits and using a start cursor.
        Returns a tuple: (list_of_track_entries, top_artists_data, last_successful_fetch_cursor)
        """
        all_tracks_entries = []
        seen_track_ids = set()
        top_artists_data = {} # Store top artists separately
        # Initialize cursor for this run
        current_before_cursor: Optional[int] = None
        # Track the cursor *before* the last successful fetch request was made
        last_successful_before_cursor: Optional[int] = start_cursor # Start assuming the provided one was last successful

        logger.info(f"Starting listening history fetch. Time limit: {FETCH_TIME_LIMIT_SECONDS}s.")
        self.start_time = time.time() # Reset start time for this specific fetch operation

        # Determine the initial 'before' cursor for recently played
        now_ms = int(time.time() * 1000)
        cursor_cutoff = now_ms - (MAX_CURSOR_AGE_DAYS * 24 * 60 * 60 * 1000)

        if start_cursor and start_cursor > cursor_cutoff:
            current_before_cursor = start_cursor
            logger.info(f"Continuing fetch from stored cursor: {start_cursor}")
        else:
            if start_cursor:
                logger.info(f"Stored cursor {start_cursor} is too old (cutoff: {cursor_cutoff}) or invalid. Starting fresh.")
            else:
                logger.info("No valid start cursor provided. Starting fresh.")
            current_before_cursor = now_ms # Start from current time
            last_successful_before_cursor = None # Reset this as we are starting fresh


        # 1. Get Recently Played tracks with pagination and time limit
        logger.info("Fetching recently played tracks...")
        page_count = 0

        while page_count < MAX_RECENT_PAGES:
            if not self._time_check():
                logger.warning("Stopping recently played fetch due to time limit.")
                break

            # Record the cursor we are *about to use* for this request
            request_cursor = current_before_cursor
            logger.info(f"Fetching recently played page {page_count + 1}/{MAX_RECENT_PAGES} (using cursor: {request_cursor})...")

            try:
                # Fetch the next page using the current_before_cursor
                tracks_page = self.get_recently_played(limit=50, before=request_cursor)
                page_count += 1 # Increment page count even if the page is empty or fails later

                # If the request was successful (didn't raise exception), store the cursor used for it
                last_successful_before_cursor = request_cursor

                if not tracks_page: # No more tracks returned
                    logger.info("No more recently played tracks found in this page.")
                    break # Assume we've reached the end for this cursor

                new_tracks_added = 0
                page_last_played_at_str: Optional[str] = None # Track the last timestamp in this specific page

                for entry in tracks_page:
                    track_data = entry.get('track')
                    played_at_str = entry.get('played_at')

                    # Keep track of the last valid timestamp string IN THIS PAGE
                    if isinstance(played_at_str, str):
                         page_last_played_at_str = played_at_str

                    if not isinstance(track_data, dict) or not track_data.get('id') or not isinstance(played_at_str, str):
                        logger.warning(f"Skipping invalid recently played entry: {entry}")
                        continue

                    track_id = track_data['id']
                    if track_id not in seen_track_ids:
                        all_tracks_entries.append(entry)
                        seen_track_ids.add(track_id)
                        new_tracks_added += 1

                logger.info(f"Page {page_count}: Found {len(tracks_page)} tracks, Added {new_tracks_added} new unique tracks.")

                # Update pagination cursor for the NEXT request
                # using the timestamp of the *last* item in the fetched page
                if page_last_played_at_str:
                     last_played_at_dt = self.parse_spotify_datetime(page_last_played_at_str)
                     if last_played_at_dt:
                         # Important: Set the cursor for the *next* iteration
                         # Subtract 1ms to avoid potential overlap issues if timestamps are identical
                         current_before_cursor = int(last_played_at_dt.timestamp() * 1000) - 1
                         logger.debug(f"Next 'before' cursor set to: {current_before_cursor}")
                     else:
                         # Log error already happened in parse_spotify_datetime
                         logger.error("Could not parse timestamp from last track to continue pagination. Stopping.")
                         break # Stop if timestamp parsing fails
                else:
                    logger.warning("No valid timestamp found in the last page fetched. Stopping pagination.")
                    break


                # Add a small delay
                time.sleep(PAGINATION_DELAY_SECONDS)

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch recently played page {page_count}: {e}")
                # Don't update last_successful_before_cursor here as the request failed
                break # Stop fetching if an error occurs

        logger.info(f"Finished fetching recently played. Total unique tracks found in this run: {len(all_tracks_entries)} in {page_count} page requests.")
        logger.info(f"Last successful fetch cursor for this run: {last_successful_before_cursor}")

        # --- Fetch Top Tracks/Artists (fetch first page if time permits) ---
        # 2. Add Top Tracks (only first page, max 50) if time permits
        if self._time_check():
            logger.info("Fetching top tracks (max 50 per range)...")
            now = datetime.now(timezone.utc)
            for time_range in ['short_term', 'medium_term', 'long_term']:
                if not self._time_check(): break # Check time before each range
                try:
                    logger.info(f"Fetching top tracks for range: {time_range}")
                    # Explicitly use limit=50, respecting the API maximum
                    top_tracks = self.get_top_tracks(time_range=time_range, limit=50)
                    new_tracks_added = 0
                    for track in top_tracks:
                        if not track or not track.get('id'):
                            logger.warning(f"Skipping invalid top track entry: {track}")
                            continue

                        if track['id'] not in seen_track_ids:
                            # Create a synthetic play entry for scoring purposes.
                            # Assigns a placeholder timestamp as the API doesn't provide one for top tracks.
                            # Why not just skip them?
                            # Including top tracks, even with a synthetic timestamp, gives a more complete picture of the user's listening habits and diversity,
                            # which contributes to the scoring (especially diversity points and potentially influencing the earliest listen date if the user only has top track data available).
                            synthetic_played_at_dt = now - timedelta(days=90)
                            synthetic_played_at = synthetic_played_at_dt.isoformat(timespec='milliseconds')

                            all_tracks_entries.append({
                             'track': track,
                             'played_at': synthetic_played_at # Use the corrected string
                            })
                            seen_track_ids.add(track['id'])
                            new_tracks_added += 1
                        logger.info(f"Added {new_tracks_added} new unique tracks from top tracks ({time_range}).")
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Failed to fetch top tracks for {time_range}: {e}. Skipping this range.")


        # 3. Fetch Top Artists (only first page, max 50) if time permits
        if self._time_check():
             logger.info("Fetching top artists (max 50)...")
             try:
                 # Explicitly use limit=50, respecting the API maximum
                 # get_top_artists now returns the list directly
                 top_artists_list = self.get_top_artists(time_range='medium_term', limit=50)
                 logger.info(f"Fetched {len(top_artists_list)} top artists.")
             except requests.exceptions.RequestException as e:
                 logger.warning(f"Failed to fetch top artists: {e}. Skipping.")
                 top_artists_list = [] # Ensure it's an empty list on error


        logger.info(f"Total listening history fetch process completed. Found {len(all_tracks_entries)} unique track entries in this run.")
        logger.info(f"Total time taken for Spotify fetch in this run: {time.time() - self.start_time:.2f} seconds.")
        # Return the tracks found in THIS run, the top artists *list*, and the cursor used BEFORE the last successful request
        # Wrap the artists list back into a dict for consistency if needed by downstream processing, otherwise just return the list
        top_artists_dict = {'items': top_artists_list}
        return all_tracks_entries, top_artists_dict, last_successful_before_cursor


    # Modified function signature and return type
    def get_formatted_history(self, start_cursor: Optional[int] = None) -> Tuple[ContributionData, Optional[int]]:
        """
        Get formatted listening history with anonymized user data, using a start cursor.
        Returns a tuple: (ContributionData, last_successful_fetch_cursor)
        """
        # Get user info and hash ID
        user = self.get_user_info() # This call now includes error handling for invalid responses
        # No need to check 'id' explicitly here, error raised in get_user_info if invalid
        account_id_hash = hashlib.sha256(user['id'].encode()).hexdigest()
        logger.info(f"Account ID hash generated: {account_id_hash}")

        # Fetch all available listening history and top artists, passing the start cursor
        all_tracks_entries, top_artists_data, last_successful_fetch_cursor = self.fetch_all_listening_history(start_cursor)

        # --- Process fetched track entries ---
        formatted_tracks: List[Track] = []
        unique_artists_ids: set[str] = set()
        total_duration_ms = 0
        earliest_listen = datetime.now(timezone.utc)
        latest_listen = datetime.fromtimestamp(0, timezone.utc)
        processed_track_count = 0 # Counter for tracks successfully processed

        if not all_tracks_entries:
            logger.warning("No track entries found after fetching history for this run.")
            stats = ListeningStats(
                total_minutes=0,
                track_count=0,
                unique_artists=[],
                activity_period_days=0,
                first_listen_date=None,
                last_listen_date=None
            )
        else:
            # Process tracks
            for entry in all_tracks_entries:
                track_data = entry.get('track')
                played_at_str = entry.get('played_at')

                # Basic validation of entry structure
                if not isinstance(track_data, dict) or not track_data.get('id') or not isinstance(played_at_str, str):
                    logger.warning(f"Skipping invalid track entry during formatting: {entry}")
                    continue

                listened_at = self.parse_spotify_datetime(played_at_str)
                if not listened_at:
                    logger.warning(f"Skipping track {track_data.get('id')} due to unparseable played_at: {played_at_str}")
                    continue

                # Artist processing
                artists = track_data.get('artists', [])
                primary_artist_id = "unknown_artist" # Default
                if isinstance(artists, list) and artists and isinstance(artists[0], dict) and artists[0].get('id'):
                    primary_artist_id = artists[0]['id']
                    for artist in artists:
                        if isinstance(artist, dict) and artist.get('id'):
                            unique_artists_ids.add(artist['id'])
                else:
                    logger.warning(f"Track {track_data.get('id')} missing valid primary artist ID.")

                # Timestamp bounds update
                earliest_listen = min(earliest_listen, listened_at)
                latest_listen = max(latest_listen, listened_at)

                # Duration processing
                duration = track_data.get('duration_ms', 0)
                # Ensure duration is a non-negative integer
                try:
                    valid_duration = max(0, int(duration)) if duration is not None else 0
                except (ValueError, TypeError):
                    logger.warning(f"Invalid duration value {duration} for track {track_data.get('id')}. Using 0.")
                    valid_duration = 0
                total_duration_ms += valid_duration

                # Create Track object
                formatted_tracks.append(Track(
                    track_id=track_data['id'],
                    artist_id=primary_artist_id,
                    duration_ms=valid_duration,
                    listened_at=listened_at
                ))
                processed_track_count += 1 # Increment only if processed successfully

            # Calculate listening stats based on tracks processed *in this run*
            if processed_track_count > 0:
                # Ensure latest is after earliest before calculating days
                if latest_listen > earliest_listen:
                    activity_period_days = (latest_listen - earliest_listen).days + 1 # Add 1 to include start/end day
                else:
                    activity_period_days = 1 # Min 1 day if only one timestamp or same day
                first_listen_date_to_use = earliest_listen
                last_listen_date_to_use = latest_listen
            else: # Handle case where no tracks could be processed
                activity_period_days = 0
                first_listen_date_to_use = None
                last_listen_date_to_use = None

            unique_artists_list = list(unique_artists_ids)

            stats = ListeningStats(
                total_minutes=total_duration_ms // 60000,
                track_count=processed_track_count, # Use count of successfully processed tracks
                unique_artists=unique_artists_list,
                activity_period_days=activity_period_days,
                first_listen_date=first_listen_date_to_use,
                last_listen_date=last_listen_date_to_use
            )

        logger.info(f"Calculated Listening Stats for this run: {stats}")

        # --- Create raw data structure ---
        # Ensure dates are formatted correctly even if None
        first_listen_iso = stats.first_listen_date.isoformat(timespec='milliseconds') if stats.first_listen_date else None
        last_listen_iso = stats.last_listen_date.isoformat(timespec='milliseconds') if stats.last_listen_date else None

        raw_data = {
            'user': { 'id_hash': account_id_hash, 'country': user.get('country'), 'product': user.get('product') },
            'stats': {
                'total_minutes': stats.total_minutes, 'track_count': stats.track_count,
                'unique_artists_count': len(stats.unique_artists), 'activity_period_days': stats.activity_period_days,
                'first_listen': first_listen_iso,
                'last_listen': last_listen_iso
            },
             'tracks': [ {
                 'track_id': t.track_id,
                 'artist_id': t.artist_id,
                 'duration_ms': t.duration_ms,
                 'listened_at': t.listened_at.isoformat(timespec='milliseconds')
                 } for t in formatted_tracks ],
             # Ensure top_artists_data is handled correctly if it's empty or invalid
             'top_artists_medium_term': top_artists_data.get('items', []) if isinstance(top_artists_data, dict) else []
        }

        contribution_data = ContributionData(
            account_id_hash=account_id_hash,
            stats=stats,
            tracks=formatted_tracks,
            raw_data=raw_data
        )

        # Return both the processed data and the cursor indicating where fetching stopped
        return contribution_data, last_successful_fetch_cursor