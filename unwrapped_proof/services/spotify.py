"""Spotify API integration service"""
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import time
import json
import os
from collections import Counter

import requests

from unwrapped_proof.config import settings
from unwrapped_proof.models.contribution import ContributionData, ListeningStats, Track
from unwrapped_proof.utils.json_encoder import DateTimeEncoder

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

# --- Helper for Insights (NEW) ---
def _get_image_url(images_list: List[Dict], preferred_index: int = 1) -> Optional[str]:
    """Safely extracts an image URL from Spotify's image list."""
    if not images_list or not isinstance(images_list, list):
        return None
    if len(images_list) > preferred_index and isinstance(images_list[preferred_index], dict):
        return images_list[preferred_index].get('url')
    if images_list and isinstance(images_list[0], dict):
        return images_list[0].get('url')
    for img in images_list:
        if isinstance(img, dict) and img.get('url'):
            return img.get('url')
    return None

def _get_spotify_url(external_urls: Optional[Dict[str, str]]) -> Optional[str]:
    """Safely extracts the Spotify URL from external_urls."""
    if isinstance(external_urls, dict):
        return external_urls.get('spotify')
    return None

def _get_primary_artist_info(artists_list: Optional[List[Dict]]) -> Tuple[Optional[str], Optional[str]]:
    """Safely extracts primary artist name and ID."""
    if isinstance(artists_list, list) and artists_list:
        primary_artist = artists_list[0]
        if isinstance(primary_artist, dict):
            return primary_artist.get('name'), primary_artist.get('id')
    return None, None
# --- End Helper for Insights ---


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
            logger.warning(f"Unexpected response format for top tracks ({time_range}): {response_data}")
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
            logger.warning(f"Unexpected response format for top artists ({time_range}): {response_data}")
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
                    debug_folder = settings.DEBUG_DIR
                    os.makedirs(debug_folder, exist_ok=True) # Ensure debug dir exists
                    # Sanitize endpoint for filename
                    safe_endpoint_name = endpoint.replace('/', '_').replace('?', '_').replace('&', '_').replace('=', '_')
                    debug_file_path = f"{debug_folder}/{int(time.time())}_{safe_endpoint_name}_debug.json"
                    with open(debug_file_path, 'w') as debug_file:
                        json.dump({
                            'request': {'url': url, 'headers': dict(self.session.headers)},
                            'response': json_response
                        }, debug_file, indent=2)
                    return json_response if isinstance(json_response, dict) else {}
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON response from {url}. Status: {response.status_code}. Response text: {response.text[:200]}")
                    return {}
            except requests.exceptions.HTTPError as e:
                last_exception = e; response = e.response
                logger.warning(f"HTTP Error on attempt {attempt} for {url}: {e}")
                if response.status_code == 401: logger.error(f"Spotify token is invalid or expired (401) for {url}. Cannot proceed."); raise
                elif response.status_code == 403: logger.error(f"Forbidden access (403) to Spotify endpoint {url}. Check scopes/permissions."); raise
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', RATE_LIMIT_RETRY_BASE_DELAY * (2 ** (attempt - 1))))
                    retry_after = max(1, min(retry_after, 60))
                    logger.warning(f"Rate limit hit (429) for {url}. Retrying after {retry_after} seconds...")
                    if not self._time_check(): logger.error(f"Fetching stopped due to time limit before rate limit (429) retry for {url}."); raise last_exception
                    time.sleep(retry_after); continue
                elif response.status_code >= 500: logger.warning(f"Spotify server error ({response.status_code}) for {url}. Retrying...")
                else: logger.error(f"Client error ({response.status_code}) for {url}. Aborting request."); raise
            except requests.exceptions.RequestException as e:
                last_exception = e; logger.warning(f"Request Error on attempt {attempt} for {url}: {e}. Retrying...")
            if attempt < retries:
                if not self._time_check(): logger.error(f"Fetching stopped due to time limit before retry sleep for {url}."); raise last_exception
                sleep_time = RATE_LIMIT_RETRY_BASE_DELAY * (1.5 ** (attempt - 1)) + (0.5 * attempt)
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
                if datetime_str.endswith('Z'): dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                else: dt = datetime.fromisoformat(datetime_str)
            elif isinstance(datetime_str, datetime): dt = datetime_str
            else: logger.warning(f"Unexpected type for datetime string: {type(datetime_str)}"); return None
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None: dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse datetime value: {datetime_str}. Error: {e}"); return None

    # Modified function signature and return type for insights
    def fetch_all_listening_history(self, start_cursor: Optional[int] = None) -> Tuple[
        List[Dict], # history_track_entries for scoring
        Dict[str, List[Dict]], # raw_top_tracks_by_range for insights
        Dict[str, List[Dict]], # raw_top_artists_by_range for insights
        Optional[int] # last_successful_fetch_cursor
    ]:
        """
        Fetch listening history iteratively, top tracks, and top artists, respecting time limits.
        """
        all_tracks_entries_for_scoring = []
        seen_track_ids_for_scoring = set() # To avoid double counting in scoring if a top track was also recent

        # For insights specifically
        raw_top_tracks_by_range: Dict[str, List[Dict]] = {"short_term": [], "medium_term": [], "long_term": []}
        raw_top_artists_by_range: Dict[str, List[Dict]] = {"short_term": [], "medium_term": [], "long_term": []}

        current_before_cursor: Optional[int] = None
        last_successful_before_cursor: Optional[int] = start_cursor

        logger.info(f"Starting full data fetch. Time limit: {FETCH_TIME_LIMIT_SECONDS}s.")
        self.start_time = time.time() # Reset start time

        # Determine the initial 'before' cursor for recently played
        now_ms = int(time.time() * 1000)
        cursor_cutoff = now_ms - (MAX_CURSOR_AGE_DAYS * 24 * 60 * 60 * 1000)

        if start_cursor and start_cursor > cursor_cutoff:
            current_before_cursor = start_cursor
            logger.info(f"Continuing fetch from stored cursor: {start_cursor}")
        else:
            if start_cursor: logger.info(f"Stored cursor {start_cursor} is too old or invalid. Starting fresh.")
            else: logger.info("No valid start cursor provided. Starting fresh.")
            current_before_cursor = now_ms
            last_successful_before_cursor = None


        # 1. Get Recently Played tracks with pagination and time limit
        logger.info("Fetching recently played tracks...")
        page_count = 0

        while page_count < MAX_RECENT_PAGES:
            if not self._time_check(): logger.warning("Stopping recently played fetch due to time limit."); break
            request_cursor = current_before_cursor
            logger.info(f"Fetching recently played page {page_count + 1}/{MAX_RECENT_PAGES} (using cursor: {request_cursor})...")

            try:
                # Fetch the next page using the current_before_cursor
                tracks_page = self.get_recently_played(limit=50, before=request_cursor)
                page_count += 1
                last_successful_before_cursor = request_cursor # Mark this cursor as successful before processing page

                if not tracks_page: logger.info("No more recently played tracks found in this page."); break

                new_tracks_added = 0; page_last_played_at_str: Optional[str] = None
                for entry in tracks_page:
                    track_data = entry.get('track'); played_at_str = entry.get('played_at')
                    if isinstance(played_at_str, str): page_last_played_at_str = played_at_str # Keep latest for next cursor
                    if not (isinstance(track_data, dict) and track_data.get('id') and played_at_str):
                        logger.warning(f"Skipping invalid recently played entry: {entry}"); continue

                    # Add to scoring list if new (recently played are primary source for scoring timestamps)
                    if track_data['id'] not in seen_track_ids_for_scoring:
                        all_tracks_entries_for_scoring.append(entry)
                        seen_track_ids_for_scoring.add(track_data['id']); new_tracks_added +=1

                logger.info(f"Page {page_count}: Found {len(tracks_page)} tracks, Added {new_tracks_added} new unique for scoring.")

                if page_last_played_at_str:
                    last_played_at_dt = self.parse_spotify_datetime(page_last_played_at_str)
                    if last_played_at_dt:
                        current_before_cursor = int(last_played_at_dt.timestamp() * 1000) - 1
                        logger.debug(f"Next 'before' cursor set to: {current_before_cursor}")
                    else: logger.error("Could not parse timestamp from last track to continue pagination. Stopping."); break
                else: logger.warning("No valid timestamp found in the last page fetched. Stopping pagination."); break
                time.sleep(PAGINATION_DELAY_SECONDS)
            except requests.exceptions.RequestException as e: logger.error(f"Failed to fetch recently played page {page_count}: {e}"); break
        logger.info(f"Finished fetching recently played. Total unique tracks for scoring: {len(all_tracks_entries_for_scoring)} in {page_count} pages.")
        logger.info(f"Last successful fetch cursor for this run: {last_successful_before_cursor}")

        # --- Fetch Top Tracks & Artists for Insights and potentially for scoring ---
        time_ranges_for_tops = ['short_term', 'medium_term', 'long_term']

        # Fetch Top Tracks
        if self._time_check():
            logger.info("Fetching all top tracks (short, medium, long term)...")
            now = datetime.now(timezone.utc) # For synthetic timestamps
            for time_range in time_ranges_for_tops:
                if not self._time_check(): break
                try:
                    logger.info(f"Fetching top tracks for range: {time_range}")
                    top_tracks = self.get_top_tracks(time_range=time_range, limit=50)
                    raw_top_tracks_by_range[time_range] = top_tracks # Store for insights

                    new_tracks_added_for_scoring = 0
                    for track in top_tracks:
                        if not (track and track.get('id')): logger.warning(f"Skipping invalid top track entry: {track}"); continue
                        if track['id'] not in seen_track_ids_for_scoring: # Add to scoring list if new
                            synthetic_played_at = (now - timedelta(days=90)).isoformat(timespec='milliseconds')
                            all_tracks_entries_for_scoring.append({'track': track, 'played_at': synthetic_played_at})
                            seen_track_ids_for_scoring.add(track['id']); new_tracks_added_for_scoring += 1
                    logger.info(f"Added {new_tracks_added_for_scoring} new unique tracks for scoring from top tracks ({time_range}).")
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Failed to fetch top tracks for {time_range}: {e}.")

        # Fetch Top Artists
        if self._time_check():
            logger.info("Fetching all top artists (short, medium, long term)...")
            for time_range in time_ranges_for_tops:
                if not self._time_check(): break
                try:
                    logger.info(f"Fetching top artists for range: {time_range}")
                    top_artists = self.get_top_artists(time_range=time_range, limit=50)
                    raw_top_artists_by_range[time_range] = top_artists # Store for insights
                    logger.info(f"Fetched {len(top_artists)} top artists for range {time_range}.")
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Failed to fetch top artists for {time_range}: {e}.")

        logger.info(f"Total listening history fetch process completed. Unique track entries for scoring: {len(all_tracks_entries_for_scoring)}.")
        logger.info(f"Total time taken for Spotify fetch in this run: {time.time() - self.start_time:.2f} seconds.")
        return all_tracks_entries_for_scoring, raw_top_tracks_by_range, raw_top_artists_by_range, last_successful_before_cursor

    # Modified function signature and return type for insights
    def get_formatted_history(self, start_cursor: Optional[int] = None) -> Tuple[ContributionData, Optional[int], Dict[str, Any]]:
        """
        Get formatted listening history with anonymized user data, insights, using a start cursor.
        Returns a tuple: (ContributionData, last_successful_fetch_cursor, insights_dictionary)
        """
        user = self.get_user_info()
        account_id_hash = hashlib.sha256(user['id'].encode()).hexdigest()
        logger.info(f"Account ID hash generated: {account_id_hash}")

        # Fetch history and raw top data needed for insights
        (history_track_entries_for_scoring, # Combined recently played & synthetic top tracks for scoring
         raw_top_tracks_by_range, # For insights
         raw_top_artists_by_range, # For insights
         last_successful_fetch_cursor
         ) = self.fetch_all_listening_history(start_cursor)

        # --- Process fetched track entries for scoring stats ---
        formatted_tracks_for_scoring: List[Track] = []
        unique_artist_ids_for_scoring: set[str] = set()
        total_duration_ms_scoring = 0
        earliest_listen_scoring = datetime.now(timezone.utc)
        latest_listen_scoring = datetime.fromtimestamp(0, timezone.utc)
        processed_track_count_scoring = 0

        # For listening persona (from scoring tracks)
        hour_counts = Counter()


        if not history_track_entries_for_scoring:
            logger.warning("No track entries found for scoring stats in this run.")
        else:
            for entry in history_track_entries_for_scoring:
                track_data = entry.get('track')
                played_at_str = entry.get('played_at')
                if not (isinstance(track_data, dict) and track_data.get('id') and isinstance(played_at_str, str)):
                    logger.warning(f"Skipping invalid scoring track entry: {entry}"); continue

                listened_at = self.parse_spotify_datetime(played_at_str)
                if not listened_at:
                    logger.warning(f"Skipping track {track_data.get('id')} (scoring) due to unparseable played_at: {played_at_str}"); continue

                # Persona data collection
                hour_counts[listened_at.hour] += 1

                _, primary_artist_id_val = _get_primary_artist_info(track_data.get('artists'))
                if primary_artist_id_val: unique_artist_ids_for_scoring.add(primary_artist_id_val)
                else: logger.warning(f"Track {track_data.get('id')} missing valid primary artist ID for scoring list.")

                earliest_listen_scoring = min(earliest_listen_scoring, listened_at)
                latest_listen_scoring = max(latest_listen_scoring, listened_at)

                duration = track_data.get('duration_ms', 0)
                try: valid_duration = max(0, int(duration)) if duration is not None else 0
                except (ValueError, TypeError): valid_duration = 0; logger.warning(f"Invalid duration {duration} for track {track_data.get('id')}")
                total_duration_ms_scoring += valid_duration

                formatted_tracks_for_scoring.append(Track(
                    track_id=track_data['id'], artist_id=primary_artist_id_val or "unknown_artist",
                    duration_ms=valid_duration, listened_at=listened_at ))
                processed_track_count_scoring += 1

        activity_period_days_scoring = 0
        first_listen_date_to_use_scoring = None
        last_listen_date_to_use_scoring = None
        if processed_track_count_scoring > 0:
            if latest_listen_scoring > earliest_listen_scoring:
                activity_period_days_scoring = (latest_listen_scoring - earliest_listen_scoring).days + 1
            else: activity_period_days_scoring = 1
            first_listen_date_to_use_scoring = earliest_listen_scoring
            last_listen_date_to_use_scoring = latest_listen_scoring

        stats_for_scoring = ListeningStats(
            total_minutes=total_duration_ms_scoring // 60000, track_count=processed_track_count_scoring,
            unique_artists=list(unique_artist_ids_for_scoring), activity_period_days=activity_period_days_scoring,
            first_listen_date=first_listen_date_to_use_scoring, last_listen_date=last_listen_date_to_use_scoring
        )
        logger.info(f"Calculated Listening Stats for this run (for scoring): {stats_for_scoring}")

        # --- Prepare Insight Attributes (NEW section) ---
        insights: Dict[str, Any] = {
            "top_tracks": [],
            "top_artists": [],
            "listening_persona": "Mystery Listener",
            "throwback_anthem": None,
            "current_obsession": None
        }

        if settings.GENERATE_INSIGHTS:

            # Top 5 Artists (priority: medium -> long -> short)
            top_artists_list_for_insight: List[Dict] = []
            for tr_label in ["medium_term", "long_term", "short_term"]:
                artist_source_list = raw_top_artists_by_range.get(tr_label, [])
                if artist_source_list: # If this time range has artists
                    for art_obj in artist_source_list[:5]: # Take up to top 5
                        if isinstance(art_obj, dict):
                            top_artists_list_for_insight.append({
                                "name": art_obj.get("name"), "id": art_obj.get("id"),
                                "image_url": _get_image_url(art_obj.get("images", [])),
                                "spotify_url": _get_spotify_url(art_obj.get("external_urls"))
                            })
                    if top_artists_list_for_insight: # If we got any artists from this range
                        break # Stop checking other time ranges
            insights["top_artists"] = top_artists_list_for_insight


            # Top 5 Medium Term Tracks
            top_medium_tracks_list_for_insight: List[Dict] = []
            fav_track_list_med = raw_top_tracks_by_range.get("medium_term", [])
            if fav_track_list_med:
                for track_obj in fav_track_list_med[:5]: # Take up to top 5
                    if isinstance(track_obj, dict):
                        artist_name, artist_id = _get_primary_artist_info(track_obj.get("artists"))
                        top_medium_tracks_list_for_insight.append({
                            "name": track_obj.get("name"), "id": track_obj.get("id"),
                            "artist_name": artist_name, "artist_id": artist_id,
                            "album_name": track_obj.get("album", {}).get("name"),
                            "album_id": track_obj.get("album", {}).get("id"),
                            "album_art_url": _get_image_url(track_obj.get("album", {}).get("images", [])),
                            "spotify_url": _get_spotify_url(track_obj.get("external_urls"))
                        })
            insights["top_tracks"] = top_medium_tracks_list_for_insight

            # Listening Persona
            if hour_counts: # Ensure there's data from scoring tracks
                max_period_hour = hour_counts.most_common(1)[0][0]
                persona_map = {
                    range(6,12): "Morning Virtuoso", range(12,18): "Afternoon Cruiser",
                    range(18,24): "Evening Connoisseur", range(0,6): "Night Owl"
                }
                for hr_range, persona_name in persona_map.items():
                    if max_period_hour in hr_range:
                        insights["listening_persona"] = persona_name; break
                else: insights["listening_persona"] = "Versatile Listener" # Fallback if somehow not caught

            # Throwback Anthem (from long_term top tracks)
            long_term_tracks_for_throwback = raw_top_tracks_by_range.get("long_term", [])
            oldest_track_obj_throwback = None
            min_release_year_throwback = datetime.now(timezone.utc).year + 1
            if long_term_tracks_for_throwback:
                for track_obj in long_term_tracks_for_throwback:
                    if isinstance(track_obj, dict) and isinstance(track_obj.get("album"), dict):
                        release_date_str = track_obj["album"].get("release_date")
                        # precision = track_obj["album"].get("release_date_precision") # Not strictly needed if parsing YYYY
                        if release_date_str:
                            try:
                                year = int(release_date_str.split("-")[0]) # Assumes YYYY-MM-DD or YYYY-MM or YYYY
                                if year < min_release_year_throwback:
                                    min_release_year_throwback = year
                                    oldest_track_obj_throwback = track_obj
                            except (ValueError, IndexError): continue # Malformed year/date
                if oldest_track_obj_throwback:
                    artist_name, artist_id = _get_primary_artist_info(oldest_track_obj_throwback.get("artists"))
                    insights["throwback_anthem"] = {
                        "name": oldest_track_obj_throwback.get("name"), "id": oldest_track_obj_throwback.get("id"),
                        "artist_name": artist_name, "artist_id": artist_id,
                        "release_year": min_release_year_throwback if min_release_year_throwback <= datetime.now(timezone.utc).year else None,
                        "album_name": oldest_track_obj_throwback.get("album", {}).get("name"),
                        "album_id": oldest_track_obj_throwback.get("album", {}).get("id"),
                        "album_art_url": _get_image_url(oldest_track_obj_throwback.get("album", {}).get("images", [])),
                        "spotify_url": _get_spotify_url(oldest_track_obj_throwback.get("external_urls"))
                    }

            # Current Obsession (#1 track from short_term top tracks)
            short_term_tracks_for_obsession = raw_top_tracks_by_range.get("short_term", [])
            if short_term_tracks_for_obsession and isinstance(short_term_tracks_for_obsession[0], dict):
                obsession_track_obj = short_term_tracks_for_obsession[0]
                artist_name, artist_id = _get_primary_artist_info(obsession_track_obj.get("artists"))
                insights["current_obsession"] = {
                    "name": obsession_track_obj.get("name"), "id": obsession_track_obj.get("id"),
                    "artist_name": artist_name, "artist_id": artist_id,
                    "album_name": obsession_track_obj.get("album", {}).get("name"),
                    "album_id": obsession_track_obj.get("album", {}).get("id"),
                    "album_art_url": _get_image_url(obsession_track_obj.get("album", {}).get("images", [])),
                    "spotify_url": _get_spotify_url(obsession_track_obj.get("external_urls"))
                }

            logger.info(f"Insights generated: {json.dumps(insights, indent=2, cls=DateTimeEncoder)}")
        else:
            logger.info("GENERATE_INSIGHTS is false. Skipping insight generation.")

        # --- Create raw data structure for storage/output (based on original needs) ---
        first_listen_iso = stats_for_scoring.first_listen_date.isoformat(timespec='milliseconds') if stats_for_scoring.first_listen_date else None
        last_listen_iso = stats_for_scoring.last_listen_date.isoformat(timespec='milliseconds') if stats_for_scoring.last_listen_date else None
        raw_data_for_export = {
            'user': { 'id_hash': account_id_hash, 'country': user.get('country'), 'product': user.get('product') },
            'stats': {
                'total_minutes': stats_for_scoring.total_minutes, 'track_count': stats_for_scoring.track_count,
                'unique_artists_count': len(stats_for_scoring.unique_artists),
                'activity_period_days': stats_for_scoring.activity_period_days,
                'first_listen': first_listen_iso, 'last_listen': last_listen_iso
            },
            'tracks': [ { # Tracks used for scoring
                'track_id': t.track_id, 'artist_id': t.artist_id, 'duration_ms': t.duration_ms,
                'listened_at': t.listened_at.isoformat(timespec='milliseconds') # Ensure milliseconds
            } for t in formatted_tracks_for_scoring ],
            'top_artists_medium_term': raw_top_artists_by_range.get('medium_term', []) # Keep this specific export as per original logic for spotify_data.json
        }

        contribution_data = ContributionData(
            account_id_hash=account_id_hash,
            stats=stats_for_scoring, # Use stats derived from scoring tracks
            tracks=formatted_tracks_for_scoring, # Use tracks derived for scoring
            raw_data=raw_data_for_export
        )
        return contribution_data, last_successful_fetch_cursor, insights