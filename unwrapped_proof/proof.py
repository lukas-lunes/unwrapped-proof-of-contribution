"""Main proof generation logic for Spotify data"""
import hashlib
import json
import logging
import os
from typing import Dict, Any, Tuple, Optional
import tempfile
import gnupg
import boto3
from urllib.parse import urlparse

from unwrapped_proof.config import Settings
from unwrapped_proof.models.proof import ProofResponse
from unwrapped_proof.services.spotify import SpotifyAPI
from unwrapped_proof.services.storage import StorageService
from unwrapped_proof.scoring import ContributionScorer
from unwrapped_proof.db import db
from unwrapped_proof.utils.json_encoder import DateTimeEncoder

logger = logging.getLogger(__name__)

class Proof:
    """Handles proof generation and validation for Spotify data"""

    def __init__(self, settings: Settings):
        """Initialize proof generator with settings"""
        if not settings.SPOTIFY_TOKEN:
            raise ValueError("SPOTIFY_TOKEN is required")
        if not settings.ENCRYPTION_KEY:
            raise ValueError("ENCRYPTION_KEY setting is required for S3 upload")

        self.settings = settings
        self.scorer = ContributionScorer()
        self.storage = StorageService(db.get_session())
        self.spotify = SpotifyAPI(
            token=settings.SPOTIFY_TOKEN,
            base_url=settings.SPOTIFY_API_URL
        )

        if not all([settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY, settings.AWS_REGION]):
            raise ValueError("AWS S3 credentials (ID, Key, Region) are required")

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        try:
            self.gpg = gnupg.GPG()
        except FileNotFoundError:
            logger.error("GPG executable not found. Ensure GnuPG is installed and in the system PATH.")
            raise

    def calculate_checksum(self, path: str) -> str:
        """Calculate SHA256 checksum of a file."""
        checksum = hashlib.sha256()
        try:
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    checksum.update(chunk)
            return checksum.hexdigest()
        except FileNotFoundError:
            logger.error(f"Checksum calculation failed: File not found at {path}")
            raise
        except Exception as e:
            logger.error(f"Checksum calculation failed for {path}: {e}")
            raise


    # Renamed 'data' -> 'raw_spotify_data' for clarity
    def _encrypt_and_upload(self, raw_spotify_data: Dict[str, Any], s3_url: str) -> Tuple[str, str]:
        """
        Encrypt Spotify data using GPG and upload to S3.

        Returns:
            Tuple[str, str]: (encrypted_checksum, decrypted_checksum)
        """
        # Validate S3 URL structure
        if not s3_url or not s3_url.startswith("https://"):
            logger.error(f"Invalid S3 URL provided for upload: {s3_url}")
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        try:
            # Create temporary directory for file operations
            with tempfile.TemporaryDirectory() as temp_dir:
                # Write data to temporary file
                unencrypted_path = os.path.join(temp_dir, "spotify_data.json")
                try:
                    with open(unencrypted_path, 'w', encoding='utf-8') as f:
                        json.dump(raw_spotify_data, f, ensure_ascii=False, cls=DateTimeEncoder)
                except IOError as e:
                    logger.error(f"Failed to write unencrypted data to temporary file {unencrypted_path}: {e}")
                    raise

                # Calculate decrypted checksum
                decrypted_checksum = self.calculate_checksum(unencrypted_path)

                # Encrypt the file using GPG
                encrypted_path = os.path.join(temp_dir, "encrypted_spotify_data")
                try:
                    with open(unencrypted_path, 'rb') as f_in:
                        # Ensure ENCRYPTION_KEY is set
                        if not self.settings.ENCRYPTION_KEY:
                            raise ValueError("Encryption key (ENCRYPTION_KEY) is not set in settings.")

                        status = self.gpg.encrypt_file(
                            fileobj_or_path=f_in, # Use file handle
                            recipients='', # Use symmetric encryption
                            output=encrypted_path,
                            passphrase=self.settings.ENCRYPTION_KEY,
                            armor=False, # Output binary format
                            symmetric=True # Explicitly use symmetric encryption
                        )

                    if not status.ok:
                        logger.error(f"GPG encryption failed. Status: {status.status}, Stderr: {status.stderr}")
                        raise Exception(f"Encryption failed: {status.status} - {status.stderr}")
                    logger.info(f"GPG encryption successful. Encrypted file at: {encrypted_path}")

                except Exception as e: # Catch broader exceptions during encryption
                    logger.error(f"Error during GPG encryption process: {e}")
                    raise

                # Calculate encrypted checksum
                encrypted_checksum = self.calculate_checksum(encrypted_path)

                # Parse S3 URL
                try:
                    s3_url_parsed = urlparse(s3_url)
                    bucket = s3_url_parsed.netloc.split('.')[0]
                    key = s3_url_parsed.path.lstrip('/')
                    if not bucket or not key:
                        raise ValueError("Could not parse bucket or key from S3 URL")
                except Exception as e:
                    logger.error(f"Failed to parse S3 URL '{s3_url}': {e}")
                    raise ValueError(f"Invalid S3 URL format: {s3_url}")

                # Upload encrypted file to S3
                try:
                    with open(encrypted_path, 'rb') as f_up:
                        self.s3_client.put_object(
                            Bucket=bucket,
                            Key=key,
                            Body=f_up,
                            ContentType='application/octet-stream',
                            ACL='public-read'
                        )
                    logger.info(f"Successfully uploaded encrypted Spotify data to s3://{bucket}/{key}")
                except Exception as e: # Catch potential Boto3/S3 errors
                    logger.error(f"Failed to upload encrypted file to S3 (s3://{bucket}/{key}): {e}")
                    raise

                return encrypted_checksum, decrypted_checksum

        except Exception as e:
            # Log generic error for this function if specifics weren't caught
            logger.error(f"Error in _encrypt_and_upload: {e}")
            raise # Re-raise the exception


    def generate(self) -> ProofResponse:
        """Generate proof by verifying Spotify user ID and fetching fresh listening data"""
        try:
            # --- Stage 1: Check for existing contribution and get start cursor ---
            logger.info("Checking for existing contribution...")
            # Fetch user info early to get hash needed for DB lookup
            user_info = self.spotify.get_user_info()
            account_id_hash = hashlib.sha256(user_info['id'].encode()).hexdigest()
            logger.info(f"Account hash: {account_id_hash}")

            has_existing, existing_data = self.storage.check_existing_contribution(account_id_hash)
            start_cursor: Optional[int] = None
            previous_cumulative_score = 0.0

            if has_existing and existing_data:
                start_cursor = existing_data.last_spotify_fetch_cursor
                previous_cumulative_score = existing_data.latest_score # Score achieved BEFORE this run
                logger.info(f"Existing contribution found. Start cursor: {start_cursor}, Previous cumulative score: {previous_cumulative_score}")
            else:
                logger.info("No existing contribution found.")


            # --- Stage 2: Fetch fresh data from Spotify using the start cursor ---
            # This now returns (ContributionData, Optional[int] <- last_successful_fetch_cursor)
            fresh_data, last_successful_fetch_cursor = self.spotify.get_formatted_history(start_cursor=start_cursor)
            # We already got the hash, ensure it matches (should always match if token is same user)
            if fresh_data.account_id_hash != account_id_hash:
                logger.error("Account ID hash mismatch between initial fetch and detailed fetch. This should not happen.")
                raise ValueError("Account ID hash mismatch during proof generation.")

            logger.info(f"Fetched fresh data. Stats: {fresh_data.stats}")
            logger.info(f"Last successful fetch cursor for this run: {last_successful_fetch_cursor}")


            # --- Stage 3: Calculate Score for THIS RUN's Data ---
            # The scorer calculates points based *only* on the data fetched *in this run*
            # This represents the potential *total* points if this were the first contribution.
            points_breakdown = self.scorer.calculate_score(fresh_data.stats)
            current_total_points = points_breakdown.total_points
            # This score represents the *potential total value* of the data fetched this run
            current_total_normalized_score = self.scorer.normalize_score(current_total_points, self.settings.MAX_POINTS)
            logger.info(f"Calculated points for this run's data: {current_total_points}, Normalized: {current_total_normalized_score:.4f}")


            # --- Stage 4: Calculate Differential Score to be Awarded ---
            # Compare the score achievable *now* with the score achieved *before* this run
            differential_normalized_score = max(0.0, current_total_normalized_score - previous_cumulative_score)
            final_score_to_award = differential_normalized_score # The score to store in the proof record for *this* run
            logger.info(f"Previous cumulative score: {previous_cumulative_score:.4f}")
            logger.info(f"Differential score for this run: {final_score_to_award:.4f}")

            # Determine previously rewarded status
            previously_rewarded = (existing_data.times_rewarded > 0) if existing_data else False


            # --- Stage 5: Encrypt and Upload Full Data ---
            # We upload the *full raw data* fetched in this run, regardless of differential score,
            # assuming the file URL corresponds to this run.
            file_url = self.settings.FILE_URL
            if not file_url:
                logger.error("FILE_URL setting is missing, cannot upload data.")
                raise ValueError("FILE_URL is required for storing contribution data.")

            logger.info(f"Encrypting and uploading data to: {file_url}")
            encrypted_checksum, decrypted_checksum = self._encrypt_and_upload(
                fresh_data.raw_data,
                file_url
            )
            logger.info(f"Encryption/Upload complete. Encrypted checksum: {encrypted_checksum}, Decrypted checksum: {decrypted_checksum}")


            # --- Stage 6: Create Proof Response ---
            proof_response = ProofResponse(
                dlp_id=self.settings.DLP_ID,
                # 'valid' could depend on whether *any* new data was fetched, or always true if API call succeeds? Let's say true if process runs.
                valid=True,
                # The score field *in the proof* represents the rewardable score for *this specific transaction*
                score=final_score_to_award,
                authenticity=1.0,  # Fresh from Spotify API implies authenticity
                ownership=1.0,     # Verified through user ID hash matching
                quality=1.0 if fresh_data.stats.track_count > 0 else 0.5, # Basic quality check
                # Uniqueness score could degrade slightly on subsequent contributions
                uniqueness=1.0 if not has_existing else 0.99,
                attributes={
                    'account_id_hash': fresh_data.account_id_hash,
                    # Report stats based on the *total data view* from this run
                    'track_count': fresh_data.stats.track_count,
                    'total_minutes': fresh_data.stats.total_minutes,
                    'data_validated': True,
                    'activity_period_days': fresh_data.stats.activity_period_days,
                    'unique_artists': len(fresh_data.stats.unique_artists),
                    # Contribution history flags
                    'previously_contributed': has_existing,
                    'previously_rewarded': previously_rewarded,
                    'times_rewarded': existing_data.times_rewarded if existing_data else 0,
                    # Points breakdown based on *this run's data view*
                    'total_points': current_total_points, # Potential total points now
                    # Differential points for context (score already reflects this)
                    'differential_points': int(final_score_to_award * self.settings.MAX_POINTS),
                    'points_breakdown': points_breakdown.__dict__
                },
                metadata={
                    'dlp_id': self.settings.DLP_ID or 0,
                    'version': '1.0.2',
                    'file_id': self.settings.FILE_ID or 0,
                    'job_id': self.settings.JOB_ID or '',
                    'owner_address': self.settings.OWNER_ADDRESS or '',
                    'file': {
                        'id': self.settings.FILE_ID or 0,
                        'source': 'TEE',
                        'url': file_url,
                        'checksums': {
                            'encrypted': encrypted_checksum,
                            'decrypted': decrypted_checksum
                        }
                    }
                }
            )


            # --- Stage 7: Store Contribution Data (Conditional) ---
            # Store if the score awarded in *this run* is greater than 0
            if final_score_to_award > 0:
                logger.info(f"Storing contribution record as score ({final_score_to_award}) > 0.")
                self.storage.store_contribution(
                    data=fresh_data, # Store the full data view from this run
                    proof=proof_response, # Pass the full proof response
                    file_id=self.settings.FILE_ID or 0,
                    file_url=file_url,
                    job_id=self.settings.JOB_ID or '',
                    owner_address=self.settings.OWNER_ADDRESS or '',
                    # Pass the cursor that marks the end of *this run's* fetch
                    last_successful_fetch_cursor=last_successful_fetch_cursor,
                    encrypted_refresh_token=self.settings.SPOTIFY_ENCRYPTED_REFRESH_TOKEN or ''
                )
            else:
                logger.info(f"No score awarded ({final_score_to_award}). Skipping storage of contribution record update.")
                # TODO:
                # Decide if we should update the cursor even if score is 0?
                # If we fetched new data but it didn't increase score, maybe we should still update cursor?
                # Let's stick to only updating cursor on score > 0 for now to ensure atomicity with reward.


            logger.info("Proof generation successful.")
            return proof_response

        except Exception as e:
            logger.exception(f"Critical error during proof generation: {e}", exc_info=True) # Log stack trace
            # Return a 'failed' ProofResponse
            return ProofResponse(
                dlp_id=self.settings.DLP_ID or 0,
                valid=False,
                score=0.0,
                attributes={'error': str(e)},
                metadata={'file_id': self.settings.FILE_ID or 0, 'job_id': self.settings.JOB_ID or ''}
            )
        finally:
            # Ensure the session is closed, regardless of success or failure
            self.storage.session.close()
            logger.info("Database session closed.")