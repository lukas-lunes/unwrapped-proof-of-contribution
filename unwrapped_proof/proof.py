"""Main proof generation logic for Spotify data"""
import hashlib
import json
import logging
import os
from typing import Dict, Any, Tuple
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

        self.settings = settings
        self.scorer = ContributionScorer()
        self.storage = StorageService(db.get_session())
        self.spotify = SpotifyAPI(settings.SPOTIFY_TOKEN)
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        self.gpg = gnupg.GPG()

    def calculate_checksum(self, path: str) -> str:
        """Calculate SHA256 checksum of a file."""
        checksum = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                checksum.update(chunk)
        return checksum.hexdigest()

    def _encrypt_and_upload(self, data: Dict[str, Any], s3_url: str) -> Tuple[str, str]:
        """
        Encrypt Spotify data using GPG and upload to S3.

        Returns:
            Tuple[str, str]: (encrypted_checksum, decrypted_checksum)
        """
        try:
            # Create temporary directory for file operations
            with tempfile.TemporaryDirectory() as temp_dir:
                # Write data to temporary file
                unencrypted_path = os.path.join(temp_dir, "spotify_data.json")
                with open(unencrypted_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, cls=DateTimeEncoder)

                # Calculate decrypted checksum
                decrypted_checksum = self.calculate_checksum(unencrypted_path)

                # Encrypt the file using GPG
                encrypted_path = os.path.join(temp_dir, "encrypted_spotify_data")
                with open(unencrypted_path, 'rb') as f:
                    status = self.gpg.encrypt_file(
                        fileobj_or_path=f,
                        recipients='',
                        output=encrypted_path,
                        passphrase=self.settings.ENCRYPTION_KEY,
                        armor=False,
                        symmetric=True
                    )

                if not status.ok:
                    raise Exception(f"Encryption failed: {status.status}")

                # Calculate encrypted checksum
                encrypted_checksum = self.calculate_checksum(encrypted_path)

                # Parse S3 URL
                s3_url_parsed = urlparse(s3_url)
                bucket = s3_url_parsed.netloc.split('.')[0]
                key = s3_url_parsed.path.lstrip('/')

                # Upload encrypted file to S3
                with open(encrypted_path, 'rb') as f:
                    self.s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=f,
                        ContentType='application/octet-stream',
                        ACL='public-read'
                    )
                logger.info(f"Successfully uploaded encrypted Spotify data to s3://{bucket}/{key}")

                return encrypted_checksum, decrypted_checksum

        except Exception as e:
            logger.error(f"Error encrypting and uploading Spotify data: {e}")
            raise

    def generate(self) -> ProofResponse:
        """Generate proof by verifying Spotify user ID and fetching fresh listening data"""
        try:
            # Fetch fresh data from Spotify
            fresh_data = self.spotify.get_formatted_history()

            # Check for existing contribution
            has_existing, existing_data = self.storage.check_existing_contribution(
                fresh_data.account_id_hash
            )

            # Calculate validity:
            # - Must have listening history
            # - Must not have been previously rewarded
            # - Must have recent activity (based on activity_period_days)
            is_valid = (
                    fresh_data.stats.track_count > 0 and
                    not has_existing and
                    fresh_data.stats.activity_period_days > 0
            )

            # Calculate scores for valid contribution based on listening stats
            points_breakdown = self.scorer.calculate_score(fresh_data.stats)
            points = points_breakdown.total_points
            score = self.scorer.normalize_score(points, self.settings.MAX_POINTS)

            # Encrypt full data and store in S3
            encrypted_checksum, decrypted_checksum = self._encrypt_and_upload(
                fresh_data.raw_data,
                self.settings.FILE_URL or ''
            )

            # Create proof response
            proof_response = ProofResponse(
                dlp_id=self.settings.DLP_ID or 0,
                valid=is_valid,
                score=score,
                authenticity=1.0,  # Fresh from Spotify API
                ownership=1.0,  # Direct API access confirms ownership
                quality=1.0 if fresh_data.stats.track_count > 0 else 0.0,
                uniqueness=0.0 if has_existing else 1.0,
                attributes={
                    'account_id_hash': fresh_data.account_id_hash,
                    'track_count': fresh_data.stats.track_count,
                    'total_minutes': fresh_data.stats.total_minutes,
                    'data_validated': True,
                    'activity_period_days': fresh_data.stats.activity_period_days,
                    'unique_artists': len(fresh_data.stats.unique_artists),
                    'previously_contributed': has_existing,
                    'times_rewarded': existing_data.times_rewarded if existing_data else 0,
                    'points': points,
                    'points_breakdown': points_breakdown.__dict__
                },
                metadata={
                    'dlp_id': self.settings.DLP_ID or 0,
                    'version': '1.0.0',
                    'file_id': self.settings.FILE_ID or 0,
                    'job_id': self.settings.JOB_ID or '',
                    'owner_address': self.settings.OWNER_ADDRESS or '',
                    'file': {
                        'id': self.settings.FILE_ID or 0,
                        'source': 'TEE',
                        'url': self.settings.FILE_URL or '',
                        'checksums': {
                            'encrypted': encrypted_checksum,
                            'decrypted': decrypted_checksum
                        }
                    }
                }
            )

            # Store contribution if score > 0
            if score > 0:
                self.storage.store_contribution(
                    fresh_data,
                    proof_response,
                    self.settings.FILE_ID or 0,
                    self.settings.FILE_URL or '',
                    self.settings.JOB_ID or '',
                    self.settings.OWNER_ADDRESS or '',
                    self.settings.SPOTIFY_ENCRYPTED_REFRESH_TOKEN or ''
                )

            return proof_response

        except Exception as e:
            logger.error(f"Error generating Spotify proof: {e}")
            raise