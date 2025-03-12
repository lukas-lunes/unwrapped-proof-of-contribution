"""Database storage service for Spotify contributions and proofs"""
import logging
import datetime
from typing import Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from unwrapped_proof.models.db import UserContribution, ContributionProof
from unwrapped_proof.models.contribution import ContributionData, ExistingContribution
from unwrapped_proof.models.proof import ProofResponse

logger = logging.getLogger(__name__)

class StorageService:
    """Handles all database operations"""

    def __init__(self, session: Session):
        self.session = session

    def check_existing_contribution(self, account_id_hash: str) -> Tuple[bool, Optional[ExistingContribution]]:
        """Check if user has already contributed and get their contribution record"""
        try:
            # Query ContributionProof table instead of UserContribution
            previous_proofs = self.session.query(ContributionProof).filter_by(
                account_id_hash=account_id_hash
            ).all()

            if previous_proofs:
                # Calculate cumulative score from all previous proofs
                total_score = sum(float(getattr(p, 'score', 0.0)) for p in previous_proofs)

                # Count how many times rewards were given (proofs with score > 0)
                times_rewarded = sum(1 for p in previous_proofs if float(getattr(p, 'score', 0.0)) > 0)

                # Get the most recent contribution for other stats
                latest_contribution = self.session.query(UserContribution).filter_by(
                    account_id_hash=account_id_hash
                ).order_by(UserContribution.latest_contribution_at.desc()).first()

                return True, ExistingContribution(
                    times_rewarded=times_rewarded,
                    track_count=getattr(latest_contribution, 'track_count', 0),
                    total_minutes=int(getattr(latest_contribution, 'total_minutes', 0)),
                    activity_period_days=getattr(latest_contribution, 'activity_period_days', 0),
                    unique_artists=getattr(latest_contribution, 'unique_artists', 0),
                    latest_score=total_score
                )
            return False, None
        except SQLAlchemyError as e:
            logger.error(f"Database error checking existing contribution: {e}")
            raise

    def store_contribution(self, data: ContributionData, proof: ProofResponse,
                           file_id: int, file_url: str, job_id: str, owner_address: str,
                           encrypted_refresh_token: str = None) -> None:
        """Store contribution and proof data if score > 0"""
        try:
            if proof.score > 0:
                # Prepare raw data for storage
                raw_data = {
                    'stats': {
                        'total_minutes': data.stats.total_minutes,
                        'track_count': data.stats.track_count,
                        'unique_artists': list(data.stats.unique_artists),
                        'activity_period_days': data.stats.activity_period_days,
                        'first_listen': data.stats.first_listen_date.isoformat() if data.stats.first_listen_date else None,
                        'last_listen': data.stats.last_listen_date.isoformat() if data.stats.last_listen_date else None
                    },
                    'tracks': [
                        {
                            'track_id': track.track_id,
                            'artist_id': track.artist_id,
                            'duration_ms': track.duration_ms,
                            'listened_at': track.listened_at.isoformat()
                        }
                        for track in data.tracks
                    ]
                }

                # Update or create user contribution record
                contribution = self.session.query(UserContribution).filter_by(
                    account_id_hash=data.account_id_hash
                ).first()

                if contribution:
                    contribution.track_count = data.stats.track_count
                    contribution.total_minutes = data.stats.total_minutes
                    contribution.activity_period_days = data.stats.activity_period_days
                    contribution.unique_artists = len(data.stats.unique_artists)
                    contribution.latest_score = proof.score
                    contribution.latest_contribution_at = datetime.datetime.now(datetime.UTC)
                    contribution.raw_data = raw_data
                    if encrypted_refresh_token:
                        contribution.encrypted_refresh_token = encrypted_refresh_token
                else:
                    contribution = UserContribution(
                        account_id_hash=data.account_id_hash,
                        track_count=data.stats.track_count,
                        total_minutes=data.stats.total_minutes,
                        activity_period_days=data.stats.activity_period_days,
                        unique_artists=len(data.stats.unique_artists),
                        latest_score=proof.score,
                        times_rewarded=0,
                        raw_data=raw_data,
                        encrypted_refresh_token=encrypted_refresh_token
                    )
                    self.session.add(contribution)

                # Store proof details
                proof_record = ContributionProof(
                    account_id_hash=data.account_id_hash,
                    file_id=file_id,
                    file_url=file_url,
                    job_id=job_id,
                    owner_address=owner_address,
                    score=proof.score,
                    authenticity=proof.authenticity,
                    ownership=proof.ownership,
                    quality=proof.quality,
                    uniqueness=proof.uniqueness
                )
                self.session.add(proof_record)

                self.session.commit()
                logger.info(f"Successfully stored contribution for {data.account_id_hash}")
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"Database error storing contribution: {e}")
            raise