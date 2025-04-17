"""Database storage service for Spotify contributions and proofs"""
import logging
import datetime
from typing import Optional, Tuple
from sqlalchemy.orm import Session, Query
from sqlalchemy import func, and_
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
        """
        Check if user has already contributed and get their latest contribution record
        and cumulative score.
        """
        try:
            # Get the most recent UserContribution record for stats and cursor
            latest_user_contribution: Optional[UserContribution] = (
                self.session.query(UserContribution)
                .filter_by(account_id_hash=account_id_hash)
                .order_by(UserContribution.latest_contribution_at.desc())
                .first()
            )

            if latest_user_contribution:
                # Calculate the cumulative score from all previous proofs for this user
                # Ensure score is treated as float for summation
                cumulative_score_result: Query = self.session.query(
                    func.sum(ContributionProof.score).label("total_score")
                ).filter_by(account_id_hash=account_id_hash)

                cumulative_score = cumulative_score_result.scalar() or 0.0

                # Count how many times rewards were actually given (proofs with score > 0)
                times_rewarded_result: Query = self.session.query(
                    func.count(ContributionProof.id)
                ).filter(
                    # Combine conditions explicitly using and_
                    and_(
                        ContributionProof.account_id_hash == account_id_hash,
                        ContributionProof.score > 0
                    )
                )
                times_rewarded = times_rewarded_result.scalar() or 0


                # Cast BigInteger cursor from DB to int for Python dataclass
                last_cursor = latest_user_contribution.last_spotify_fetch_cursor
                last_cursor_int = int(last_cursor) if last_cursor is not None else None

                existing_data = ExistingContribution(
                    times_rewarded=times_rewarded,
                    track_count=latest_user_contribution.track_count,
                    total_minutes=latest_user_contribution.total_minutes,
                    activity_period_days=latest_user_contribution.activity_period_days,
                    unique_artists=latest_user_contribution.unique_artists,
                    latest_score=float(cumulative_score), # Cumulative score BEFORE this run
                    last_spotify_fetch_cursor=last_cursor_int
                )
                return True, existing_data
            else:
                # No previous contribution found
                return False, None
        except SQLAlchemyError as e:
            logger.error(f"Database error checking existing contribution for hash {account_id_hash}: {e}")
            # Rollback in case of error during read
            self.session.rollback()
            raise

    def store_contribution(self, data: ContributionData, proof: ProofResponse,
                           file_id: int, file_url: str, job_id: str, owner_address: str,
                           last_successful_fetch_cursor: Optional[int],
                           encrypted_refresh_token: str = None) -> None:
        """Store contribution and proof data if score > 0"""
        try:
            # Only proceed if the score for *this specific run* is positive
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

                # Convert cursor to BigInt compatible type for DB if not None
                db_cursor = last_successful_fetch_cursor

                if contribution:
                    logger.info(f"Updating existing UserContribution for hash: {data.account_id_hash}")
                    # Update stats based on the *latest full data view* we have now
                    contribution.track_count = data.stats.track_count
                    contribution.total_minutes = data.stats.total_minutes
                    contribution.activity_period_days = data.stats.activity_period_days
                    contribution.unique_artists = len(data.stats.unique_artists)
                    # TODO: 'latest_score' in UserContribution might be confusing.
                    # It now represents the stats AS OF this contribution, not the score awarded.
                    # Maybe rename this field later? For now, update it.
                    contribution.latest_score = proof.score # Or maybe cumulative? Let's keep it as this run's score for now.
                    contribution.latest_contribution_at = datetime.datetime.now(datetime.UTC)
                    contribution.raw_data = raw_data
                    if encrypted_refresh_token:
                        contribution.encrypted_refresh_token = encrypted_refresh_token
                    # Update the fetch cursor
                    contribution.last_spotify_fetch_cursor = db_cursor
                else:
                    logger.info(f"Creating new UserContribution for hash: {data.account_id_hash}")
                    contribution = UserContribution(
                        account_id_hash=data.account_id_hash,
                        track_count=data.stats.track_count,
                        total_minutes=data.stats.total_minutes,
                        activity_period_days=data.stats.activity_period_days,
                        unique_artists=len(data.stats.unique_artists),
                        latest_score=proof.score, # Score of this first contribution
                        times_rewarded=0, # Will be updated when checking next time
                        raw_data=raw_data,
                        encrypted_refresh_token=encrypted_refresh_token,
                        last_spotify_fetch_cursor=db_cursor # Store cursor from the first fetch
                    )
                    self.session.add(contribution)

                # Store proof details FOR THIS RUN
                # score stored here is the differential score awarded in this run
                proof_record = ContributionProof(
                    account_id_hash=data.account_id_hash,
                    file_id=file_id,
                    file_url=file_url,
                    job_id=job_id,
                    owner_address=owner_address,
                    score=proof.score, # Score for this specific contribution
                    authenticity=proof.authenticity,
                    ownership=proof.ownership,
                    quality=proof.quality,
                    uniqueness=proof.uniqueness,
                    created_at=datetime.datetime.now(datetime.UTC) # Explicitly set timestamp
                )
                self.session.add(proof_record)

                # Commit transaction for both UserContribution and ContributionProof
                self.session.commit()
                logger.info(f"Successfully stored contribution proof for hash {data.account_id_hash} with score {proof.score}. Cursor updated to {db_cursor}")
            else:
                logger.info(f"Score for this run is {proof.score}. No contribution data stored for hash {data.account_id_hash}.")
                # TODO:
                # Even if score is 0, we might want to update the cursor if fetching happened?
                # Let's update cursor only if score > 0 to ensure reward was processed.
                # If we want to update cursor even on 0 score runs, we'd need to move
                # the UserContribution update logic outside the `if proof.score > 0` block.
                # For now, only update cursor on successful (score > 0) runs.

        except SQLAlchemyError as e:
            self.session.rollback() # Rollback transaction on error
            logger.error(f"Database error storing contribution for hash {data.account_id_hash}: {e}")
            raise
        except Exception as e: # Catch other potential errors
            self.session.rollback()
            logger.error(f"Unexpected error storing contribution for hash {data.account_id_hash}: {e}")
            raise