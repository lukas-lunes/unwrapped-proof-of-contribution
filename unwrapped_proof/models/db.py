"""SQLAlchemy database models for storing Spotify contribution data"""
import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class UserContribution(Base):
    """
    Tracks user listening data contributions and rewards.
    Uses hashed account IDs for privacy.
    """
    __tablename__ = 'user_contributions'

    id = Column(Integer, primary_key=True)
    account_id_hash = Column(String, unique=True, nullable=False, index=True)
    track_count = Column(Integer, nullable=False)
    total_minutes = Column(Integer, nullable=False)
    activity_period_days = Column(Integer, nullable=False)
    unique_artists = Column(Integer, nullable=False)
    latest_score = Column(Float, nullable=False)
    times_rewarded = Column(Integer, default=0)
    first_contribution_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))
    latest_contribution_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))
    raw_data = Column(JSON, nullable=True)
    encrypted_refresh_token = Column(String, nullable=True)
    # Store the 'before' timestamp (in ms) from the last fetch
    last_spotify_fetch_cursor = Column(BigInteger, nullable=True)

class ContributionProof(Base):
    """
    Stores proof details for each Spotify contribution attempt.
    Links to user_contributions through account_id_hash.
    """
    __tablename__ = 'contribution_proofs'

    id = Column(Integer, primary_key=True)
    account_id_hash = Column(String, nullable=False, index=True)
    file_id = Column(BigInteger, nullable=False)
    file_url = Column(String, nullable=False)
    job_id = Column(String, nullable=False)
    owner_address = Column(String, nullable=False)
    score = Column(Float, nullable=False) # Score for this specific contribution attempt
    authenticity = Column(Float, nullable=False)
    ownership = Column(Float, nullable=False)
    quality = Column(Float, nullable=False)
    uniqueness = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))