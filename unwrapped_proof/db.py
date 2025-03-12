"""Database connection and session management"""
import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from unwrapped_proof.models.db import Base
from unwrapped_proof.db_config import DatabaseManager

logger = logging.getLogger(__name__)

class Database:
    """Database connection and session manager for TEE environment"""

    def __init__(self):
        """Initialize database manager state"""
        self._engine = None
        self._SessionLocal = None

    def _get_connection_string(self) -> str:
        """
        Get database connection string using credential management.

        Returns:
            str: Properly formatted connection string with hardcoded parameters

        Raises:
            ValueError: If required environment variables are missing
            SQLAlchemyError: If database connection fails
        """
        try:
            return DatabaseManager.initialize_from_env()
        except ValueError as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise

    def init(self) -> None:
        """
        Initialize database connection and create tables.
        """
        try:
            # Get connection string with credentials
            connection_string = self._get_connection_string()
            self._engine = create_engine(connection_string)
            Base.metadata.create_all(self._engine)
            self._SessionLocal = sessionmaker(bind=self._engine)
            logger.info("Database initialized successfully")

        except SQLAlchemyError as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations"""
        if not self._SessionLocal:
            raise RuntimeError("Database not initialized. Call init() first.")

        session = self._SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self) -> Session:
        """Get a new database session"""
        if not self._SessionLocal:
            raise RuntimeError("Database not initialized. Call init() first.")
        return self._SessionLocal()

    def dispose(self) -> None:
        """
        Clean up database connections.
        Should be called during application shutdown.
        """
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._SessionLocal = None

# Global database instance
db = Database()