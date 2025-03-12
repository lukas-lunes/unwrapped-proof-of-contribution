"""Database configuration and credentials management for TEE"""
from dataclasses import dataclass

from unwrapped_proof.config import settings

# Network-specific database configurations
MAINNET_CONFIG = {
    'HOST': 'ep-polished-mouse-a5y9f8l3-pooler.us-east-2.aws.neon.tech',  # Update with actual host
    'PORT': '5432',
    'NAME': 'unwrapped-dev',
    'USER': 'unwrapped-dev_owner',
    'SSL_MODE': 'require'
}

TESTNET_CONFIG = {
    'HOST': 'ep-polished-mouse-a5y9f8l3-pooler.us-east-2.aws.neon.tech',
    'PORT': '5432',
    'NAME': 'unwrapped-beta',
    'USER': 'unwrapped-dev_owner',
    'SSL_MODE': 'require'
}

LOCAL_CONFIG = {
    'HOST': 'localhost',
    'PORT': '5432',
    'NAME': 'unwrapped',
    'USER': 'unwrapped',
    'SSL_MODE': 'disable'
}

def determine_network_config() -> dict:
    """Determine database configuration based on DLP_ID."""
    if not settings.DLP_ID:
        raise ValueError("DLP_ID setting is required")

    if settings.DLP_ID == 17:
        return MAINNET_CONFIG
    elif settings.DLP_ID == 25:
        return TESTNET_CONFIG
    elif settings.DLP_ID == 0:
        return LOCAL_CONFIG
    else:
        raise ValueError(f"Invalid DLP_ID {settings.DLP_ID}. Must be 17 (mainnet), 25 (moksha) or 0 (local)")

# Select configuration based on DLP_ID
DB_CONFIG = determine_network_config()

@dataclass
class DatabaseCredentials:
    """Database credentials container with validation"""
    host: str
    port: str
    name: str
    user: str
    password: str
    ssl_mode: str = 'require'

    def to_connection_string(self) -> str:
        """Generate database connection string with proper escaping"""
        return (
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/"
            f"{self.name}?sslmode={self.ssl_mode}"
        )

    @classmethod
    def from_config(cls, password: str) -> 'DatabaseCredentials':
        """Create credentials from config and provided password"""
        return cls(
            host=DB_CONFIG['HOST'],
            port=DB_CONFIG['PORT'],
            name=DB_CONFIG['NAME'],
            user=DB_CONFIG['USER'],
            password=password,
            ssl_mode=DB_CONFIG['SSL_MODE']
        )

class DatabaseManager:
    """Manages database connections in TEE environment"""

    @staticmethod
    def get_connection_string(db_password: str) -> str:
        """
        Generate database connection string from config and password

        Args:
            db_password: Decrypted database password

        Returns:
            Complete database connection string
        """
        credentials = DatabaseCredentials.from_config(db_password)
        return credentials.to_connection_string()

    @classmethod
    def initialize_from_env(cls) -> str:
        """
        Initialize database connection from environment variables

        Returns:
            Database connection string

        Raises:
            ValueError: If required environment variables are missing
        """
        if not settings.DB_PASSWORD:
            raise ValueError("DB_PASSWORD setting is required")

        return cls.get_connection_string(settings.DB_PASSWORD)