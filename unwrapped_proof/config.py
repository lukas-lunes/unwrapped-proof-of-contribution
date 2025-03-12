"""Application configuration and environment settings"""
from typing import Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class S3Settings(BaseModel):
    """S3 specific settings"""
    access_key_id: str = Field(..., description="AWS access key ID")
    secret_access_key: str = Field(..., description="AWS secret access key")
    region: str = Field(..., description="AWS region")

class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    # Required settings
    DB_PASSWORD: str = Field(..., description="Database password")
    SPOTIFY_TOKEN: str = Field(..., description="Spotify API access token")
    SPOTIFY_ENCRYPTED_REFRESH_TOKEN: str = Field(..., description="Encrypted Spotify refresh token")
    ENCRYPTION_KEY: Optional[str] = Field(..., description="Encryption key for the file")

    # S3 credentials
    AWS_ACCESS_KEY_ID: str = Field(..., description="AWS access key ID")
    AWS_SECRET_ACCESS_KEY: str = Field(..., description="AWS secret access key")
    AWS_REGION: str = Field(default="us-east-1", description="AWS region")

    # Optional settings with defaults
    REWARD_FACTOR: int = Field(1000, description="Token reward multiplier (x10^18)")
    MAX_POINTS: int = Field(1000, description="Maximum possible points for scoring")

    # Optional context settings - can be None if not provided
    DLP_ID: Optional[int] = Field(17, description="Data Liquidity Pool ID") # 17 - mainnet, 26 - testnet
    FILE_ID: Optional[int] = Field(0, description="File ID being processed")
    FILE_URL: Optional[str] = Field('https://spotify-exports.s3.us-east-1.amazonaws.com/encrypted_100000000000_spotify_export_1000000000000.json', description="URL of the encrypted file")
    JOB_ID: Optional[int] = Field(0, description="TEE job ID")
    OWNER_ADDRESS: Optional[str] = Field("0xD91d66783da9aBCCeDf72c28BC3b7741Bb997069", description="Owner's wallet address")

    # Input/Output directories with defaults
    INPUT_DIR: str = Field("/input", description="Directory containing input files")
    OUTPUT_DIR: str = Field("/output", description="Directory for output files")

    @property
    def s3_settings(self) -> S3Settings:
        """Get S3 settings as a separate model"""
        return S3Settings(
            access_key_id=self.AWS_ACCESS_KEY_ID,
            secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            region=self.AWS_REGION
        )

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=True
    )

settings = Settings()

# Constants
MAX_POINTS = 1000