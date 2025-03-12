# Unwrapped Proof of Contribution

A proof of contribution system for Unwrapped's Data Liquidity Pool (DLP) that validates and rewards Spotify listening data contributions while preserving user privacy through data hashing.

## Overview

The proof system validates that:
- Submitted data authentically matches Spotify's records
- The contributor owns the data through valid Spotify access token
- The contribution is unique (one reward per user)
- The data meets quality and completeness requirements

Privacy is protected by:
- Storing only hashed account IDs
- Exposing aggregate statistics only
- Anonymizing listening data

### Reward System

Contribution scores are calculated based on:

#### Listening Volume
- 5000+ minutes (~3.5 days) = 500 points
- 1000+ minutes (~17 hours) = 150 points
- 500+ minutes (~8 hours) = 50 points
- 100+ minutes (~1.7 hours) = 25 points
- 30+ minutes = 5 points

#### Artist Diversity
- 50+ artists = 150 points
- 25+ artists = 75 points
- 10+ artists = 30 points
- 5+ artists = 10 points
- 3+ artists = 5 points

#### Historical Data
- 180+ days (6 months) = 100 points
- 90+ days (3 months) = 50 points
- 30+ days (1 month) = 25 points
- 7+ days = 10 points

Final score is normalized to 0-1 range and multiplied by REWARD_FACTOR to determine token reward amount.

### Proof Output Format

```json
{
  "dlp_id": 17,
  "valid": true,
  "score": 0.95,
  "authenticity": 1.0,
  "ownership": 1.0,
  "quality": 1.0,
  "uniqueness": 1.0,
  "attributes": {
    "account_id_hash": "hash_of_spotify_account_id",
    "track_count": 157,
    "total_minutes": 25000,
    "data_validated": true,
    "activity_period_days": 365,
    "unique_artists": 12,
    "previously_contributed": false,
    "times_rewarded": 0,
    "total_points": 175,
    "differential_points": 175,
    "points_breakdown": {
      "volume_points": 150,
      "volume_reason": "150 (5000+ minutes)",
      "diversity_points": 25,
      "diversity_reason": "25 (10+ artists)",
      "history_points": 0,
      "history_reason": "0 (< 6 months)"
    }
  },
  "metadata": {
    "dlp_id": 17,
    "version": "1.0.0",
    "file_id": 5678,
    "job_id": "job-123",
    "owner_address": "0x..."
  }
}
```

## Network Configuration

The system works with two networks:

- **Mainnet** (DLP ID: 17)
- **Testnet** (DLP ID: 26)
- **Local Development** (DLP ID: 0)

## Installation

### Prerequisites
- Python 3.12+
- PostgreSQL database
- Docker (optional)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/unwrapped/proof-of-contribution
cd proof-of-contribution
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```env
# Required for dev. In TEE this will come from the environment
DB_PASSWORD=your_database_password
SPOTIFY_TOKEN=your_spotify_token
SPOTIFY_ENCRYPTED_REFRESH_TOKEN=encrypted_refresh_token

# Optional with defaults
INPUT_DIR=/input
OUTPUT_DIR=/output
REWARD_FACTOR=1000
MAX_POINTS=1000

# Context variables
DLP_ID=17  # 17 for mainnet, 26 for testnet, 0 for local
FILE_ID=5678
FILE_URL=https://...
JOB_ID=job-123
OWNER_ADDRESS=0x...
```

## Usage

### Local Development

Run the proof generation:
```bash
python -m unwrapped_proof
```

### Docker Deployment

1. Build container:
```bash
docker build -t unwrapped-proof .
```

2. Run with Docker:
```bash
docker run \
  --rm \
  --env-file .env \
  --volume $(pwd)/input:/input \
  --volume $(pwd)/output:/output \
  unwrapped-proof
```

## Project Structure

```
unwrapped-proof/
├── unwrapped_proof/
│   ├── models/
│   │   ├── db.py            # Database models
│   │   ├── contribution.py  # Domain models
│   │   └── proof.py         # ProofResponse model
│   ├── services/
│   │   ├── spotify.py       # Spotify API service
│   │   └── storage.py       # Database operations
│   ├── utils/
│   │   └── json_encoder.py  # JSON utilities
│   ├── config.py            # Configuration
│   ├── db.py                # Database connection
│   ├── db_config.py         # Database config with hardcoded parameters
│   ├── proof.py             # Main proof logic
│   └── scoring.py           # Points calculation
├── Dockerfile
├── README.md
└── requirements.txt
```

## Security Considerations

1. Data Privacy:
   - All user IDs are hashed using SHA-256
   - Only aggregate statistics are stored
   - Personal information is stripped before storage

2. Data Validation:
   - Track verification with API
   - Timestamp validation
   - Differential scoring for follow-up contributions

## Contact

For issues or questions, please open a GitHub issue in this repository.