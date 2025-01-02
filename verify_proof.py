import json
from spotify_proof.config import settings
from spotify_proof.proof import Proof
from spotify_proof.db import db

# Initialize database
db.init()

# Create proof instance
proof = Proof(settings)

# Generate proof
proof_response = proof.generate()

# Print results
print(json.dumps(proof_response.model_dump(), indent=2))