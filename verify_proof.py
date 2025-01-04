import json
from unwrapped_proof.config import settings
from unwrapped_proof.proof import Proof
from unwrapped_proof.db import db

# Initialize database
db.init()

# Create proof instance
proof = Proof(settings)

# Generate proof
proof_response = proof.generate()

# Print results
print(json.dumps(proof_response.model_dump(), indent=2))