"""Embedding models"""

# Imports
import numpy as np
from sentence_transformers import SentenceTransformer
from src.config import hf_token

# Download from the 🤗 Hub
model = SentenceTransformer("google/embeddinggemma-300m", token=hf_token)


def vectorise_text(text):
    """Use embedding model to embed text."""
    embedding = np.asarray(model.encode_document(text))

    return embedding
