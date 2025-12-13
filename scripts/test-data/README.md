# Test Data Generation Scripts

This directory contains scripts for generating test data and embeddings.

## Scripts

### `datagen.py`

Generates test documents with embeddings from text files in the `doc-corpus` directory.

**Features:**

- Reads `.txt` files from the `doc-corpus` directory
- Chunks text using sliding window approach with configurable size and overlap
- Generates embeddings using FastEmbed (BAAI/bge-small-en-v1.5 model)
- Outputs structured JSON with documents, embeddings, and metadata

**Configuration:**

- `CHUNK_SIZE`: 500 characters per chunk
- `OVERLAP`: 50 characters overlap between chunks
- Output file: `output.json`

**Usage:**
