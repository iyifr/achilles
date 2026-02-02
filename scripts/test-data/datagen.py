import os
import json
import uuid
from typing import List
from fastembed import TextEmbedding

# Configuration
DOC_CORPUS_DIR = os.path.join(os.path.dirname(__file__), "doc-corpus")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "output.json")
CHUNK_SIZE = 500  # characters
OVERLAP = 50      # characters

def chunk_text(text: str, chunk_size: int, overlap: int) -> List[str]:
    """
    Sliding window chunking that respects word boundaries.
    """
    if not text:
        return []
    
    chunks = []
    text_len = len(text)
    start = 0
    
    while start < text_len:
        # Tentative end
        end = min(start + chunk_size, text_len)
        
        # If not at the very end of text, try to snap back to a space
        # to avoid cutting words in half.
        if end < text_len:
            # Look for last space in the current slice
            last_space = text.rfind(' ', start, end)
            if last_space != -1:
                end = last_space + 1  # Include the trailing space in this chunk
        
        # Extract chunk
        chunk = text[start:end]
        if chunk.strip(): # Avoid empty whitespace chunks
            chunks.append(chunk)
        
        # If we reached the end, stop
        if end == text_len:
            break
            
        # Calculate next start position
        # We want the next chunk to overlap the previous one by `overlap` amount
        # relative to the current `end`.
        target_start = end - overlap
        
        # Adjust target_start to align with a word boundary (start of word)
        # We look for a space before target_start
        start_space = text.rfind(' ', start, target_start)
        if start_space != -1:
            next_start = start_space + 1
        else:
            # Fallback if no space found (e.g. one huge word or first chunk)
            next_start = target_start
            
        # Guarantee progress to prevent infinite loops
        if next_start <= start:
            next_start = start + 1
            
        start = next_start
        
    return chunks

def main():
    # Initialize embedding model
    print("Loading embedding model (BAAI/bge-small-en-v1.5)...")
    # Using a popular, efficient model supported by FastEmbed
    model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")

    # Initialize SOA (Struct of Arrays) format
    ids = []
    contents = []
    embeddings = []
    metadatas = []

    if not os.path.exists(DOC_CORPUS_DIR):
        print(f"Error: Directory '{DOC_CORPUS_DIR}' not found.")
        return

    files = [f for f in os.listdir(DOC_CORPUS_DIR) if f.endswith(".txt")]
    if not files:
        print(f"No .txt files found in {DOC_CORPUS_DIR}")
        return

    print(f"Found {len(files)} files. Processing...")

    all_chunks = []
    all_metadatas = []

    # 1. Read and chunk all files
    for filename in files:
        filepath = os.path.join(DOC_CORPUS_DIR, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

            file_chunks = chunk_text(content, CHUNK_SIZE, OVERLAP)

            for i, chunk in enumerate(file_chunks):
                all_chunks.append(chunk)
                all_metadatas.append({
                    "source": filename,
                    "chunk_index": i,
                    "version": 1,
                    "type": "text_segment"
                })
        except Exception as e:
            print(f"Error reading {filename}: {e}")

    if not all_chunks:
        print("No chunks generated.")
        return

    print(f"Generated {len(all_chunks)} chunks. Computing embeddings...")

    # 2. Generate embeddings
    # fastembed.embed returns a generator
    embeddings_generator = model.embed(all_chunks)

    # 3. Construct SOA payload
    for i, embedding in enumerate(embeddings_generator):
        doc_id = str(uuid.uuid4())

        # Append to parallel arrays (SOA format)
        ids.append(doc_id)
        contents.append(all_chunks[i])
        embeddings.append(embedding.tolist())  # Convert numpy array to list
        metadatas.append(all_metadatas[i])

    # 4. Create SOA output structure (ChromaDB-compatible)
    output = {
        "ids": ids,
        "documents": contents,  # Note: field name is "documents" not "contents"
        "embeddings": embeddings,
        "metadatas": metadatas
    }

    # 5. Save to JSON
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print(f"Success! Generated SOA format with {len(ids)} documents")
        print(f"  - IDs: {len(ids)} strings")
        print(f"  - Documents: {len(contents)} text chunks")
        print(f"  - Embeddings: {len(embeddings)} vectors of dimension {len(embeddings[0])}")
        print(f"  - Metadatas: {len(metadatas)} objects")
        print(f"Output saved to: {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error writing output file: {e}")

if __name__ == "__main__":
    main()

