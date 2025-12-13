import argparse
import json
import sys
from fastembed import TextEmbedding

def main():
    parser = argparse.ArgumentParser(description="Generate embeddings for a query string using FastEmbed.")
    parser.add_argument("query", type=str, help="The query text to embed")
    parser.add_argument("--model", type=str, default="BAAI/bge-small-en-v1.5", help="The model to use (default: BAAI/bge-small-en-v1.5)")
    
    args = parser.parse_args()
    
    try:
        # Initialize embedding model (suppress output if possible or just print to stderr)
        # We print initialization logs to stderr so stdout is clean JSON
        print(f"Loading model {args.model}...", file=sys.stderr)
        model = TextEmbedding(model_name=args.model)
        
        # fastembed.embed expects a list of documents
        embeddings_generator = model.embed([args.query])
        
        # Get the first (and only) embedding
        embedding = next(embeddings_generator)
        
        # Print only the embedding list to stdout
        print(json.dumps(embedding.tolist()))
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

