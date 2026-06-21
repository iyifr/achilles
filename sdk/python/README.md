# achillesdb

Python client for [AchillesDB](https://github.com/iyifr/glowstickdb), a hybrid
vector and document database (WiredTiger + FAISS under the hood).

## Install

```bash
pip install achillesdb
```

Requires Python >= 3.10. The client talks to a running AchillesDB server over
HTTP, so you'll need one reachable (defaults to `localhost:8180`).

## Quickstart (sync)

```python
from achillesdb import AchillesClient

with AchillesClient(host="localhost", port=8180) as client:
    db = client.create_database("docs")

    collection = db.create_collection("articles")
    collection.add_documents(
        ids=["1", "2"],
        documents=["hello world", "goodbye world"],
        embeddings=[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
        metadatas=[{"category": "tech"}, {"category": "food"}],
    )

    results = collection.query(top_k=5, query_embedding=[0.1, 0.2, 0.3])
    print(results)
```

## Quickstart (async)

```python
import asyncio
from achillesdb import AsyncAchillesClient

async def main():
    async with AsyncAchillesClient(host="localhost", port=8180) as client:
        db = await client.create_database("docs")
        collection = await db.create_collection("articles")
        await collection.add_documents(
            ids=["1"],
            documents=["hello world"],
            embeddings=[[0.1, 0.2, 0.3]],
        )
        results = await collection.query(top_k=5, query_embedding=[0.1, 0.2, 0.3])
        print(results)

asyncio.run(main())
```

## Filtering with `where`

Pass a plain dict shaped like the HTTP API's `where` field, or use the `W`
builder for a fluent API. Supported operators: `$eq`, `$ne`, `$gt`, `$gte`,
`$lt`, `$lte`, `$in`, `$nin`, `$arrContains`, `$and`, `$or`.

```python
from achillesdb import W

collection.query(
    top_k=5,
    query_embedding=[0.1, 0.2, 0.3],
    where=W.and_(W.eq("category", "tech"), W.nin_("year", [2020, 2021])),
)

# equivalent plain-dict form
collection.query(
    top_k=5,
    query_embedding=[0.1, 0.2, 0.3],
    where={"$and": [{"category": "tech"}, {"year": {"$nin": [2020, 2021]}}]},
)
```

## Embedding functions

Pass `embedding_function` to the client to embed text automatically instead
of supplying `embeddings`/`query_embedding` yourself:

```python
client = AchillesClient(embedding_function=my_embed_fn)
collection.add_documents(ids=["1"], documents=["hello world"])
collection.query(top_k=5, query="hello")
```

`my_embed_fn` takes a `list[str]` and returns a `list[list[float]]` (sync or
async, matching the client's mode).

## Development

```bash
cd sdk/python
uv sync
uv run pytest -m unit            # unit + mocked tests, no server required
uv run pytest -m integration     # requires a running AchillesDB server
```

## License

See the repository root for license information.
