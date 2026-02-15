# AchillesDB SDK Implementation Examples
## Python Examples

### Basic Setup

```python
from achillesdb import AdminClient, Client

# Admin operations (database management)
admin = AdminClient(host="localhost", port=8180, ssl=False)

# Create database
admin.create_database("my_database")

# Client operations (within database)
client = Client(
    host="localhost",
    port=8180,
    ssl=False,
    database="my_database"
)
```

### Working with Collections

```python
# Create collection
collection = client.create_collection(
    name="documents",
    metadata={"description": "My document collection"}
)

# Get existing collection
collection = client.get_collection("documents")

# Get or create
collection = client.get_or_create_collection("documents")

# List all collections
collections = client.list_collections(limit=10, offset=0)

# Count collections
count = client.count_collections()

# Delete collection
client.delete_collection("documents")
```

### Adding Documents

```python
# Add with embeddings
collection.add(
    ids=["doc1", "doc2", "doc3"],
    embeddings=[
        [0.1, 0.2, 0.3],
        [0.4, 0.5, 0.6],
        [0.7, 0.8, 0.9]
    ],
    metadatas=[
        {"source": "web", "category": "tech"},
        {"source": "api", "category": "science"},
        {"source": "upload", "category": "tech"}
    ],
    documents=[
        "First document content",
        "Second document content",
        "Third document content"
    ]
)

# Add with auto-embedding (requires embedding_function)
from achillesdb.embeddings import OpenAIEmbedding

embedding_fn = OpenAIEmbedding(api_key="sk-...", model="text-embedding-3-small")
collection = client.create_collection(
    name="auto_embedded",
    embedding_function=embedding_fn
)

collection.add(
    ids=["doc1"],
    documents=["This will be automatically embedded"]
)

# Add single document
collection.add(
    ids="single_doc",
    documents="Single document content",
    metadatas={"tag": "important"}
)
```

### Querying Documents

```python
# Query with embeddings
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    n_results=10,
    include=["documents", "metadatas", "distances"]
)

# Access results
for i, doc_list in enumerate(results.documents):
    print(f"Query {i} results:")
    for j, doc in enumerate(doc_list):
        print(f"  Doc: {doc}")
        print(f"  Distance: {results.distances[i][j]}")
        print(f"  Metadata: {results.metadatas[i][j]}")

# Query with text (auto-embedding)
results = collection.query(
    query_texts=["search for relevant documents"],
    n_results=5
)

# Query with metadata filter
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    n_results=10,
    where={"category": "tech"}
)

# Query specific IDs only
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    ids=["doc1", "doc2", "doc3"],
    n_results=2
)
```

### Getting Documents

```python
# Get all documents
results = collection.get()

# Get specific documents by ID
results = collection.get(
    ids=["doc1", "doc2"]
)

# Get with metadata filter
results = collection.get(
    where={"category": "tech"},
    limit=10,
    offset=0
)

# Get with specific fields
results = collection.get(
    ids=["doc1"],
    include=["documents", "metadatas", "embeddings"]
)

# Peek at first N documents
results = collection.peek(limit=5)
```

### Updating Documents

```python
# Update documents
collection.update(
    ids=["doc1", "doc2"],
    metadatas=[
        {"category": "tech", "updated": True},
        {"category": "science", "updated": True}
    ]
)

# Update with new documents (re-embedding)
collection.update(
    ids=["doc1"],
    documents=["Updated document content"]
)

# Update with new embeddings
collection.update(
    ids=["doc1"],
    embeddings=[[0.9, 0.8, 0.7]]
)
```

### Upserting Documents

```python
# Upsert (create or update)
collection.upsert(
    ids=["doc1", "doc4"],  # doc1 exists, doc4 is new
    documents=["Updated content for doc1", "New doc4 content"],
    metadatas=[{"status": "updated"}, {"status": "new"}]
)
```

### Deleting Documents

```python
# Delete by IDs
collection.delete(ids=["doc1", "doc2"])

# Delete by metadata filter
collection.delete(
    where={"category": "archived"}
)

# Delete with complex filter
collection.delete(
    where={
        "$and": [
            {"status": "old"},
            {"category": "temp"}
        ]
    }
)
```

### Async Client

```python
from achillesdb import AsyncClient, AsyncAdminClient
import asyncio

async def main():
    # Admin operations
    admin = AsyncAdminClient(host="localhost", port=8180)
    await admin.create_database("async_db")

    # Client operations
    client = AsyncClient(host="localhost", port=8180, database="async_db")

    collection = await client.create_collection("documents")

    # Add documents
    await collection.add(
        ids=["doc1", "doc2"],
        documents=["First doc", "Second doc"]
    )

    # Query
    results = await collection.query(
        query_texts=["search query"],
        n_results=5
    )

    print(results.documents)

asyncio.run(main())
```


---

## TypeScript Examples

### Basic Setup

```typescript
import { AdminClient, Client } from 'achillesdb';

// Admin operations
const admin = new AdminClient({
    host: 'localhost',
    port: 8180,
    ssl: false
});

// Create database
await admin.createDatabase('my_database');

// Client operations
const client = new Client({
    host: 'localhost',
    port: 8180,
    ssl: false,
    database: 'my_database'
});
```

### Working with Collections

```typescript
// Create collection
const collection = await client.createCollection({
    name: 'documents',
    metadata: { description: 'My document collection' }
});

// Get existing collection
const collection = await client.getCollection('documents');

// Get or create
const collection = await client.getOrCreateCollection('documents');

// List all collections
const collections = await client.listCollections({ limit: 10, offset: 0 });

// Count collections
const count = await client.countCollections();

// Delete collection
await client.deleteCollection('documents');
```

### Adding Documents

```typescript
// Add with embeddings
await collection.add({
    ids: ['doc1', 'doc2', 'doc3'],
    embeddings: [
        [0.1, 0.2, 0.3],
        [0.4, 0.5, 0.6],
        [0.7, 0.8, 0.9]
    ],
    metadatas: [
        { source: 'web', category: 'tech' },
        { source: 'api', category: 'science' },
        { source: 'upload', category: 'tech' }
    ],
    documents: [
        'First document content',
        'Second document content',
        'Third document content'
    ]
});

// Add with auto-embedding
import { OpenAIEmbedding } from 'achillesdb/embeddings';

const embeddingFn = new OpenAIEmbedding({
    apiKey: 'sk-...',
    model: 'text-embedding-3-small'
});

const collection = await client.createCollection({
    name: 'auto_embedded',
    embeddingFunction: embeddingFn
});

await collection.add({
    ids: ['doc1'],
    documents: ['This will be automatically embedded']
});
```

### Querying Documents

```typescript
// Query with embeddings
const results = await collection.query({
    queryEmbeddings: [[0.1, 0.2, 0.3]],
    nResults: 10,
    include: ['documents', 'metadatas', 'distances']
});

// Access results
results.documents.forEach((docList, i) => {
    console.log(`Query ${i} results:`);
    docList.forEach((doc, j) => {
        console.log(`  Doc: ${doc}`);
        console.log(`  Distance: ${results.distances[i][j]}`);
        console.log(`  Metadata:`, results.metadatas[i][j]);
    });
});

// Query with text
const results = await collection.query({
    queryTexts: ['search for relevant documents'],
    nResults: 5
});

// Query with metadata filter
const results = await collection.query({
    queryEmbeddings: [[0.1, 0.2, 0.3]],
    nResults: 10,
    where: { category: 'tech' }
});


### Getting Documents

```typescript
// Get all documents
const results = await collection.get();

// Get specific documents
const results = await collection.get({
    ids: ['doc1', 'doc2']
});

// Get with filter
const results = await collection.get({
    where: { category: 'tech' },
    limit: 10,
    offset: 0
});

// Peek
const results = await collection.peek({ limit: 5 });
```

### Updating Documents

```typescript
// Update documents
await collection.update({
    ids: ['doc1', 'doc2'],
    metadatas: [
        { category: 'tech', updated: true },
        { category: 'science', updated: true }
    ]
});

// Update with new documents
await collection.update({
    ids: ['doc1'],
    documents: ['Updated document content']
});
```

### Upserting Documents

```typescript
// Upsert
await collection.upsert({
    ids: ['doc1', 'doc4'],
    documents: ['Updated content for doc1', 'New doc4 content'],
    metadatas: [{ status: 'updated' }, { status: 'new' }]
});
```

### Deleting Documents

```typescript
// Delete by IDs
await collection.delete({ ids: ['doc1', 'doc2'] });

// Delete by filter
await collection.delete({
    where: { category: 'archived' }
});
```

---

## Complete Example: RAG Application

### Python

```python
from achillesdb import AdminClient, Client
from achillesdb.embeddings import OpenAIEmbedding

# Setup
admin = AdminClient(host="localhost", port=8180)
admin.create_database("rag_app")

client = Client(host="localhost", port=8180, database="rag_app")

# Create collection with embedding function
embedding_fn = OpenAIEmbedding(api_key="sk-...", model="text-embedding-3-small")
knowledge_base = client.create_collection(
    name="knowledge_base",
    embedding_function=embedding_fn
)

# Add documents to knowledge base
documents = [
    "AchillesDB is a vector database for AI applications.",
    "Vector databases store high-dimensional embeddings.",
    "Semantic search uses embeddings to find similar documents.",
    "RAG combines retrieval with language model generation."
]

knowledge_base.add(
    ids=[f"doc{i}" for i in range(len(documents))],
    documents=documents,
    metadatas=[{"source": "docs", "index": i} for i in range(len(documents))]
)

# Query the knowledge base
user_query = "What is a vector database?"

results = knowledge_base.query(
    query_texts=[user_query],
    n_results=3,
    include=["documents", "metadatas", "distances"]
)

# Get context for LLM
context_docs = results.documents[0]
context = "\n".join(context_docs)

print(f"Query: {user_query}")
print(f"\nRetrieved context:\n{context}")

# Use context with your LLM of choice
# response = your_llm.generate(context=context, query=user_query)
```

### TypeScript

```typescript
import { AdminClient, Client } from 'achillesdb';
import { OpenAIEmbedding } from 'achillesdb/embeddings';

async function main() {
    // Setup
    const admin = new AdminClient({ host: 'localhost', port: 8180 });
    await admin.createDatabase('rag_app');

    const client = new Client({
        host: 'localhost',
        port: 8180,
        database: 'rag_app'
    });

    // Create collection
    const embeddingFn = new OpenAIEmbedding({
        apiKey: 'sk-...',
        model: 'text-embedding-3-small'
    });

    const knowledgeBase = await client.createCollection({
        name: 'knowledge_base',
        embeddingFunction: embeddingFn
    });

    // Add documents
    const documents = [
        'AchillesDB is a vector database for AI applications.',
        'Vector databases store high-dimensional embeddings.',
        'Semantic search uses embeddings to find similar documents.',
        'RAG combines retrieval with language model generation.'
    ];

    await knowledgeBase.add({
        ids: documents.map((_, i) => `doc${i}`),
        documents,
        metadatas: documents.map((_, i) => ({ source: 'docs', index: i }))
    });

    // Query
    const userQuery = 'What is a vector database?';

    const results = await knowledgeBase.query({
        queryTexts: [userQuery],
        nResults: 3,
        include: ['documents', 'metadatas', 'distances']
    });

    // Get context
    const contextDocs = results.documents[0];
    const context = contextDocs.join('\n');

    console.log(`Query: ${userQuery}`);
    console.log(`\nRetrieved context:\n${context}`);
}

main();
```
