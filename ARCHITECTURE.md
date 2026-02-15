##  High level design.
A vector database enables fast retrieval of relevant documents from a large corpus by comparing query embeddings against stored document embeddings. 

This requires **two core components**:

1. Document store
2. A vector index

Then you need a bridge between the two abstractions.

```
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                                                                         │
    │   ╭─────────────────────────╮         ╭─────────────────────────╮      │
    │   │                         │         │                         │      │
    │   │   📄 DOCUMENT STORE     │          |  🔍 VECTOR INDEX       │      │
    │   │                         │         │                         │      │
    │   │   WiredTiger + BSON     │         │       FAISS             │      │
    │   │                         │         │                         │      │
    │   │  • CRUD operations      │         │  • Similarity search    │      │
    │   │  • Metadata storage     │         │  • Top-K retrieval      │      │
    │   │  • B-tree indexing      │         │  • Optimized ANN algorithms    │
    │   │                         │         │                         │      │
    │   ╰─────────────────────────╯         ╰─────────────────────────╯      │
    │              │                                    │                    │
    │              │         ╭──────────────╮          │                    │
    │              ╰────────►│  LABELS KV   │◄─────────╯                    │
    │                        │   Table      │                                │
    │                        │              │                                │
    │                        │ FAISS_ID ──► │                                │
    │                        │   DOC_ID     │                                │
    │                        ╰──────────────╯                                │
    │                              ▲                                         │
    │                              │                                         │
    │                        THE BRIDGE                                      │
    │                                                                        │
    └────────────────────────────────────────────────────────────────────────┘
```

---

### Pillar 1: Document Storage

```
    ╔══════════════════════════════════════════════════════════════════╗
    ║  WIREDTIGER + BSON                                               ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║                                                                  ║
    ║    ID (Key)  ──────────►  Document (BSON bytes)                  ║
    ║                                                                  ║
    ║    ┌──────────┐           ┌─────────────────────────────────┐   ║
    ║    │"doc-001" │ ────────► │ { content: "...",               │   ║
    ║    └──────────┘           │   metadata: {                   │   ║
    ║                           │     tags: ["ml", "python"],     │   ║
    ║                           │     author: { name: "jane" },   │   ║
    ║                           │     acls: ["admin", "reader"]   │   ║
    ║                           │   }                             │   ║
    ║                           │ }                               │   ║
    ║                           └─────────────────────────────────┘   ║
    ║                                                                  ║
    ║    ✓ Nested objects in metadata? No problem!                    ║
    ║    ✓ Array query filters? No problem!                           ║
    ║                                                                  ║
    ╚══════════════════════════════════════════════════════════════════╝
```

BSON (Binary-JSON) is awesome for this usecase. In Go, the bson library lets you marshal arbitrary structs (representing objects) into `[]byte`. We can store this in a Wiredtiger table.

---

### Pillar 2: Vector Search

```
    ╔══════════════════════════════════════════════════════════════════╗
    ║  FAISS INDEX LIFECYCLE                                           ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║                                                                  ║
    ║     ┌─────────────┐      ┌─────────────┐      ┌─────────────┐   ║
    ║     │   CREATE    │      │     ADD     │      │   SEARCH    │   ║
    ║     │   INDEX     │─────►│   VECTORS   │─────►│   TOP-K     │   ║
    ║     └─────────────┘      └─────────────┘      └─────────────┘   ║
    ║           │                    │                    │           ║
    ║           ▼                    ▼                    ▼           ║
    ║     ┌─────────────┐      ┌─────────────┐      ┌─────────────┐   ║
    ║     │  • Flat     │      │ Auto-assigns│      │  Returns:   │   ║
    ║     │  • HNSW     │      │   labels    │      │  (labels,   │   ║
    ║     │  • IVF      │      │   1, 2, 3…  │      │  distances) │   ║
    ║     └─────────────┘      └─────────────┘      └─────────────┘   ║
    ║                                                                  ║
    ╚══════════════════════════════════════════════════════════════════╝
```

---

### The Data Flow: Query → Results

```
    ╭───────────────────────────────────────────────────────────────────╮
    │                        🔎 QUERY EMBEDDING                         │
    │                     [0.15, -0.32, 0.51, ...]                      │
    ╰───────────────────────────────────────────────────────────────────╯
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │  STEP 1                                                           │
    │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
    │                                                                   │
    │     FAISS.search(query, k=5)  ──────►  labels: [42, 17, 89, ...]  │
    │                                        distances: [0.1, 0.2, ...] │
    │                                                                   │
    └───────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │  STEP 2                                                           │
    │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
    │                                                                   │
    │     Labels Table:  42 ──────► "article-1"                         │
    │                    17 ──────► "article-7"                         │
    │                    89 ──────► "article-3"                         │
    │                                                                   │
    └───────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │  STEP 3                                                           │
    │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
    │                                                                   │
    │     WiredTiger.get("article-1")  ──────►  [BSON bytes]            │
    │                                                                   │
    └───────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │  STEP 4                                                           │
    │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
    │                                                                   │
    │     bson.Unmarshal(bytes)  ──────►  GlowstickDocument{            │
    │                                       ID: "article-1",            │
    │                                       Content: "...",             │
    │                                       Metadata: {...}             │
    │                                     }                             │
    │                                                                   │
    └───────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ╭───────────────────────────────────────────────────────────────────╮
    │                      📄 RETRIEVED DOCUMENTS                       │
    ╰───────────────────────────────────────────────────────────────────╯
```

---

### Hybrid Search

In some RAG workflows, retrieving semantically similar chunks isn't enough—you need the _right_ chunks for that user at that time. This is where structured metadata comes in:

```
    ┌─────────────────────────────────────────────────────────────────────┐
    │                                                                     │
    │     User Query: "Who are the highest performing engs on
    |     Team Object storage"                                            │
    │     User ACLs:  ["IT-managers", "Execs"]                            │
    │                                                                     │
    │                            ▼                                        │
    │                                                                     │
    │     ┌─────────────────────────────────────────────────────────┐    │
    │     │  Vector Search         Metadata Filter                   │    │
    │     │       │                      │                           │    │
    │     │       ▼                      ▼                           │    │
    │     │  ┌─────────┐           ┌──────────────────────┐         │    │
    │     │  │ Top 100 │  ──────►  │ WHERE                │         │    │
    │     │  │ similar │           │   acls $arrContains  │         │    │
    │     │  │ chunks  │           │   ["managers"]       │         │    │
    │     │  └─────────┘           └──────────────────────┘         │    │
    │     │                               │                          │    │
    │     │                               ▼                          │    │
    │     │                        ┌─────────────┐                   │    │
    │     │                        │  Top 10     │                   │    │
    │     │                        │  PERMITTED  │                   │    │
    │     │                        │  results    │                   │    │
    │     │                        └─────────────┘                   │    │
    │     └─────────────────────────────────────────────────────────┘    │
    │                                                                     │
    └─────────────────────────────────────────────────────────────────────┘
```

---
