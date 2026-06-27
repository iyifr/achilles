##  High level design.
A vector database enables fast retrieval of relevant documents from a large corpus by comparing query embeddings against stored document embeddings. 

To build a vector database you need the following (simplified) - 

1. Document store.
2. An embedding store to store document embeddings.
3. A fast search function to get the top *k* embeddings in the dataset closest to the query embedding.
4. A mapping between the embedding store and the document store so we know what document pieces own a relevant embedding at query time

---

### The Mapping: Internal IDs
**One int64 serves as both the FAISS vector ID and the WiredTiger document key**.

When a document is inserted:
1. A monotonically incrementing `internal_id` (int64) is assigned and stored in `CollectionStats.NextInternalId`.
2. The document row is written to WiredTiger using that int64 as the key (big-endian encoded so byte order matches numeric order).
3. The vector is added to FAISS using the same int64 as the label, via `AddWithIds`.
4. A thin alias entry maps the user-facing string ID → int64 in `table:doc_id_alias`.

```
    User string ID  ──►  doc_id_alias table  ──►  int64 internal ID
                                                        │
                                         ┌──────────────┴──────────────┐
                                         │                             │
                                         ▼                             ▼
                                   WiredTiger key               FAISS vector label
                                   (document row)               (similarity index)
```

This means a FAISS search hit is directly usable as a document table key — no secondary lookup needed. The old separate labels table (`table:label_docID`) is gone.

---

### Pillar 1: Document Storage

```
    ╔══════════════════════════════════════════════════════════════════╗
    ║  WIREDTIGER + BSON                                               ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║                                                                  ║
    ║    int64 key (big-endian)  ──────►  Document (BSON bytes)        ║
    ║                                                                  ║
    ║    ┌──────────┐           ┌─────────────────────────────────┐    ║
    ║    │ \x00...1 │ ────────► │ { id: "doc-001",                │    ║
    ║    └──────────┘           │   content: "...",               │    ║
    ║                           │   metadata: {                   │    ║
    ║                           │     tags: ["ml", "python"],     │    ║
    ║                           │     author: { name: "jane" },   │    ║
    ║                           │     acls: ["admin", "reader"]   │    ║
    ║                           │   }                             │    ║
    ║                           │ }                               │    ║
    ║                           └─────────────────────────────────┘    ║
    ║                                                                  ║
    ║  Keys are big-endian so WiredTiger's byte-lexicographic          ║
    ║  ordering matches numeric ordering. Full-table scans work        ║
    ║  correctly without a separate index.                             ║
    ║                                                                  ║
    ╚══════════════════════════════════════════════════════════════════╝
```

BSON (Binary-JSON) works well for this usecase. In Go, the bson library lets you marshal arbitrary structs (representing objects) into 
`[]byte`. We can store this in a WiredTiger table.

---

### Pillar 2: Vector Search & Storage
Handled by the FAISS library.

```
    ╔══════════════════════════════════════════════════════════════════╗
    ║  FAISS INDEX LIFECYCLE                                           ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║                                                                  ║
    ║     ┌─────────────┐      ┌─────────────┐      ┌─────────────┐    ║
    ║     │   CREATE    │      │     ADD     │      │   SEARCH    │    ║
    ║     │   INDEX     │─────►│   VECTORS   │─────►│   TOP-K     │    ║
    ║     └─────────────┘      └─────────────┘      └─────────────┘    ║
    ║           │                    │                    │            ║
    ║           ▼                    ▼                    ▼            ║
    ║     ┌─────────────┐      ┌─────────────┐      ┌─────────────┐    ║
    ║     │  • Flat     │      │ AddWithIds  │      │  Returns:   │    ║
    ║     │  • HNSW     │      │  int64 IDs  │      │  (int64s,   │    ║
    ║     │  • IVF      │      │  (stable)   │      │  distances) │    ║
    ║     └─────────────┘      └─────────────┘      └─────────────┘    ║
    ║                                                                  ║
    ║  The index is wrapped in an IndexIDMap so vectors are            ║
    ║  stored under caller-assigned int64 IDs rather than             ║
    ║  implicit sequential positions. This makes deletion safe:        ║
    ║  RemoveIds([]int64{...}) removes exactly the right vectors       ║
    ║  even after other documents have been deleted.                   ║
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
    │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   │
    │                                                                   │
    │     FAISS.search(query, k=5)  ──────►  ids: [42, 17, 89, ...]    │
    │                                        distances: [0.1, 0.2, ...] │
    │                                                                   │
    │     These ids ARE the WiredTiger document keys.                   │
    │     No intermediate lookup required.                              │
    └───────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │  STEP 2                                                           │
    │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   │
    │                                                                   │
    │     WiredTiger.get(encode(42))  ──────►  [BSON bytes]            │
    │     WiredTiger.get(encode(17))  ──────►  [BSON bytes]            │
    │     WiredTiger.get(encode(89))  ──────►  [BSON bytes]            │
    │                                                                   │
    └───────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │  STEP 3                                                           │
    │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   │
    │                                                                   │
    │     bson.Unmarshal(bytes)  ──────►  Document{                     │
    │                                       ID: "doc-001",             │
    │                                       Content: "...",            │
    │                                       Metadata: {...}            │
    │                                     }                            │
    │                                                                   │
    └───────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ╭───────────────────────────────────────────────────────────────────╮
    │                      📄 RETRIEVED DOCUMENTS                       │
    ╰───────────────────────────────────────────────────────────────────╯
```

---

### Key Tables (WiredTiger)

| Table | Key | Value | Purpose |
|-------|-----|-------|---------|
| `table:_catalog` | `db:<name>` or `<ns>` | BSON | Database and collection metadata |
| `table:_stats` | `<ns>` | BSON | Doc count, index size, next internal ID |
| `table:doc_id_alias` | user string ID | int64 string | Maps user IDs → internal IDs |
| `table:collection-{name}-{db}` | int64 (big-endian) | BSON | Per-collection document storage |

The alias table is the only place user-facing string IDs appear as keys. Everything else runs on int64s.
