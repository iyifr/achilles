"""
sandbox_sync.py
===============
Comprehensive sandbox / manual-test file for the **synchronous** AchillesDB SDK.

Each section lives in its own function so you can comment-out whichever parts
you don't want to run in a given session.

NOTE: No embedding function is used.  Embeddings are supplied directly as
      Python lists of floats throughout the file.

Sections
--------
 1. test_client          – connect, ping, list databases, create database
 2. test_database        – list collections, create collection, get collection
 3. test_add_documents   – add_documents (with metadata, without, single, before_insert)
 4. test_get_docs        – get_documents, count, peek
 5. test_query           – query_documents (by embedding, all where filter variants)
 6. test_update          – update_documents
 7. test_delete_docs     – delete_documents (single & batch)
 8. test_query_colls     – query_collections (cross-collection)
 9. test_context_manager – with-statement usage
10. test_errors          – error / edge-case demonstrations

Run from the repo root:
    python3 tests/sandbox_sync.py
"""

import pprint
from achillesdb import AchillesClient
from achillesdb.errors import AchillesError
from achillesdb.where import W

# ── pretty-printer ────────────────────────────────────────────────────────────
pp = pprint.PrettyPrinter(indent=2, width=100)


def sep(title: str) -> None:
    bar = "─" * 70
    print(f"\n{bar}\n  {title}\n{bar}")


# ── connection / shared names ─────────────────────────────────────────────────
HOST     = "localhost"
PORT     = 8180
DB_NAME  = "sandbox_sync_db2"
COLL_A   = "articles"
COLL_B   = "books"

# 4-dimensional dummy embeddings (all vectors must share the same dimension)
EMB_APPLE       = [0.1, 0.2, 0.3, 0.4]
EMB_BANANA      = [0.5, 0.6, 0.7, 0.8]
EMB_CHERRY      = [0.9, 0.8, 0.7, 0.6]
EMB_DOG         = [0.1, 0.1, 0.9, 0.9]
EMB_CAT         = [0.2, 0.2, 0.8, 0.8]
EMB_QUERY_FRUIT = [0.3, 0.4, 0.5, 0.6]
EMB_QUERY_ANIM  = [0.15, 0.15, 0.85, 0.85]


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1 – Client
# ═════════════════════════════════════════════════════════════════════════════

def test_client(client: AchillesClient):
    sep("SECTION 1 – Client: connect, ping, list & create databases")

    # 1a. ping
    result = client.ping()
    print(f"ping() → {result}")

    # 1b. list_databases
    databases = client.list_databases()
    print(f"list_databases() → {[db.name for db in databases]}")

    # 1c. create_database (idempotent – falls back to existing DB on conflict)
    try:
        db = client.create_database(DB_NAME)
        print(f"create_database('{DB_NAME}') → {db}")
    except AchillesError as e:
        print(f"create_database('{DB_NAME}') skipped – already exists: {e}")
        db = client.database(DB_NAME)

    # 1d. get existing database handle (no server call)
    db_ref = client.database(DB_NAME)
    print(f"database('{DB_NAME}') ref → {db_ref}")

    # 1e. list_databases after creation
    databases = client.list_databases()
    print(f"list_databases() after create → {[d.name for d in databases]}")

    return db


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2 – Database
# ═════════════════════════════════════════════════════════════════════════════

def test_database(db):
    sep("SECTION 2 – Database: list & create collections, get collection")

    # 2a. list_collections (initially empty)
    colls = db.list_collections()
    print(f"list_collections() (before create) → {colls}")

    # 2b. create_collection (idempotent – falls back to existing on conflict)
    try:
        coll_a = db.create_collection(COLL_A)
        print(f"create_collection('{COLL_A}') → {coll_a}")
    except AchillesError as e:
        print(f"create_collection('{COLL_A}') skipped – already exists: {e}")
        coll_a = db.get_collection(COLL_A)

    try:
        coll_b = db.create_collection(COLL_B)
        print(f"create_collection('{COLL_B}') → {coll_b}")
    except AchillesError as e:
        print(f"create_collection('{COLL_B}') skipped – already exists: {e}")
        coll_b = db.get_collection(COLL_B)

    # 2c. list_collections after creation
    colls = db.list_collections()
    print(f"list_collections() after create → {[c.name for c in colls]}")

    # 2d. get_collection
    fetched = db.get_collection(COLL_A)
    print(f"get_collection('{COLL_A}') → id={fetched.id}, name={fetched.name}")

    # 2e. collection() handle (resolves via get_collection internally)
    coll_a_handle = db.collection(COLL_A)
    print(f"collection('{COLL_A}') from db → {coll_a_handle}")

    return coll_a, coll_b


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3 – add_documents
# ═════════════════════════════════════════════════════════════════════════════

def test_add_documents(coll_a, coll_b):
    sep("SECTION 3 – add_documents")

    # 3a. add documents WITH metadata
    coll_a.add_documents(
        ids=["doc-1", "doc-2", "doc-3"],
        documents=[
            "The apple is a sweet red fruit.",
            "Bananas are rich in potassium.",
            "Cherries are small and tart.",
        ],
        embeddings=[EMB_APPLE, EMB_BANANA, EMB_CHERRY],
        metadatas=[
            {"category": "fruit", "year": 2021, "popular": True},
            {"category": "fruit", "year": 2022, "popular": True},
            {"category": "fruit", "year": 2020, "popular": False},
        ],
    )
    print("add_documents() – 3 fruit docs added to COLL_A  ✓")

    # 3b. add documents to COLL_B
    coll_b.add_documents(
        ids=["bk-1", "bk-2"],
        documents=[
            "Dogs are loyal and friendly animals.",
            "Cats are independent but affectionate.",
        ],
        embeddings=[EMB_DOG, EMB_CAT],
        metadatas=[
            {"category": "animal", "year": 2023},
            {"category": "animal", "year": 2019},
        ],
    )
    print("add_documents() – 2 animal docs added to COLL_B  ✓")

    # 3c. add a single document
    coll_a.add_documents(
        ids=["doc-4"],
        documents=["Mangoes are tropical and sweet."],
        embeddings=[[0.4, 0.5, 0.6, 0.7]],
        metadatas=[{"category": "fruit", "year": 2024, "popular": True}],
    )
    print("add_documents() – single doc (doc-4) added to COLL_A  ✓")

#     # 3d. add with before_insert transformer
#     coll_a.add_documents(
#         ids=["doc-5"],
#         documents=["Lemons are sour citrus fruits."],
#         embeddings=[[0.05, 0.15, 0.25, 0.35]],
#         metadatas=[{"category": "fruit", "year": 2018, "popular": False}],
#         before_insert=lambda docs: [d.upper() for d in docs],
#     )
#     print("add_documents() – doc with before_insert (doc-5) added  ✓")


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4 – get_documents / count / peek
# ═════════════════════════════════════════════════════════════════════════════

def test_get_docs(coll_a):
    sep("SECTION 4 – get_documents / count / peek")

    # 4a. get_documents
    all_docs = coll_a.get_documents()
    print(f"get_documents() → {len(all_docs)} docs in '{COLL_A}'")
    pp.pprint(all_docs)

    # 4b. count
    n = coll_a.count()
    print(f"count() → {n}")

    # 4c. peek (default n=5)
    peeked = coll_a.peek()
    print(f"peek() → {len(peeked)} docs")
    pp.pprint([d.model_dump() for d in peeked])

    # 4d. peek with custom n
    peeked_2 = coll_a.peek(n=2)
    print(f"peek(n=2) → {len(peeked_2)} docs")
    pp.pprint([d.model_dump() for d in peeked_2])


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5 – query_documents
# ═════════════════════════════════════════════════════════════════════════════

def test_query(coll_a, coll_b):
    sep("SECTION 5 – query_documents")

    # 5a. basic vector query
    res = coll_a.query_documents(top_k=3, query_embedding=EMB_QUERY_FRUIT)
    print("query_documents(top_k=3, no filter):")
    pp.pprint(res)

    # 5b. where – equality (W.eq)
    res = coll_a.query_documents(
        top_k=500, query_embedding=EMB_QUERY_FRUIT,
        where=W.eq("category", "fruit"),
    )
    print("\nwhere=W.eq('category', 'fruit'):")
    pp.pprint(res)

    # 5c. where – greater-than (W.gt)
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.gt("year", 2020),
    )
    print("\nwhere=W.gt('year', 2020):")
    pp.pprint(res)

    # 5d. where – greater-than-or-equal (W.gte)
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.gte("year", 2021),
    )
    print("\nwhere=W.gte('year', 2021):")
    pp.pprint(res)

    # 5e. where – less-than (W.lt)
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.lt("year", 2022),
    )
    print("\nwhere=W.lt('year', 2022):")
    pp.pprint(res)

    # 5f. where – less-than-or-equal (W.lte)
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.lte("year", 2021),
    )
    print("\nwhere=W.lte('year', 2021):")
    pp.pprint(res)

    # 5g. where – not-equal (W.ne)
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.ne("popular", False),
    )
    print("\nwhere=W.ne('popular', False):")
    pp.pprint(res)

    # 5h. where – in list (W.in_)
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.in_("year", [2021, 2022, 2024]),
    )
    print("\nwhere=W.in_('year', [2021, 2022, 2024]):")
    pp.pprint(res)

    # 5i. where – $and compound
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.and_(W.eq("category", "fruit"), W.gt("year", 2020)),
    )
    print("\nwhere=W.and_(eq + gt):")
    pp.pprint(res)

    # 5j. where – $or compound
    res = coll_a.query_documents(
        top_k=5, query_embedding=EMB_QUERY_FRUIT,
        where=W.or_(W.eq("popular", True), W.lt("year", 2019)),
    )
    print("\nwhere=W.or_(eq + lt):")
    pp.pprint(res)

    # 5k. where – raw dict
    res = coll_a.query_documents(
        top_k=3, query_embedding=EMB_QUERY_FRUIT,
        where={"year": {"$gte": 2022}},
    )
    print("\nwhere=raw dict {'year': {'$gte': 2022}}:")
    pp.pprint(res)

    # 5l. query COLL_B
    res = coll_b.query_documents(top_k=2, query_embedding=EMB_QUERY_ANIM)
    print("\nquery_documents() in COLL_B (animals):")
    pp.pprint(res)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6 – update_documents
# ═════════════════════════════════════════════════════════════════════════════

def test_update(coll_a):
    sep("SECTION 6 – update_documents")

    # 6a. update metadata fields on doc-1
    coll_a.update_documents(
        document_id="doc-1",
        where={},
        updates={"popular": False, "reviewed": True},
    )
    print("update_documents('doc-1') → popular=False, reviewed=True  ✓")

    # verify
    for d in coll_a.get_documents():
        if d["id"] == "doc-1":
            print("  after update:", d)
            break

    # 6b. update year on doc-2
    coll_a.update_documents(
        document_id="doc-2",
        where={},
        updates={"year": 2025},
    )
    print("update_documents('doc-2') → year=2025  ✓")


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 7 – delete_documents
# ═════════════════════════════════════════════════════════════════════════════

def test_delete_docs(coll_a):
    sep("SECTION 7 – delete_documents")

    # 7a. delete a single document
    coll_a.delete_documents(document_ids=["doc-5"])
    print("delete_documents(['doc-5'])  ✓")
    print(f"count() after delete → {coll_a.count()}")

    # 7b. delete multiple documents
    coll_a.delete_documents(document_ids=["doc-3", "doc-4"])
    print("delete_documents(['doc-3', 'doc-4'])  ✓")
    print(f"count() after batch delete → {coll_a.count()}")

    # verify remaining
    remaining = coll_a.get_documents()
    print("remaining doc ids:", [d["id"] for d in remaining])


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 8 – query_collections (cross-collection)
# ═════════════════════════════════════════════════════════════════════════════

# def test_query_colls(db, coll_a):
#     sep("SECTION 8 – query_collections (cross-collection)")
#
#     # seed a couple of extra docs so there is material for cross-collection queries
#     coll_a.add_documents(
#         ids=["doc-10", "doc-11"],
#         documents=["Oranges are vitamin C rich.", "Grapes come in red and green."],
#         embeddings=[[0.3, 0.3, 0.5, 0.5], [0.2, 0.4, 0.4, 0.6]],
#         metadatas=[
#             {"category": "fruit", "year": 2023, "popular": True},
#             {"category": "fruit", "year": 2022, "popular": False},
#         ],
#     )
#
#     # 8a. basic cross-collection query
#     res = db.query_collections(
#         collection_names=[COLL_A, COLL_B],
#         top_k=4,
#         query_embedding=EMB_QUERY_FRUIT,
#     )
#     print(f"query_collections(['{COLL_A}', '{COLL_B}'], top_k=4):")
#     pp.pprint(res)
#
#     # 8b. cross-collection with where filter
#     res = db.query_collections(
#         collection_names=[COLL_A, COLL_B],
#         top_k=3,
#         query_embedding=EMB_QUERY_ANIM,
#         where=W.gt("year", 2020),
#     )
#     print("\nquery_collections with where=W.gt('year', 2020):")
#     pp.pprint(res)
#
#     # 8c. single-collection animal query via query_collections
#     res = db.query_collections(
#         collection_names=[COLL_B],
#         top_k=2,
#         query_embedding=EMB_QUERY_ANIM,
#     )
#     print("\nquery_collections(COLL_B only, animal query):")
#     pp.pprint(res)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 9 – Context-manager usage
# ═════════════════════════════════════════════════════════════════════════════

def test_context_manager():
    sep("SECTION 9 – Context manager (with-statement)")

    with AchillesClient(host=HOST, port=PORT) as c:
        print("Inside context manager, client:", c)
        dbs = c.list_databases()
        print("  list_databases():", [d.name for d in dbs])

        try:
            _db = c.create_database("ctx_mgr_db")
        except AchillesError as e:
            print(f"  create_database('ctx_mgr_db') skipped – already exists: {e}")
            _db = c.database("ctx_mgr_db")

        try:
            _coll = _db.create_collection("ctx_coll")
        except AchillesError as e:
            print(f"  create_collection('ctx_coll') skipped – already exists: {e}")
            _coll = _db.get_collection("ctx_coll")

        _coll.add_documents(
            ids=["ctx-1"],
            documents=["Context manager test document."],
            embeddings=[[0.5, 0.5, 0.5, 0.5]],
            metadatas=[{"source": "ctx_test"}],
        )
        print("  count() inside ctx:", _coll.count())

    print("Connection closed after exiting context manager  ✓")


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 10 – Error / edge-case demonstrations
# ═════════════════════════════════════════════════════════════════════════════

def test_errors(db, coll_a):
    sep("SECTION 10 – Error / edge-case demos")

    # 10a. get a collection that doesn't exist
    print("\n[10a] get_collection on non-existent name:")
    try:
        db.get_collection("does_not_exist_xyz")
        print("  ✗ expected AchillesError, but none was raised")
    except AchillesError as e:
        print(f"  ✓ AchillesError caught: {e}")
    except Exception as e:
        print(f"  ! Unexpected exception ({type(e).__name__}): {e}")

    # 10b. mismatched array lengths
    print("\n[10b] add_documents with mismatched array lengths:")
    try:
        coll_a.add_documents(
            ids=["x-1", "x-2"],
            documents=["only one doc"],
            embeddings=[[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]],
            metadatas=[{}],
        )
        print("  ✗ expected an error, but none was raised")
    except (ValueError, AchillesError) as e:
        print(f"  ✓ Error caught: {e}")

    # 10c. duplicate IDs in the same batch
    print("\n[10c] add_documents with duplicate IDs:")
    try:
        coll_a.add_documents(
            ids=["dup-1", "dup-1"],
            documents=["first", "second"],
            embeddings=[[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]],
            metadatas=[{}, {}],
        )
        print("  ✗ expected an error, but none was raised")
    except (ValueError, AchillesError) as e:
        print(f"  ✓ Error caught: {e}")

    # 10d. empty embedding vector
    print("\n[10d] add_documents with empty embedding vector:")
    try:
        coll_a.add_documents(
            ids=["e-1"],
            documents=["doc"],
            embeddings=[[]],
            metadatas=[{}],
        )
        print("  ✗ expected an error, but none was raised")
    except (ValueError, AchillesError) as e:
        print(f"  ✓ Error caught: {e}")

    # 10e. query with no embedding and no embedding_function
    print("\n[10e] query_documents without query_embedding or embedding_function:")
    try:
        coll_a.query_documents(top_k=1)
        print("  ✗ expected AchillesError, but none was raised")
    except (AchillesError, ValueError) as e:
        print(f"  ✓ Error caught: {e}")

    # 10f. add with embeddings=None and no embedding_function set on client
    print("\n[10f] add_documents with embeddings=None and no embedding_function:")
    try:
        coll_a.add_documents(
            ids=["no-emb-1"],
            documents=["no embedding doc"],
            embeddings=None,
            metadatas=[{}],
        )
        print("  ✗ expected AchillesError, but none was raised")
    except AchillesError as e:
        print(f"  ✓ AchillesError caught: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN – comment out any section you don't want to run
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    client = AchillesClient(host=HOST, port=PORT)

    db = test_client(client)
    coll_a, coll_b = test_database(db)
    test_add_documents(coll_a, coll_b)
    test_get_docs(coll_a)
    test_query(coll_a, coll_b)
    test_update(coll_a)
    test_delete_docs(coll_a)
    # test_query_colls(db, coll_a)
    test_context_manager()
    test_errors(db, coll_a)

    sep("ALL SECTIONS COMPLETE")
    client.close()
    print("client.close()  ✓")
