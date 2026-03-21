"""
tests/integration/test_query.py
================================
Integration tests for query and query_collections.
Requires a running AchillesDB server.

These tests verify that where filters are correctly transmitted
to and interpreted by the server — something mocked tests cannot do.
"""

import pytest
from achillesdb.errors import AchillesError
from achillesdb.where import W
from tests.integration.conftest import skip_if_no_server


@skip_if_no_server
@pytest.mark.integration
class TestQueryDocumentsIntegration:

    @pytest.fixture(autouse=True)
    def seeded_collection(self, sync_db, sample_embeddings):
        import uuid
        self.emb = sample_embeddings
        name = f"query_coll_{uuid.uuid4().hex[:8]}"
        coll = sync_db.create_collection(name)

        # seed with known documents and metadata for filter assertions
        coll.add_documents(
            ids=["doc-1", "doc-2", "doc-3", "doc-4", "doc-5"],
            documents=[
                "apple is sweet",
                "banana is yellow",
                "cherry is tart",
                "dog is loyal",
                "cat is independent",
            ],
            embeddings=[
                self.emb["apple"],
                self.emb["banana"],
                self.emb["cherry"],
                self.emb["apple"],   # reuse close embedding
                self.emb["banana"],  # reuse close embedding
            ],
            metadatas=[
                {"category": "fruit", "year": 2021, "popular": True},
                {"category": "fruit", "year": 2022, "popular": True},
                {"category": "fruit", "year": 2020, "popular": False},
                {"category": "animal", "year": 2023, "popular": True},
                {"category": "animal", "year": 2019, "popular": False},
            ],
        )
        self.collection = coll
        yield coll

    def test_basic_query_returns_results(self):
        results = self.collection.query(
            top_k=3,
            query_embedding=self.emb["query"],
        )
        assert isinstance(results, list)
        assert len(results) <= 3

    def test_results_have_distance_field(self):
        results = self.collection.query(
            top_k=3,
            query_embedding=self.emb["query"],
        )
        assert all("distance" in r for r in results)

    def test_top_k_respected(self):
        results = self.collection.query(
            top_k=2,
            query_embedding=self.emb["query"],
        )
        assert len(results) <= 2

    def test_where_equality_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.eq("category", "fruit"),
        )
        for r in results:
            assert r["metadata"]["category"] == "fruit"

    def test_where_gt_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.gt("year", 2020),
        )
        for r in results:
            assert r["metadata"]["year"] > 2020

    def test_where_gte_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.gte("year", 2021),
        )
        for r in results:
            assert r["metadata"]["year"] >= 2021

    def test_where_lt_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.lt("year", 2022),
        )
        for r in results:
            assert r["metadata"]["year"] < 2022

    def test_where_lte_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.lte("year", 2021),
        )
        for r in results:
            assert r["metadata"]["year"] <= 2021

    def test_where_ne_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.ne("popular", False),
        )
        for r in results:
            assert r["metadata"]["popular"] is not False

    def test_where_in_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.in_("year", [2021, 2022]),
        )
        for r in results:
            assert r["metadata"]["year"] in [2021, 2022]

    def test_where_and_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.and_(
                W.eq("category", "fruit"),
                W.gt("year", 2020),
            ),
        )
        for r in results:
            assert r["metadata"]["category"] == "fruit"
            assert r["metadata"]["year"] > 2020

    def test_where_or_filter(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=W.or_(
                W.eq("category", "fruit"),
                W.eq("category", "animal"),
            ),
        )
        # all docs match one side or the other — should return all 5
        assert len(results) == 5

    def test_where_as_raw_dict(self):
        # plain dict where clause must work end-to-end (auto-normalized to WhereClause)
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where={"year": {"$gte": 2022}},
        )
        for r in results:
            assert r["metadata"]["year"] >= 2022

    def test_where_none_returns_all(self):
        results = self.collection.query(
            top_k=10,
            query_embedding=self.emb["query"],
            where=None,
        )
        assert len(results) == 5


# @skip_if_no_server
# @pytest.mark.integration
# @pytest.mark.server_bug
# class TestQueryCollectionsIntegration:
# 
#     @pytest.fixture(autouse=True)
#     def seeded_collections(self, sync_db, sample_embeddings):
#         import uuid
#         self.emb = sample_embeddings
#         suffix = uuid.uuid4().hex[:8]
# 
#         coll_a = sync_db.create_collection(f"fruits_{suffix}")
#         coll_b = sync_db.create_collection(f"animals_{suffix}")
# 
#         coll_a.add_documents(
#             ids=["f1", "f2"],
#             documents=["apple is sweet", "banana is yellow"],
#             embeddings=[self.emb["apple"], self.emb["banana"]],
#             metadatas=[
194: #                 {"category": "fruit", "year": 2021},
194: #                 {"category": "fruit", "year": 2022},
194: #             ],
194: #         )
194: #         coll_b.add_documents(
194: #             ids=["a1", "a2"],
194: #             documents=["dog is loyal", "cat is independent"],
194: #             embeddings=[self.emb["apple"], self.emb["banana"]],
194: #             metadatas=[
194: #                 {"category": "animal", "year": 2023},
194: #                 {"category": "animal", "year": 2019},
194: #             ],
194: #         )
194: # 
194: #         self.db = sync_db
194: #         self.coll_a_name = f"fruits_{suffix}"
194: #         self.coll_b_name = f"animals_{suffix}"
194: #         yield
194: # 
194: #     def test_query_collections_returns_results_from_both(self):
194: #         results = self.db.query_collections(
194: #             collection_names=[self.coll_a_name, self.coll_b_name],
194: #             top_k=4,
194: #             query_embedding=self.emb["query"],
194: #         )
194: #         ids = [r["id"] for r in results]
194: #         assert any(i.startswith("f") for i in ids), f"Expected 'f' items in {ids}"
194: #         assert any(i.startswith("a") for i in ids), f"Expected 'a' items in {ids}"
194: #         assert any(i.startswith("f") for i in ids)
194: #         assert any(i.startswith("a") for i in ids)
194: # 
194: #     def test_query_collections_respects_top_k(self):
194: #         results = self.db.query_collections(
194: #             collection_names=[self.coll_a_name, self.coll_b_name],
194: #             top_k=2,
194: #             query_embedding=self.emb["query"],
194: #         )
194: #         assert len(results) <= 2
194: # 
194: #     def test_query_collections_with_where_filter(self):
194: #         results = self.db.query_collections(
194: #             collection_names=[self.coll_a_name, self.coll_b_name],
194: #             top_k=10,
194: #             query_embedding=self.emb["query"],
194: #             where=W.gt("year", 2020),
194: #         )
194: #         for r in results:
194: #             assert r["metadata"]["year"] > 2020
