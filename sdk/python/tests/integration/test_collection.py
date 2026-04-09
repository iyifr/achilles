import pytest
from achillesdb.errors import AchillesError
from tests.integration.conftest import skip_if_no_server


@skip_if_no_server
@pytest.mark.integration
class TestSyncCollectionIntegration:

    @pytest.fixture(autouse=True)
    def collection(self, sync_db, sample_embeddings):
        import uuid
        self.emb = sample_embeddings
        name = f"test_coll_{uuid.uuid4().hex[:8]}"
        coll = sync_db.create_collection(name)
        yield coll

    def test_add_documents_succeeds(self, collection):
        collection.add_documents(
            ids=["doc-1", "doc-2"],
            documents=["apple is sweet", "banana is yellow"],
            embeddings=[self.emb["apple"], self.emb["banana"]],
            metadatas=[{"year": 2021}, {"year": 2022}],
        )

    def test_count_after_add(self, collection):
        collection.add_documents(
            ids=["doc-1", "doc-2", "doc-3"],
            documents=["a", "b", "c"],
            embeddings=[self.emb["apple"], self.emb["banana"], self.emb["cherry"]],
            metadatas=[{}, {}, {}],
        )
        assert collection.count() == 3

    def test_get_documents_returns_inserted_docs(self, collection):
        collection.add_documents(
            ids=["doc-1"],
            documents=["apple doc"],
            embeddings=[self.emb["apple"]],
            metadatas=[{"category": "fruit"}],
        )
        docs = collection.get_documents()
        ids = [d["id"] for d in docs]
        assert "doc-1" in ids

    def test_peek_returns_at_most_n_docs(self, collection):
        collection.add_documents(
            ids=["doc-1", "doc-2", "doc-3", "doc-4", "doc-5"],
            documents=["a", "b", "c", "d", "e"],
            embeddings=[
                self.emb["apple"], self.emb["banana"], self.emb["cherry"],
                [0.1, 0.1, 0.1, 0.1], [0.2, 0.2, 0.2, 0.2],
            ],
            metadatas=[{}, {}, {}, {}, {}],
        )
        peeked = collection.peek(n=3)
        assert len(peeked) <= 3

    def test_update_document_metadata(self, collection):
        collection.add_documents(
            ids=["doc-1"],
            documents=["apple doc"],
            embeddings=[self.emb["apple"]],
            metadatas=[{"popular": True}],
        )
        collection.update_document(
            document_id="doc-1",
            updates={"popular": False},
        )
        # verify by fetching
        docs = collection.get_documents()
        doc = next(d for d in docs if d["id"] == "doc-1")
        assert doc["metadata"]["popular"] is False

    def test_delete_document_reduces_count(self, collection):
        collection.add_documents(
            ids=["doc-1", "doc-2"],
            documents=["a", "b"],
            embeddings=[self.emb["apple"], self.emb["banana"]],
            metadatas=[{}, {}],
        )
        res = collection.delete_documents(document_ids=["doc-1"])
        assert collection.count() == 1
        assert res["deleted_count"] == 1
        assert "doc-1" in res["deleted_ids"]

    def test_delete_multiple_documents(self, collection):
        collection.add_documents(
            ids=["doc-1", "doc-2", "doc-3"],
            documents=["a", "b", "c"],
            embeddings=[self.emb["apple"], self.emb["banana"], self.emb["cherry"]],
            metadatas=[{}, {}, {}],
        )
        res = collection.delete_documents(document_ids=["doc-1", "doc-2"])
        remaining = collection.get_documents()
        ids = [d["id"] for d in remaining]
        assert "doc-1" not in ids
        assert "doc-2" not in ids
        assert "doc-3" in ids
        assert res["deleted_count"] == 2
        assert set(res["deleted_ids"]) == {"doc-1", "doc-2"}

    def test_add_documents_mismatched_lengths_raises(self, collection):
        with pytest.raises((ValueError, AchillesError)):
            collection.add_documents(
                ids=["doc-1", "doc-2"],
                documents=["only one"],
                embeddings=[self.emb["apple"], self.emb["banana"]],
                metadatas=[{}, {}],
            )

    def test_add_documents_duplicate_ids_raises(self, collection):
        with pytest.raises((ValueError, AchillesError)):
            collection.add_documents(
                ids=["dup", "dup"],
                documents=["a", "b"],
                embeddings=[self.emb["apple"], self.emb["banana"]],
                metadatas=[{}, {}],
            )
