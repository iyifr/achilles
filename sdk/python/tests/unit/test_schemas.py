"""
tests/unit/test_schemas.py
===========================
Unit tests for achillesdb/schemas.py

Covers:
  - GetCollectionsRes       (None normalization)
  - GetDocumentsRes         (None normalization)
  - QueryRes                (None normalization)
  - InsertDocumentReqInput  (all validators)
  - QueryReqInput           (model_dump where override)
  - ComparisonOp            (at_least_one — including falsy-zero bug)
  - WhereClause             (to_dict serialization, alias keys, nesting)
  - Document                (field defaults)
"""

import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

from achillesdb.schemas import (
    GetCollectionsRes,
    GetDocumentsRes,
    QueryRes,
    InsertDocumentReqInput,
    QueryReqInput,
    ComparisonOp,
    WhereClause,
    Document,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _catalog_entry_dict(**overrides) -> dict:
    """Minimal valid CollectionCatalogEntry payload."""
    base = {
        "_id": "abc123",
        "ns": "mydb.mycollection",
        "table_uri": "s3://bucket/table",
        "vector_index_uri": "s3://bucket/index",
        "createdAt": datetime.now(tz=timezone.utc).isoformat(),
        "updatedAt": datetime.now(tz=timezone.utc).isoformat(),
    }
    return {**base, **overrides}


def _document_dict(**overrides) -> dict:
    """Minimal valid Document payload."""
    base = {"id": "doc1", "content": "hello world"}
    return {**base, **overrides}


def _valid_insert(**overrides) -> dict:
    """Minimal valid InsertDocumentReqInput payload."""
    base = {
        "ids": ["a", "b"],
        "documents": ["doc a", "doc b"],
        "embeddings": [[0.1, 0.2], [0.3, 0.4]],
        "metadatas": [{}, {}],
    }
    return {**base, **overrides}


# ─────────────────────────────────────────────────────────────────────────────
# GetCollectionsRes
# ─────────────────────────────────────────────────────────────────────────────

class TestGetCollectionsRes:

    def test_collections_none_normalized_to_empty_list(self):
        res = GetCollectionsRes(collections=None, collection_count=0)
        assert res.collections == []

    def test_collections_empty_list_stays_empty(self):
        res = GetCollectionsRes(collections=[], collection_count=0)
        assert res.collections == []

    def test_collections_with_entries_parses_correctly(self):
        res = GetCollectionsRes(
            collections=[_catalog_entry_dict()],
            collection_count=1,
        )
        assert len(res.collections) == 1
        assert res.collections[0].ns == "mydb.mycollection"

    def test_collection_catalog_entry_id_parsed_from_underscore_id_alias(self):
        # CollectionCatalogEntry uses alias="_id" for the id field
        res = GetCollectionsRes(
            collections=[_catalog_entry_dict(_id="entry_xyz")],
            collection_count=1,
        )
        assert res.collections[0].id == "entry_xyz"

    def test_collection_count_preserved(self):
        res = GetCollectionsRes(collections=None, collection_count=5)
        assert res.collection_count == 5

    def test_multiple_entries_all_parsed(self):
        entries = [
            _catalog_entry_dict(_id=f"id{i}", ns=f"db.coll{i}")
            for i in range(3)
        ]
        res = GetCollectionsRes(collections=entries, collection_count=3)
        assert len(res.collections) == 3
        assert res.collections[2].ns == "db.coll2"


# ─────────────────────────────────────────────────────────────────────────────
# GetDocumentsRes
# ─────────────────────────────────────────────────────────────────────────────

class TestGetDocumentsRes:

    def test_documents_none_normalized_to_empty_list(self):
        res = GetDocumentsRes(documents=None, doc_count=0)
        assert res.documents == []

    def test_documents_empty_list_stays_empty(self):
        res = GetDocumentsRes(documents=[], doc_count=0)
        assert res.documents == []

    def test_documents_with_entries_parses_correctly(self):
        res = GetDocumentsRes(
            documents=[_document_dict()],
            doc_count=1,
        )
        assert len(res.documents) == 1
        assert res.documents[0].id == "doc1"

    def test_doc_count_preserved(self):
        res = GetDocumentsRes(documents=None, doc_count=42)
        assert res.doc_count == 42

    def test_normalize_returns_get_documents_res_instance(self):
        res = GetDocumentsRes(documents=None, doc_count=0)
        assert isinstance(res, GetDocumentsRes)


# ─────────────────────────────────────────────────────────────────────────────
# QueryRes
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryRes:

    def test_documents_none_normalized_to_empty_list(self):
        res = QueryRes(documents=None, doc_count=0)
        assert res.documents == []

    def test_documents_empty_list_stays_empty(self):
        res = QueryRes(documents=[], doc_count=0)
        assert res.documents == []

    def test_documents_with_entries_parses_correctly(self):
        res = QueryRes(
            documents=[_document_dict(distance=0.42)],
            doc_count=1,
        )
        assert len(res.documents) == 1
        assert res.documents[0].distance == pytest.approx(0.42)

    def test_doc_count_preserved(self):
        res = QueryRes(documents=None, doc_count=7)
        assert res.doc_count == 7

    def test_normalize_fires_even_when_doc_count_nonzero(self):
        # validator must run regardless of doc_count value
        res = QueryRes(documents=None, doc_count=3)
        assert res.documents == []


# ─────────────────────────────────────────────────────────────────────────────
# InsertDocumentReqInput
# ─────────────────────────────────────────────────────────────────────────────

class TestInsertDocumentReqInput:

    # ── happy path ────────────────────────────────────────────────────────────

    def test_valid_input_passes(self):
        obj = InsertDocumentReqInput(**_valid_insert())
        assert len(obj.ids) == 2

    def test_metadatas_defaults_to_empty_list_when_no_documents(self):
        # empty insert is valid — all arrays are length 0, equal lengths pass
        obj = InsertDocumentReqInput(
            ids=[],
            documents=[],
            embeddings=[],
        )
        assert obj.metadatas == []

    def test_metadatas_must_be_provided_when_documents_present(self):
        # omitting metadatas with documents present causes a length mismatch
        with pytest.raises(ValidationError):
            InsertDocumentReqInput(
                ids=["a"],
                documents=["doc a"],
                embeddings=[[0.1, 0.2]],
                # metadatas omitted — defaults to [] which is length 0, mismatch with ids length 1
            )

    def test_single_document_passes(self):
        InsertDocumentReqInput(
            ids=["only"],
            documents=["solo doc"],
            embeddings=[[1.0, 0.0]],
            metadatas=[{}],
        )

    def test_with_metadata_passes(self):
        InsertDocumentReqInput(**_valid_insert(
            metadatas=[{"author": "jane"}, {"author": "john"}]
        ))

    # ── empty embeddings ──────────────────────────────────────────────────────

    def test_empty_embeddings_list_raises(self):
        with pytest.raises(ValidationError, match="non-empty"):
            InsertDocumentReqInput(
                ids=["a"],
                documents=["doc"],
                embeddings=[],
            )

    # ── NaN / Inf validation ──────────────────────────────────────────────────

    def test_nan_in_embedding_raises(self):
        with pytest.raises(ValidationError, match="NaN or Inf"):
            InsertDocumentReqInput(
                ids=["a"],
                documents=["doc"],
                embeddings=[[float("nan"), 0.1]],
            )

    def test_positive_inf_in_embedding_raises(self):
        with pytest.raises(ValidationError, match="NaN or Inf"):
            InsertDocumentReqInput(
                ids=["a"],
                documents=["doc"],
                embeddings=[[float("inf"), 0.1]],
            )

    def test_negative_inf_in_embedding_raises(self):
        with pytest.raises(ValidationError, match="NaN or Inf"):
            InsertDocumentReqInput(
                ids=["a"],
                documents=["doc"],
                embeddings=[[float("-inf"), 0.1]],
            )

    def test_nan_in_second_vector_raises(self):
        with pytest.raises(ValidationError, match="NaN or Inf"):
            InsertDocumentReqInput(**_valid_insert(
                embeddings=[[0.1, 0.2], [float("nan"), 0.4]]
            ))

    def test_zero_values_in_embedding_pass(self):
        # 0.0 is finite — must not raise
        InsertDocumentReqInput(
            ids=["a"],
            documents=["doc"],
            embeddings=[[0.0, 0.0, 0.0]],
            metadatas=[{}],
        )

    # ── dimension consistency ─────────────────────────────────────────────────

    def test_consistent_embedding_dimensions_pass(self):
        InsertDocumentReqInput(**_valid_insert(
            ids=["a", "b", "c"],
            documents=["x", "y", "z"],
            embeddings=[[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
            metadatas=[{}, {}, {}],
        ))

    def test_embedding_dimension_mismatch_raises(self):
        with pytest.raises(ValidationError, match="dimension mismatch"):
            InsertDocumentReqInput(
                ids=["a", "b"],
                documents=["x", "y"],
                embeddings=[[0.1, 0.2], [0.3, 0.4, 0.5]],
            )

    def test_dimension_mismatch_error_includes_index(self):
        with pytest.raises(ValidationError, match="index 1"):
            InsertDocumentReqInput(
                ids=["a", "b"],
                documents=["x", "y"],
                embeddings=[[0.1, 0.2], [0.3, 0.4, 0.5]],
            )

    def test_dimension_mismatch_at_third_embedding_shows_index_2(self):
        with pytest.raises(ValidationError, match="index 2"):
            InsertDocumentReqInput(
                ids=["a", "b", "c"],
                documents=["x", "y", "z"],
                embeddings=[[0.1, 0.2], [0.3, 0.4], [0.5]],
            )

    # ── duplicate ids ─────────────────────────────────────────────────────────

    def test_duplicate_ids_raises(self):
        with pytest.raises(ValidationError, match="duplicates"):
            InsertDocumentReqInput(
                ids=["a", "a", "b"],
                documents=["x", "y", "z"],
                embeddings=[[0.1], [0.2], [0.3]],
            )

    def test_duplicate_id_value_appears_in_error_message(self):
        with pytest.raises(ValidationError, match="dup_id"):
            InsertDocumentReqInput(
                ids=["dup_id", "dup_id"],
                documents=["x", "y"],
                embeddings=[[0.1], [0.2]],
            )

    def test_all_unique_ids_pass(self):
        InsertDocumentReqInput(
            ids=["a", "b", "c"],
            documents=["x", "y", "z"],
            embeddings=[[0.1], [0.2], [0.3]],
            metadatas=[{}, {}, {}],
        )

    # ── array length mismatch ─────────────────────────────────────────────────

    def test_ids_shorter_than_documents_raises(self):
        with pytest.raises(ValidationError):
            InsertDocumentReqInput(
                ids=["a"],
                documents=["x", "y"],
                embeddings=[[0.1], [0.2]],
            )

    def test_embeddings_shorter_than_ids_raises(self):
        with pytest.raises(ValidationError):
            InsertDocumentReqInput(
                ids=["a", "b"],
                documents=["x", "y"],
                embeddings=[[0.1]],
            )

    def test_metadatas_shorter_than_ids_raises(self):
        with pytest.raises(ValidationError):
            InsertDocumentReqInput(
                ids=["a", "b"],
                documents=["x", "y"],
                embeddings=[[0.1], [0.2]],
                metadatas=[{"k": 1}],
            )


# ─────────────────────────────────────────────────────────────────────────────
# QueryReqInput
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryReqInput:
    """
    QueryReqInput overrides model_dump() to serialize WhereClause via to_dict().
    These tests verify that override behaves correctly.
    """

    def test_empty_query_embedding_raises(self):
        with pytest.raises(ValidationError, match="non-empty"):
            QueryReqInput(query_embedding=[], top_k=10)

    def test_valid_query_embedding_passes(self):
        q = QueryReqInput(query_embedding=[0.1, 0.2, 0.3], top_k=5)
        assert q.top_k == 5

    def test_top_k_defaults_to_10(self):
        q = QueryReqInput(query_embedding=[0.1])
        assert q.top_k == 10

    def test_model_dump_without_where_omits_where_key(self):
        q = QueryReqInput(query_embedding=[0.1, 0.2], top_k=10, where=None)
        dumped = q.model_dump(exclude_none=True)
        assert "where" not in dumped

    def test_model_dump_with_where_serializes_via_to_dict(self):
        # to_dict() uses by_alias=True so "$gt" not "gt" appears in output
        where = WhereClause(**{"year": {"$gt": 2020}})
        q = QueryReqInput(query_embedding=[0.1, 0.2], top_k=5, where=where)
        dumped = q.model_dump()
        assert "where" in dumped
        assert dumped["where"] == {"year": {"$gt": 2020}}

    def test_model_dump_where_uses_dollar_prefix_alias_keys(self):
        # $and not and_ must appear in serialized output
        inner = WhereClause(**{"category": "tech"})
        where = WhereClause(**{"$and": [inner.to_dict()]})
        q = QueryReqInput(query_embedding=[0.1], top_k=3, where=where)
        dumped = q.model_dump()
        assert "$and" in dumped["where"]
        assert "and_" not in dumped["where"]


# ─────────────────────────────────────────────────────────────────────────────
# ComparisonOp
# ─────────────────────────────────────────────────────────────────────────────

class TestComparisonOp:

    # ── happy path ────────────────────────────────────────────────────────────

    def test_gt_operator_passes(self):
        op = ComparisonOp(**{"$gt": 5})
        assert op.gt == 5

    def test_eq_string_operator_passes(self):
        op = ComparisonOp(**{"$eq": "tech"})
        assert op.eq == "tech"

    def test_multiple_operators_passes(self):
        op = ComparisonOp(**{"$gt": 1, "$lte": 10})
        assert op.gt == 1
        assert op.lte == 10

    def test_gte_operator_passes(self):
        op = ComparisonOp(**{"$gte": 100})
        assert op.gte == 100

    def test_lt_operator_passes(self):
        op = ComparisonOp(**{"$lt": 50})
        assert op.lt == 50

    def test_ne_string_operator_passes(self):
        op = ComparisonOp(**{"$ne": "spam"})
        assert op.ne == "spam"

    # ── no operators ─────────────────────────────────────────────────────────

    def test_no_operators_raises(self):
        with pytest.raises(ValidationError, match="at least one"):
            ComparisonOp()

    # ── falsy-zero edge cases ─────────────────────────────────────────────────
    # The current implementation uses `not any([self.gt, ...])` with truthiness
    # checks. This means 0, False, 0.0 are treated as "no operator set" and
    # incorrectly raise "at least one operator".
    # These tests document that bug — they will FAIL until the validator
    # is fixed to use `is not None` checks instead of truthiness.

    def test_eq_false_does_not_raise(self):
        # W.eq("active", False) — False is a valid equality value
        op = ComparisonOp(**{"$eq": False})
        assert op.eq is False

    def test_eq_zero_does_not_raise(self):
        # W.eq("count", 0)
        op = ComparisonOp(**{"$eq": 0})
        assert op.eq == 0

    def test_gt_zero_does_not_raise(self):
        # W.gt("score", 0) — greater than zero
        op = ComparisonOp(**{"$gt": 0})
        assert op.gt == 0

    def test_lte_zero_float_does_not_raise(self):
        op = ComparisonOp(**{"$lte": 0.0})
        assert op.lte == pytest.approx(0.0)

    def test_ne_false_does_not_raise(self):
        op = ComparisonOp(**{"$ne": False})
        assert op.ne is False

    def test_gte_zero_does_not_raise(self):
        op = ComparisonOp(**{"$gte": 0})
        assert op.gte == 0


# ─────────────────────────────────────────────────────────────────────────────
# WhereClause
# ─────────────────────────────────────────────────────────────────────────────

class TestWhereClause:

    # ── to_dict alias keys ────────────────────────────────────────────────────

    def test_and_clause_uses_dollar_and_key(self):
        inner = WhereClause(**{"category": "tech"})
        clause = WhereClause(**{"$and": [inner.to_dict()]})
        result = clause.to_dict()
        assert "$and" in result
        assert "and_" not in result

    def test_or_clause_uses_dollar_or_key(self):
        inner = WhereClause(**{"category": "tech"})
        clause = WhereClause(**{"$or": [inner.to_dict()]})
        result = clause.to_dict()
        assert "$or" in result
        assert "or_" not in result

    # ── None exclusion via exclude_none=True ──────────────────────────────────

    def test_to_dict_excludes_dollar_and_when_absent(self):
        clause = WhereClause(**{"category": "tech"})
        result = clause.to_dict()
        assert "$and" not in result
        assert "$or" not in result

    def test_to_dict_contains_no_none_values(self):
        clause = WhereClause(**{"year": 2024})
        result = clause.to_dict()
        for v in result.values():
            assert v is not None

    # ── extra field round-trip ────────────────────────────────────────────────

    def test_scalar_field_round_trips(self):
        clause = WhereClause(**{"category": "tech"})
        assert clause.to_dict()["category"] == "tech"

    def test_operator_dict_field_round_trips(self):
        clause = WhereClause(**{"year": {"$gt": 2022}})
        assert clause.to_dict()["year"] == {"$gt": 2022}

    def test_multiple_fields_round_trip(self):
        clause = WhereClause(**{"category": "tech", "year": {"$gte": 2020}})
        result = clause.to_dict()
        assert result["category"] == "tech"
        assert result["year"] == {"$gte": 2020}

    def test_boolean_false_field_survives_round_trip(self):
        # exclude_none drops None — it must NOT drop False
        clause = WhereClause(**{"active": False})
        result = clause.to_dict()
        assert "active" in result
        assert result["active"] is False

    def test_zero_int_field_survives_round_trip(self):
        # exclude_none must not drop 0
        clause = WhereClause(**{"count": 0})
        result = clause.to_dict()
        assert "count" in result
        assert result["count"] == 0

    # ── nesting ───────────────────────────────────────────────────────────────

    def test_and_with_two_clauses(self):
        clause = WhereClause(**{"$and": [
            WhereClause(**{"category": "tech"}).to_dict(),
            WhereClause(**{"year": {"$gte": 2020}}).to_dict(),
        ]})
        result = clause.to_dict()
        assert "$and" in result
        assert len(result["$and"]) == 2

    def test_or_with_two_clauses(self):
        clause = WhereClause(**{"$or": [
            WhereClause(**{"category": "tech"}).to_dict(),
            WhereClause(**{"category": "science"}).to_dict(),
        ]})
        result = clause.to_dict()
        assert "$or" in result
        assert len(result["$or"]) == 2

    def test_nested_and_inside_or(self):
        inner_and = WhereClause(**{"$and": [
            WhereClause(**{"category": "tech"}).to_dict(),
            WhereClause(**{"year": {"$gte": 2020}}).to_dict(),
        ]})
        outer_or = WhereClause(**{"$or": [
            inner_and.to_dict(),
            WhereClause(**{"category": "science"}).to_dict(),
        ]})
        result = outer_or.to_dict()
        assert "$or" in result
        assert "$and" in result["$or"][0]

    def test_deeply_nested_three_levels(self):
        leaf = WhereClause(**{"status": "active"})
        mid = WhereClause(**{"$and": [leaf.to_dict()]})
        top = WhereClause(**{"$or": [mid.to_dict()]})
        result = top.to_dict()
        assert "$or" in result
        assert "$and" in result["$or"][0]


# ─────────────────────────────────────────────────────────────────────────────
# Document
# ─────────────────────────────────────────────────────────────────────────────

class TestDocument:

    def test_distance_defaults_to_none(self):
        doc = Document(id="d1", content="hello")
        assert doc.distance is None

    def test_distance_can_be_set(self):
        doc = Document(id="d1", content="hello", distance=0.75)
        assert doc.distance == pytest.approx(0.75)

    def test_distance_can_be_zero(self):
        # exact match — 0.0 is valid and must not be dropped
        doc = Document(id="d1", content="hello", distance=0.0)
        assert doc.distance == pytest.approx(0.0)

    def test_metadata_defaults_to_empty_dict(self):
        doc = Document(id="d1", content="hello")
        assert doc.metadata == {}

    def test_metadata_default_factory_not_shared_across_instances(self):
        # guard against `metadata: dict = {}` shared mutable default bug
        doc1 = Document(id="d1", content="a")
        doc2 = Document(id="d2", content="b")
        doc1.metadata["key"] = "value"
        assert "key" not in doc2.metadata

    def test_metadata_with_values_preserved(self):
        doc = Document(id="d1", content="hello", metadata={"author": "jane", "year": 2024})
        assert doc.metadata["author"] == "jane"
        assert doc.metadata["year"] == 2024

    def test_id_and_content_required(self):
        with pytest.raises(ValidationError):
            Document(content="missing id")

    def test_model_dump_exclude_none_drops_distance_when_absent(self):
        # get_documents responses have no distance — verify it can be excluded
        doc = Document(id="d1", content="hello")
        dumped = doc.model_dump(exclude_none=True)
        assert "distance" not in dumped

    def test_model_dump_includes_distance_when_set(self):
        doc = Document(id="d1", content="hello", distance=0.5)
        dumped = doc.model_dump()
        assert dumped["distance"] == pytest.approx(0.5)

    def test_model_dump_includes_all_fields(self):
        doc = Document(id="d1", content="hello", metadata={"k": "v"}, distance=0.3)
        dumped = doc.model_dump()
        assert dumped["id"] == "d1"
        assert dumped["content"] == "hello"
        assert dumped["metadata"] == {"k": "v"}
        assert dumped["distance"] == pytest.approx(0.3)
