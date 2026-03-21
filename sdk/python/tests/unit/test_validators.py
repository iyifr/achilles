"""
tests/unit/test_validators.py
==============================
Unit tests for achillesdb/validators.py

Covers:
  - validate_name
  - validate_equal_lengths
"""

import pytest
from achillesdb.validators import validate_name, validate_equal_lengths


# ─────────────────────────────────────────────────────────────────────────────
# validate_name
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateName:

    # ── happy path ────────────────────────────────────────────────────────────

    def test_simple_name_passes(self):
        # plain lowercase word — most common case
        validate_name("mydb", "Database name")

    def test_name_with_underscores_passes(self):
        validate_name("my_collection_v2", "Collection name")

    def test_name_starting_with_underscore_passes(self):
        # leading underscore is a valid Python identifier
        validate_name("_internal", "Collection name")

    def test_name_with_uppercase_passes(self):
        validate_name("MyDatabase", "Database name")

    def test_single_character_name_passes(self):
        validate_name("a", "Collection name")

    # ── empty / blank ─────────────────────────────────────────────────────────

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="empty"):
            validate_name("", "Database name")

    # ── invalid identifier characters ─────────────────────────────────────────

    def test_name_with_spaces_raises(self):
        with pytest.raises(ValueError, match="Database name"):
            validate_name("my db", "Database name")

    def test_name_with_hyphen_raises(self):
        # hyphens are common in slugs but not valid Python identifiers
        with pytest.raises(ValueError, match="Collection name"):
            validate_name("my-collection", "Collection name")

    def test_name_with_dot_raises(self):
        with pytest.raises(ValueError, match="Collection name"):
            validate_name("db.collection", "Collection name")

    def test_name_starting_with_digit_raises(self):
        # "123abc" is not a valid identifier — starts with a digit
        with pytest.raises(ValueError):
            validate_name("123abc", "Database name")

    def test_name_all_digits_raises(self):
        with pytest.raises(ValueError):
            validate_name("12345", "Database name")

    def test_name_with_slash_raises(self):
        with pytest.raises(ValueError):
            validate_name("db/collection", "Collection name")

    def test_name_with_at_sign_raises(self):
        with pytest.raises(ValueError):
            validate_name("my@db", "Database name")

    # ── error message content ─────────────────────────────────────────────────

    def test_error_message_includes_name_type(self):
        # the caller-supplied label should appear in the error message
        with pytest.raises(ValueError, match="Collection name"):
            validate_name("bad name", "Collection name")

    def test_error_message_includes_invalid_value(self):
        with pytest.raises(ValueError, match="bad-name"):
            validate_name("bad-name", "Database name")


# ─────────────────────────────────────────────────────────────────────────────
# validate_equal_lengths
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateEqualLengths:

    # ── happy path ────────────────────────────────────────────────────────────

    def test_all_equal_lengths_passes(self):
        validate_equal_lengths(
            ids=["a", "b", "c"],
            documents=["x", "y", "z"],
            embeddings=[[0.1], [0.2], [0.3]],
        )

    def test_single_field_passes(self):
        validate_equal_lengths(ids=["a", "b"])

    def test_all_none_fields_passes(self):
        # None fields are excluded from the comparison — should be a no-op
        validate_equal_lengths(ids=None, documents=None, embeddings=None)

    def test_mix_of_none_and_present_passes_when_present_are_equal(self):
        # metadatas=None should not be compared; ids and documents are equal
        validate_equal_lengths(
            ids=["a", "b"],
            documents=["x", "y"],
            metadatas=None,
        )

    def test_empty_lists_are_equal(self):
        validate_equal_lengths(ids=[], documents=[], embeddings=[])

    def test_single_item_lists_are_equal(self):
        validate_equal_lengths(ids=["a"], documents=["x"], embeddings=[[0.1]])

    # ── mismatch cases ────────────────────────────────────────────────────────

    def test_one_shorter_list_raises(self):
        with pytest.raises(ValueError):
            validate_equal_lengths(
                ids=["a", "b", "c"],
                documents=["x", "y"],       # one short
                embeddings=[[0.1], [0.2], [0.3]],
            )

    def test_one_longer_list_raises(self):
        with pytest.raises(ValueError):
            validate_equal_lengths(
                ids=["a", "b"],
                documents=["x", "y", "z"],  # one long
            )

    def test_all_different_lengths_raises(self):
        with pytest.raises(ValueError):
            validate_equal_lengths(
                ids=["a"],
                documents=["x", "y"],
                embeddings=[[0.1], [0.2], [0.3]],
            )

    # ── error message content ─────────────────────────────────────────────────

    def test_error_message_includes_all_field_names(self):
        # the breakdown should name every field so the user knows what mismatched
        with pytest.raises(ValueError, match="ids"):
            validate_equal_lengths(
                ids=["a", "b", "c"],
                documents=["x", "y"],
            )

    def test_error_message_includes_lengths(self):
        with pytest.raises(ValueError, match="3"):
            validate_equal_lengths(
                ids=["a", "b", "c"],
                documents=["x", "y"],
            )

    # ── none mixed with mismatched present fields ─────────────────────────────

    def test_none_field_not_included_in_mismatch_check(self):
        # ids=3, documents=3, metadatas=None → should pass because None is excluded
        validate_equal_lengths(
            ids=["a", "b", "c"],
            documents=["x", "y", "z"],
            metadatas=None,
        )

    def test_none_field_does_not_mask_real_mismatch(self):
        # ids=3, documents=2 is still a mismatch even if metadatas=None
        with pytest.raises(ValueError):
            validate_equal_lengths(
                ids=["a", "b", "c"],
                documents=["x", "y"],
                metadatas=None,
            )
