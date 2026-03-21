import pytest
from achillesdb.util import get_collections_name


class TestGetCollectionsName:

    def test_standard_namespace(self):
        # typical format: "database.collection"
        assert get_collections_name("mydb.mycollection") == "mycollection"

    def test_nested_namespace(self):
        # deeper dot-separated path — should return only the last segment
        assert get_collections_name("a.b.c") == "c"

    def test_no_dot_returns_full_string(self):
        # no dot — the whole string is the name
        assert get_collections_name("mycollection") == "mycollection"

    def test_empty_string(self):
        # edge case: empty string
        assert get_collections_name("") == ""

    def test_trailing_dot(self):
        # trailing dot — last segment is empty string
        assert get_collections_name("mydb.") == ""

    def test_leading_dot(self):
        # leading dot — last segment is the collection name
        assert get_collections_name(".mycollection") == "mycollection"

    def test_multiple_dots(self):
        assert get_collections_name("a.b.c.d") == "d"

    def test_preserves_case(self):
        assert get_collections_name("MyDB.MyCollection") == "MyCollection"

    def test_underscores_preserved(self):
        assert get_collections_name("my_db.my_collection_v2") == "my_collection_v2"
