import pytest
from achillesdb.where import W
from achillesdb.schemas import WhereClause


class TestWBuilderOperators:

    def test_eq_produces_correct_clause(self):
        clause = W.eq("category", "fruit")
        assert clause.to_dict() == {
            "category": {"$eq": "fruit"}
        }

    def test_gt_produces_correct_clause(self):
        clause = W.gt("year", 2020)
        assert clause.to_dict() == {
            "year": {"$gt": 2020}
        }

    def test_gte_produces_correct_clause(self):
        clause = W.gte("year", 2021)
        assert clause.to_dict() == {
            "year": {"$gte": 2021}
        }

    def test_lt_produces_correct_clause(self):
        clause = W.lt("year", 2022)
        assert clause.to_dict() == {
            "year": {"$lt": 2022}
        }

    def test_lte_produces_correct_clause(self):
        clause = W.lte("year", 2021)
        assert clause.to_dict() == {
            "year": {"$lte": 2021}
        }

    def test_ne_produces_correct_clause(self):
        clause = W.ne("popular", False)
        assert clause.to_dict() == {
            "popular": {"$ne": False}
        }

    def test_in_produces_correct_clause(self):
        clause = W.in_("year", [2021, 2022, 2024])
        assert clause.to_dict() == {
            "year": {"$in": [2021, 2022, 2024]}
        }

    def test_arr_contains_produces_correct_clause(self):
        clause = W.arr_contains("tags", ["new", "sale"])
        assert clause.to_dict() == {
            "tags": {"$arrContains": ["new", "sale"]}
        }

    def test_eq_with_int_value(self):
        clause = W.eq("year", 2021)
        d = clause.to_dict()
        assert d["year"]["$eq"] == 2021

    def test_eq_with_bool_value(self):
        clause = W.eq("popular", True)
        d = clause.to_dict()
        assert d["popular"]["$eq"] is True

    def test_all_operators_return_where_clause(self):
        builders = [
            W.eq("f", "v"),
            W.gt("f", 1),
            W.gte("f", 1),
            W.lt("f", 1),
            W.lte("f", 1),
            W.ne("f", "v"),
            W.in_("f", ["v"]),
            W.arr_contains("f", ["v"]),
        ]
        for clause in builders:
            assert isinstance(clause, WhereClause)


class TestWBuilderCompound:

    def test_and_produces_correct_structure(self):
        clause = W.and_(W.eq("category", "fruit"), W.gt("year", 2020))
        d = clause.to_dict()
        assert "$and" in d
        assert len(d["$and"]) == 2
        assert {"category": {"$eq": "fruit"}} in d["$and"]
        assert {"year": {"$gt": 2020}} in d["$and"]

    def test_or_produces_correct_structure(self):
        clause = W.or_(W.eq("popular", True), W.lt("year", 2019))
        d = clause.to_dict()
        assert "$or" in d
        assert len(d["$or"]) == 2

    def test_and_with_three_clauses(self):
        clause = W.and_(W.eq("a", 1), W.gt("b", 2), W.lt("c", 3))
        d = clause.to_dict()
        assert len(d["$and"]) == 3

    def test_nested_and_or(self):
        clause = W.and_(
            W.eq("category", "fruit"),
            W.or_(W.gt("year", 2020), W.eq("popular", True))
        )
        d = clause.to_dict()
        assert "$and" in d
        inner = d["$and"]
        assert any("$or" in item for item in inner)

    def test_and_returns_where_clause(self):
        clause = W.and_(W.eq("a", 1), W.eq("b", 2))
        assert isinstance(clause, WhereClause)

    def test_or_returns_where_clause(self):
        clause = W.or_(W.eq("a", 1), W.eq("b", 2))
        assert isinstance(clause, WhereClause)


class TestWhereClauseToDict:

    def test_to_dict_uses_aliases(self):
        clause = W.eq("category", "tech")
        d = clause.to_dict()
        # should use $eq not eq
        assert d == {"category": {"$eq": "tech"}}

    def test_to_dict_excludes_none_fields(self):
        clause = W.gt("year", 2020)
        d = clause.to_dict()
        # should not include $and, $or or other None fields
        assert "$and" not in d
        assert "$or" not in d

    def test_to_dict_and_clause(self):
        clause = W.and_(W.eq("a", 1), W.eq("b", 2))
        d = clause.to_dict()
        assert "$and" in d
        assert "$or" not in d

    def test_to_dict_or_clause(self):
        clause = W.or_(W.eq("a", 1), W.eq("b", 2))
        d = clause.to_dict()
        assert "$or" in d
        assert "$and" not in d

    def test_to_dict_returns_plain_dict(self):
        clause = W.eq("category", "fruit")
        d = clause.to_dict()
        assert isinstance(d, dict)
