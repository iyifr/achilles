from __future__ import annotations

from achillesdb.schemas import Scalar, WhereClause


class W:
    """
    Fluent builder for where clauses.

    Usage:
        W.eq("category", "tech")
        W.gt("year", 2022)
        W.in_("author", ["jane", "john"])
        W.arr_contains("allowed_acls", ["acl-readers"])
        W.and_(W.eq("category", "tech"), W.gt("year", 2022))
        W.or_(W.eq("category", "food"), W.lt("year", 2024))
    """

    @staticmethod
    def eq(field: str, value: Scalar) -> WhereClause:
        return WhereClause(**{field: {"$eq": value}})

    @staticmethod
    def gt(field: str, value: int | float) -> WhereClause:
        return WhereClause(**{field: {"$gt": value}})

    @staticmethod
    def gte(field: str, value: int | float) -> WhereClause:
        return WhereClause(**{field: {"$gte": value}})

    @staticmethod
    def lt(field: str, value: int | float) -> WhereClause:
        return WhereClause(**{field: {"$lt": value}})

    @staticmethod
    def lte(field: str, value: int | float) -> WhereClause:
        return WhereClause(**{field: {"$lte": value}})

    @staticmethod
    def ne(field: str, value: Scalar) -> WhereClause:
        return WhereClause(**{field: {"$ne": value}})

    @staticmethod
    def in_(field: str, values: list[Scalar]) -> WhereClause:
        return WhereClause(**{field: {"$in": values}})

    @staticmethod
    def arr_contains(field: str, values: list[Scalar]) -> WhereClause:
        return WhereClause(**{field: {"$arrContains": values}})

    @staticmethod
    def and_(*clauses: WhereClause) -> WhereClause:
        return WhereClause(**{"$and": [c.to_dict() for c in clauses]})

    @staticmethod
    def or_(*clauses: WhereClause) -> WhereClause:
        return WhereClause(**{"$or": [c.to_dict() for c in clauses]})
