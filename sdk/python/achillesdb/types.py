from typing import Any, Callable, Awaitable, TypeAlias, TypedDict

EmbeddingFn: TypeAlias = Callable[
    [list[str]],
    list[list[float]] | Awaitable[list[list[float]]]
]

class QueryDocDict(TypedDict):
    id: str
    content: str
    metadata: dict[str, Any]
    distance: float

class GetDocDict(TypedDict):
    id: str
    content: str
    metadata: dict[str, Any]

class DeleteDocDict(TypedDict):
    deleted_count: int
    deleted_ids: list[str]
