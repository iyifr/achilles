from typing import Callable, Awaitable, TypeAlias

EmbeddingFn: TypeAlias = Callable[
    [list[str]],
    list[list[float]] | Awaitable[list[list[float]]]
]
