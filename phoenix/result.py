"""
Generic operation result types.
"""
import attr
from typing import Generic, TypeVar


E = TypeVar("E")
T = TypeVar("T")


@attr.s(frozen=True)
class Success(Generic[T]):
    value: T = attr.ib()


@attr.s(frozen=True)
class Failure(Generic[E]):
    value: E = attr.ib()
