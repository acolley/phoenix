import attr
from attr.validators import instance_of, optional
import inspect
from pyrsistent import PRecord, field
from typing import Callable, Optional, Union

# TODO: make it easier to inspect the graph of behaviours that constitute an actor


class Receive(PRecord):

    f = field(
        invariant=lambda x: (inspect.iscoroutinefunction(x), f"Not a coroutine: {x}")
    )

    async def __call__(self, message):
        return await self.f(message)


def receive(f) -> Receive:
    return Receive(f=f)


def receive_decorator(f):
    def _receive(self) -> Receive:
        return Receive(f=f)

    return _receive


class Setup(PRecord):

    f = field(
        invariant=lambda x: (inspect.iscoroutinefunction(x), f"Not a coroutine: {x}")
    )

    async def __call__(self, spawn):
        return await self.f(spawn)


def setup(f) -> Setup:
    return Setup(f=f)


def setup_decorator(f):
    def _setup(self) -> Setup:
        return Setup(f=f)

    return _setup


@attr.s(frozen=True)
class Schedule:
    f = attr.ib()

    @f.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")

    async def __call__(self, timers):
        return await self.f(timers)


def schedule(f) -> Schedule:
    return Schedule(f)


class Same:
    pass


def same() -> Same:
    return Same()


class Ignore:
    pass


def ignore() -> Ignore:
    return Ignore()


class Stop:
    pass


def stop() -> Stop:
    return Stop()


@attr.s(frozen=True)
class Restart:
    """
    Restart the Actor if it fails.

    See Akka Behaviours.supervise for ideas.
    """

    behaviour = attr.ib(validator=instance_of((Schedule, Setup, Receive, Same, Ignore)))
    max_restarts: Optional[int] = attr.ib(
        validator=optional(instance_of(int)), default=3
    )
    restarts: int = attr.ib(validator=instance_of(int), default=0)
    backoff: Callable[[int], Union[float, int]] = attr.ib(default=None)

    @max_restarts.validator
    def check(self, attribute: str, value: Optional[int]):
        if value is not None and value < 1:
            raise ValueError("max_restarts must be greater than zero.")

    @restarts.validator
    def check(self, attribute: str, value: int):
        if self.max_restarts is not None and self.restarts > self.max_restarts:
            raise ValueError("restarts cannot be greater than max_restarts")

    def with_max_restarts(self, max_restarts: Optional[int]) -> "Restarts":
        return attr.evolve(self, max_restarts=max_restarts)

    def with_backoff(self, f: Callable[[int], Union[float, int]]) -> "Restarts":
        """
        Use this to wait for a set period of time before restarting.

        ``f`` is a function that should receive the restart count and
        return a time to wait in seconds.
        """
        return attr.evolve(self, backoff=f)


def restart(behaviour) -> Restart:
    return Restart(behaviour=behaviour)


class Enqueue:
    pass


def enqueue() -> Enqueue:
    return Enqueue()


Behaviour = Union[Schedule, Stop, Ignore, Setup, Receive, Same]
