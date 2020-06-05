import attr
from attr.validators import instance_of, optional
import inspect
from pyrsistent import PRecord, field
from typing import Callable, Optional, Union

# TODO: make it easier to inspect the graph of behaviours that constitute an actor

class Receive(PRecord):

    behaviour = field(invariant=lambda x: (inspect.iscoroutinefunction(x), f"Not a coroutine: {x}"))

    async def __call__(self, message):
        return await self.behaviour(message)


def receive(behaviour) -> Receive:
    return Receive(behaviour=behaviour)


def receive_decorator(behaviour):
    def _receive(self) -> Receive:
        return Receive(behaviour=behaviour)
    return _receive


class Setup(PRecord):

    behaviour = field(invariant=lambda x: (inspect.iscoroutinefunction(x), f"Not a coroutine: {x}"))

    async def __call__(self, spawn):
        return await self.behaviour(spawn)


def setup(behaviour) -> Setup:
    return Setup(behaviour=behaviour)


def setup_decorator(behaviour):
    def _setup(self) -> Setup:
        return Setup(behaviour=behaviour)
    return _setup


@attr.s(frozen=True)
class Schedule:
    behaviour = attr.ib()

    @behaviour.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")
    
    async def __call__(self, scheduler):
        return await self.behaviour(scheduler)


def schedule(behaviour) -> Schedule:
    return Schedule(behaviour)


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

    behaviour = attr.ib(validator=instance_of((Setup, Receive, Same, Ignore)))
    max_restarts: Optional[int] = attr.ib(validator=optional(instance_of(int)), default=3)
    backoff: Callable[[int], Union[float, int]] = attr.ib(default=None)

    @max_restarts.validator
    def check(self, attribute: str, value: Optional[int]):
        if value is not None and value < 1:
            raise ValueError("max_restarts must be greater than zero.")

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


Behaviour = Union[Schedule, Stop, Ignore, Setup, Receive, Same]
