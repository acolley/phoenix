import attr
from attr.validators import instance_of, optional
import inspect
from typing import Any, Callable, Dict, Optional, Union

# TODO: make it easier to inspect the graph of behaviours that constitute an actor


@attr.s
class Receive:

    f = attr.ib()

    @f.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")

    async def __call__(self, message):
        return await self.f(message)


def receive(f) -> Receive:
    return Receive(f=f)


def receive_decorator(f):
    def _receive(self) -> Receive:
        return Receive(f=f)

    return _receive


@attr.s
class Setup:

    f = attr.ib()

    @f.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")

    async def __call__(self, context):
        return await self.f(context)


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
    name: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
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


@attr.s(frozen=True)
class Persist:
    id: str = attr.ib(validator=instance_of(str))
    empty_state = attr.ib()
    command_handler = attr.ib()
    event_handler = attr.ib()
    encode: Callable[[Any], dict] = attr.ib()
    decode: Callable[[dict], Any] = attr.ib()


def persist(
    id: str, empty_state, command_handler, event_handler, encode, decode
) -> Persist:
    return Persist(
        id=id,
        empty_state=empty_state,
        command_handler=command_handler,
        event_handler=event_handler,
        encode=encode,
        decode=decode,
    )


Behaviour = Union[Schedule, Stop, Ignore, Setup, Persist, Receive, Same]
