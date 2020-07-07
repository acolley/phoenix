"""
Implementation of standard behaviours.
"""

import asyncio
import attr
from attr.validators import instance_of, optional
import inspect
import logging
from pyrsistent import v
from typing import Any, Callable, Coroutine, Dict, Generic, Optional, TypeVar, Union

from phoenix.actor.lifecycle import PostStop, PreRestart, RestartActor, StopActor
from phoenix.actor.timers import FixedDelayEnvelope

logger = logging.getLogger(__name__)

# TODO: make it easier to inspect the graph of behaviours that constitute an actor


@attr.s
class Receive:

    f = attr.ib()
    on_lifecycle: Optional[Callable] = attr.ib(default=None)

    @f.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")

    def with_on_lifecycle(self, on_lifecycle: Callable):
        return attr.evolve(self, on_lifecycle=on_lifecycle)

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)
        message = await context.ref.inbox.async_q.get()
        event = None
        if isinstance(message, FixedDelayEnvelope):
            event = message.event
            message = message.msg
        logger.debug("[%s] Message received: %s", context.ref, repr(message)[:200])
        try:
            next_ = await self.f(message)
        finally:
            context.ref.inbox.async_q.task_done()
            if event:
                event.set()
        return next_


@attr.s
class Setup:

    f = attr.ib()
    on_lifecycle: Optional[Callable] = attr.ib(default=None)

    @f.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")

    def with_on_lifecycle(self, on_lifecycle: Callable):
        return attr.evolve(self, on_lifecycle=on_lifecycle)

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)

        return await self.f(context)


class Same:
    pass


@attr.s
class Ignore:
    on_lifecycle: Optional[Callable] = attr.ib(default=None)

    def with_on_lifecycle(self, on_lifecycle: Callable):
        return attr.evolve(self, on_lifecycle=on_lifecycle)

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)
        await context.ref.inbox.async_q.get()
        context.ref.inbox.async_q.task_done()
        return Same()


@attr.s
class Stop:
    on_lifecycle: Optional[Callable] = attr.ib(default=None)

    def with_on_lifecycle(self, on_lifecycle: Callable):
        return attr.evolve(self, on_lifecycle=on_lifecycle)

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)
        raise StopActor


@attr.s(frozen=True)
class Restart:
    """
    Restart the Actor if it fails.

    See Akka Behaviours.supervise for ideas.
    """

    behaviour = attr.ib()
    name: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    max_restarts: Optional[int] = attr.ib(
        validator=optional(instance_of(int)), default=3
    )
    restarts: int = attr.ib(validator=instance_of(int), default=0)
    backoff: Callable[[int], Union[float, int]] = attr.ib(default=None)
    on_lifecycle: Optional[Callable] = attr.ib(default=None)

    @max_restarts.validator
    def check(self, attribute: str, value: Optional[int]):
        if value is not None and value < 1:
            raise ValueError("max_restarts must be greater than zero.")

    @restarts.validator
    def check(self, attribute: str, value: int):
        if self.max_restarts is not None and self.restarts > self.max_restarts:
            raise ValueError("restarts cannot be greater than max_restarts")

    def with_on_lifecycle(self, on_lifecycle: Callable):
        return attr.evolve(self, on_lifecycle=on_lifecycle)

    def with_max_restarts(self, max_restarts: Optional[int]) -> "Restarts":
        return attr.evolve(self, max_restarts=max_restarts)

    def with_backoff(self, f: Callable[[int], Union[float, int]]) -> "Restarts":
        """
        Use this to wait for a set period of time before restarting.

        ``f`` is a function that should receive the restart count and
        return a time to wait in seconds.
        """
        return attr.evolve(self, backoff=f)

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)

        # FIXME Given this behaviour and phoenix.actor.Actor both
        # deal with lifecycle callbacks it's possible that lifecycles
        # will be handled twice.

        behaviours = [self.behaviour]
        while behaviours:
            logger.debug("[%s] Restart: %s", context.ref, behaviours)
            current = behaviours.pop()
            try:
                next_ = await current.execute(context)
            except StopActor:
                if current.on_lifecycle:
                    await current.on_lifecycle(PostStop())
                raise
            except asyncio.CancelledError:
                if current.on_lifecycle:
                    await current.on_lifecycle(PostStop())
                raise
            except Exception:
                if self.max_restarts is not None and self.restarts >= self.max_restarts:
                    if current.on_lifecycle:
                        await current.on_lifecycle(PostStop())
                    raise

                logger.debug("Restart behaviour caught exception", exc_info=True)

                if self.backoff:
                    await asyncio.sleep(self.backoff(self.restarts))

                if current.on_lifecycle:
                    behaviour = await current.on_lifecycle(PreRestart())
                    if isinstance(behaviour, Same):
                        behaviour = attr.evolve(self, restarts=self.restarts + 1)
                else:
                    behaviour = attr.evolve(self, restarts=self.restarts + 1)

                raise RestartActor(behaviour=behaviour)
            else:
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)


@attr.s
class Unstash:
    messages = attr.ib()
    behaviour: Receive = attr.ib(validator=instance_of(Receive))

    async def execute(self, context) -> "Behaviour":
        behaviour = self.behaviour
        for msg in self.messages:
            next_ = await behaviour.f(msg)
            if not isinstance(next_, Same):
                behaviour = next_
        return behaviour


@attr.s
class Buffer:
    capacity: int = attr.ib(validator=instance_of(int))
    messages = attr.ib(default=v())

    def stash(self, msg) -> "Behaviour":
        if len(self.messages) == self.capacity:
            raise ValueError("Buffer capacity overflow.")
        self.messages = self.messages.append(msg)
    
    def unstash(self, behaviour: Receive) -> "Behaviour":
        return Unstash(messages=self.messages, behaviour=behaviour)


@attr.s
class Stash:
    f = attr.ib()
    capacity: int = attr.ib(validator=instance_of(int))

    async def execute(self, context) -> "Behaviour":
        buffer = Buffer(self.capacity)
        return await self.f(buffer)


Behaviour = Union[Stop, Ignore, Setup, Receive, Same, Stash, Unstash]
