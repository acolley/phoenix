import asyncio
import attr
from attr.validators import instance_of, optional
import inspect
import logging
from typing import Any, Callable, Coroutine, Dict, Generic, Optional, TypeVar, Union

from phoenix.actor.lifecycle import RestartActor, StopActor

logger = logging.getLogger(__name__)

# TODO: make it easier to inspect the graph of behaviours that constitute an actor


@attr.s
class Receive:

    f = attr.ib()

    @f.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)
        message = await context.ref.inbox.async_q.get()
        logger.debug("[%s] Message received: %s", context.ref, repr(message)[:200])
        try:
            next_ = await self.f(message)
        finally:
            context.ref.inbox.async_q.task_done()
        return next_


@attr.s
class Setup:

    f = attr.ib()

    @f.validator
    def check(self, attribute: str, value):
        if not inspect.iscoroutinefunction(value):
            raise ValueError(f"Not a coroutine: {value}")

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)

        return await self.f(context)


class Same:
    pass


class Ignore:
    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)
        await context.ref.inbox.async_q.get()
        context.ref.inbox.async_q.task_done()
        return Same()


class Stop:
    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)
        raise StopActor


@attr.s(frozen=True)
class Restart:
    """
    Restart the Actor if it fails.

    See Akka Behaviours.supervise for ideas.
    """

    behaviour = attr.ib(validator=instance_of((Setup, Receive, Same, Ignore)))
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

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)

        behaviours = [self.behaviour]
        while behaviours:
            logger.debug("[%s] Restart: %s", context.ref, behaviours)
            current = behaviours.pop()
            try:
                next_ = await current.execute(context)
            except StopActor:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                if self.restarts >= self.max_restarts:
                    raise

                logger.debug("Restart behaviour caught exception", exc_info=True)

                if self.backoff:
                    await asyncio.sleep(self.backoff(self.restarts))

                raise RestartActor(attr.evolve(self, restarts=self.restarts + 1))
            else:
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)


Behaviour = Union[Stop, Ignore, Setup, Receive, Same]
