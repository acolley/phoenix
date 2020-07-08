"""
Implementation of standard behaviours.
"""

import asyncio
import attr
from attr.validators import instance_of, optional
import inspect
import logging
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

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
    max_restarts: Optional[int] = attr.ib(validator=optional(instance_of(int)))
    backoff: Callable[[int], Union[float, int]] = attr.ib()
    restarts: int = attr.ib(validator=instance_of(int), default=0)

    @max_restarts.validator
    def check(self, attribute: str, value: Optional[int]):
        if value is not None and value < 1:
            raise ValueError("max_restarts must be greater than zero.")

    @restarts.validator
    def check(self, attribute: str, value: int):
        if self.max_restarts is not None and self.restarts > self.max_restarts:
            raise ValueError("restarts cannot be greater than max_restarts")

    async def __call__(
        self, exc: Exception, supervise: "Supervise", current: "Behaviour"
    ):
        """
        Called from the exception handler in the Supervise behaviour.

        Args:
            exc (Exception): The exception that was caught.
            supervise (Supervise): The Supervise behaviour executing this strategy.
            current (Behaviour): The current behaviour being executed.
        """
        if self.max_restarts is not None and self.restarts >= self.max_restarts:
            if current.on_lifecycle:
                await current.on_lifecycle(PostStop())
            raise

        if self.backoff:
            await asyncio.sleep(self.backoff(self.restarts))

        if current.on_lifecycle:
            behaviour = await current.on_lifecycle(PreRestart())
            if isinstance(behaviour, Same):
                behaviour = attr.evolve(
                    supervise,
                    failure=attr.evolve(
                        supervise.failure,
                        strategy=attr.evolve(self, restarts=self.restarts + 1),
                    ),
                )
        else:
            behaviour = attr.evolve(
                supervise,
                failure=attr.evolve(
                    supervise.failure,
                    strategy=attr.evolve(self, restarts=self.restarts + 1),
                ),
            )

        raise RestartActor(behaviour=behaviour)


@attr.s(frozen=True)
class RestartStrategy:
    """
    The restart supervision strategy.

    Indicates that actors should be restarted after encountering failure.
    """

    max_restarts: Optional[int] = attr.ib(
        validator=optional(instance_of(int)), default=3
    )
    backoff: Callable[[int], Union[float, int]] = attr.ib(default=None)
    """
    A function that should receive the restart count and
    return a time to wait in seconds.

    Use this to wait for a set period of time before restarting.
    """

    def __call__(self) -> Restart:
        return Restart(max_restarts=self.max_restarts, backoff=self.backoff)


@attr.s(frozen=True)
class OnFailure:
    when: Callable[[Exception], bool] = attr.ib()
    """
    A callable that determines whether a given exception
    should be considered by the supervision strategy.

    Exceptions not considered by this callable are raised
    through the normal exception behaviour.

    Examples:
        >>> when = lambda e: isinstance(e, Exception)
        >>> when(Exception)
        True
    """
    strategy = attr.ib()


@attr.s(frozen=True)
class Supervise:
    """
    Supervise the actor and perform actions upon failure.

    See Akka Behaviours.supervise for ideas.
    """

    behaviour = attr.ib()
    failure: Optional[OnFailure] = attr.ib(
        validator=optional(instance_of(OnFailure)), default=None
    )
    on_lifecycle: Optional[Callable] = attr.ib(default=None)

    def on_failure(self, when: Callable[[Exception], bool], strategy) -> "Supervise":
        return attr.evolve(self, failure=OnFailure(when=when, strategy=strategy()))

    def with_on_lifecycle(self, on_lifecycle: Callable):
        return attr.evolve(self, on_lifecycle=on_lifecycle)

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
            except Exception as e:
                logger.warning(
                    "[%s] Supervise caught exception", context.ref, exc_info=True
                )

                if not self.failure:
                    logger.warning("No Supervise on_failure handler configured.")
                    raise

                if self.failure.when(e):
                    await self.failure.strategy(e, self, current)
                else:
                    raise
            else:
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)


Behaviour = Union[Stop, Ignore, Setup, Receive, Same, Supervise]
