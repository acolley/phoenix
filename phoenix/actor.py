import abc
import asyncio
import attr
from attr.validators import instance_of, optional
import contextlib
from datetime import timedelta
import janus
import logging
from multipledispatch import dispatch
import threading
from typing import Any, Callable, List, Optional, Union
import uuid

from phoenix.behaviour import (
    Behaviour,
    Ignore,
    Receive,
    Restart,
    Same,
    Schedule,
    Setup,
    Stop,
)
from phoenix.ref import Ref
from phoenix.system.messages import SpawnActor

logger = logging.getLogger(__name__)


@attr.s
class ActorContext:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    """
    The Ref of this Actor.
    """

    parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
    """
    The parent of this Actor.
    """

    thread: threading.Thread = attr.ib(validator=instance_of(threading.Thread))
    """
    The thread that is executing this Actor.
    """

    loop: asyncio.AbstractEventLoop = attr.ib(
        validator=instance_of(asyncio.AbstractEventLoop)
    )
    """
    The event loop that is executing this Actor.
    """

    system: Ref = attr.ib(validator=instance_of(Ref))
    """
    The system that this Actor belongs to.
    """

    # FIXME: come up with a different way to allow the
    # parent actor kill its children on exit.
    # This should ideally only contain a list of Refs
    # for an actor implementer to communicate with their
    # children.
    children: List["ActorCell"] = attr.ib(default=[])

    async def spawn(
        self,
        behaviour: Behaviour,
        id: Optional[str] = None,
        dispatcher: Optional[str] = None,
    ) -> Ref:
        id = id or str(uuid.uuid1())
        response = await self.system.ask(
            lambda reply_to:
            SpawnActor(
                reply_to=reply_to, id=id, behaviour=behaviour, dispatcher=dispatcher, parent=self.ref
            )
        )
        self.children.append(response.cell)
        return response.cell.context.ref


@attr.s
class ActorFailed:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    exc: Exception = attr.ib(validator=instance_of(Exception))


@attr.s
class ActorStopped:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorKilled:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorSpawned:
    parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
    behaviour: Behaviour = attr.ib()
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorRestarted:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    behaviour: Restart = attr.ib(validator=instance_of(Restart))


class RestartActor(Exception):
    def __init__(self, behaviour: Restart):
        self.behaviour = behaviour


class StopActor(Exception):
    pass


@attr.s
class Timers:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    lock: asyncio.Lock = attr.ib(validator=instance_of(asyncio.Lock))
    _timers = attr.ib(init=False, default={})

    async def start_fixed_delay_timer(self, message: Any, delay: timedelta, name: Optional[str] = None):
        name = name or str(uuid.uuid1())

        async def _fixed_delay_timer():
            while True:
                await asyncio.sleep(delay.total_seconds())
                await self.ref.tell(message)
        
        async with self.lock:
            if name in self._timers:
                raise ValueError(f"Timer `{name}` already exists.")

            self._timers[name] = asyncio.create_task(_fixed_delay_timer())
    
    async def start_single_shot_timer(self, message: Any, delay: timedelta, name: Optional[str] = None):
        name = name or str(uuid.uuid1())

        async def _single_shot_timer():
            asyncio.sleep(delay.total_seconds())
            await self.ref.tell(message)
            await self.cancel(name)
        
        async with self.lock:
            if name in self._timers:
                raise ValueError(f"Timer `{name}` already exists.")

            self._timers[name] = asyncio.create_task(_single_shot_timer())

    async def cancel(self, name: str):
        async with self.lock:
            timer = self._timers.pop(name)
            timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await timer

    async def cancel_all(self):
        async with self.lock:
            timers = self._timers.values()
            for timer in timers:
                timer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await timer


@attr.s
class Actor:
    """
    Responsible for executing the behaviours returned
    by the actor's starting behaviour.
    """

    start: Behaviour = attr.ib()
    context: ActorContext = attr.ib(validator=instance_of(ActorContext))
    timers: Timers = attr.ib(validator=instance_of(Timers))

    @start.validator
    def check(self, attribute: str, value: Behaviour):
        if not isinstance(value, (Ignore, Receive, Restart, Schedule, Setup)):
            raise ValueError(f"Invalid start behaviour: {value}")

    async def run(self):
        """
        Execute the actor in a coroutine.

        Returns terminating behaviours for the actor
        so that the supervisor for this actor knows
        how to react to a termination.
        """

        # A stack of actor behaviours.
        # The top of the stack determines
        # what the behaviour will be on
        # the next loop cycle.
        try:
            behaviours = [self.start]
            while behaviours:
                logger.debug("[%s] Main: %s", self.context.ref, behaviours)
                current = behaviours.pop()
                next_ = await self.execute(current)
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)
        except RestartActor as e:
            return ActorRestarted(ref=self.context.ref, behaviour=e.behaviour)
        except StopActor:
            return ActorStopped(ref=self.context.ref)
        except asyncio.CancelledError:
            # We were deliberately cancelled.
            # TODO: Allow the actor to perform cleanup.
            return ActorKilled(ref=self.context.ref)
        except Exception as e:
            logger.debug(
                "[%s] Behaviour raised unhandled exception",
                self.context.ref,
                exc_info=True,
            )
            return ActorFailed(ref=self.context.ref, exc=e)

    @dispatch(Setup)
    async def execute(self, behaviour: Setup):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)

        next_ = await behaviour(self.context)
        return next_

    @dispatch(Receive)
    async def execute(self, behaviour: Receive):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        message = await self.context.ref.inbox.async_q.get()
        logger.debug("[%s] Message received: %s", self.context.ref, message)
        try:
            next_ = await behaviour(message)
        finally:
            self.context.ref.inbox.async_q.task_done()
        return next_

    @dispatch(Ignore)
    async def execute(self, behaviour: Ignore):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        await self.context.ref.inbox.async_q.get()
        self.context.ref.inbox.async_q.task_done()
        return Same()

    @dispatch(Restart)
    async def execute(self, behaviour: Restart):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)

        behaviours = [behaviour.behaviour]
        while behaviours:
            logger.debug("[%s] Restart: %s", self.context.ref, behaviours)
            current = behaviours.pop()
            try:
                next_ = await self.execute(current)
            except StopActor:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                if behaviour.restarts >= behaviour.max_restarts:
                    raise

                logger.debug("Restart behaviour caught exception", exc_info=True)

                if behaviour.backoff:
                    await asyncio.sleep(behaviour.backoff(behaviour.restarts))

                raise RestartActor(
                    attr.evolve(behaviour, restarts=behaviour.restarts + 1)
                )
            else:
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)

    @dispatch(Schedule)
    async def execute(self, behaviour: Schedule):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        return await behaviour(self.timers)

    @dispatch(Stop)
    async def execute(self, behaviour: Stop):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        raise StopActor


@attr.s
class ActorCell:
    """
    Manage Actor lifecycle.
    """

    behaviour: Behaviour = attr.ib()
    context: ActorContext = attr.ib(validator=instance_of(ActorContext))
    _actor: Optional[Actor] = attr.ib(
        init=False, validator=optional(instance_of(Actor)), default=None
    )
    _task: Optional[asyncio.Task] = attr.ib(
        init=False, validator=optional(instance_of(asyncio.Task)), default=None
    )
    _timers: Optional[Timers] = attr.ib(init=False, validator=optional(instance_of(Timers)), default=None)

    async def run(self):
        self._timers = Timers(ref=self.context.ref, lock=asyncio.Lock())
        self._actor = Actor(start=self.behaviour, context=self.context, timers=self._timers)

        try:
            self._task = asyncio.create_task(self._actor.run())

            while True:
                result = await self._task
                await self.handle(result)
        except asyncio.CancelledError:
            self._task.cancel()
            await self._timers.cancel_all()

    async def stop(self):
        # This is called from ActorCell.handle(ActorFailed)
        # in order to stop child actors.
        # NOTE: is this thread-safe???
        self._task.cancel()

    @dispatch(ActorRestarted)
    async def handle(self, msg: ActorRestarted):
        await self._timers.cancel_all()
        self._timers = Timers(ref=self.context.ref, lock=asyncio.Lock())
        self._actor = Actor(start=msg.behaviour, context=self.context, timers=self._timers)
        self._task = asyncio.create_task(self._actor.run())
    
    @dispatch(ActorKilled)
    async def handle(self, msg: ActorKilled):
        # TODO: Notify parent
        # NOTE: is this thread-safe???
        for child in self.context.children:
            await child.stop()

    @dispatch(ActorFailed)
    async def handle(self, msg: ActorFailed):
        # TODO: Notify parent
        # NOTE: is this thread-safe???
        for child in self.context.children:
            await child.stop()
