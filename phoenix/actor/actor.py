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

from phoenix.actor.context import ActorContext
from phoenix.actor.timers import Timers
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
from phoenix.system import messages

logger = logging.getLogger(__name__)


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
            logger.warning(
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
