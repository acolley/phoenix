import asyncio
import attr
from attr.validators import instance_of, optional
import contextlib
import logging
from multipledispatch import dispatch
from typing import Optional

from phoenix.actor import actor
from phoenix.actor.timers import Timers
from phoenix.behaviour import Behaviour
from phoenix.ref import Ref
from phoenix.system.messages import StopActor

logger = logging.getLogger(__name__)


@attr.s
class ActorStopped:
    ref: Ref = attr.ib(validator=instance_of(Ref))


class Stop(Exception):
    pass


@attr.s
class ActorCell:
    """
    Manage Actor lifecycle.
    """

    behaviour: Behaviour = attr.ib()
    context: actor.ActorContext = attr.ib(validator=instance_of(actor.ActorContext))
    _actor: Optional[actor.Actor] = attr.ib(
        init=False, validator=optional(instance_of(actor.Actor)), default=None
    )
    _task: Optional[asyncio.Task] = attr.ib(
        init=False, validator=optional(instance_of(asyncio.Task)), default=None
    )

    async def run(self):
        try:
            self._actor = actor.Actor(start=self.behaviour, context=self.context)

            # asyncio.shield allows the task to continue after a first cancelled error.
            # This is to prevent this task being cancelled when the ActorCell task is cancelled.
            self._task = asyncio.shield(
                asyncio.create_task(
                    self._actor.run(), name=f"actor-{self.context.ref.id}"
                )
            )

            while True:
                result = await self._task
                await self.handle(result)
        except (asyncio.CancelledError, Stop):
            logger.debug("[%s] ActorCell stopping...", self.context.ref)
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            await self.context.timers.cancel_all()

    @dispatch(actor.ActorRestarted)
    async def handle(self, msg: actor.ActorRestarted):
        logger.debug("[%s] ActorCell ActorRestarted", self.context.ref)
        await self.context.timers.cancel_all()
        self._actor = actor.Actor(start=msg.behaviour, context=self.context)
        self._task = asyncio.shield(asyncio.create_task(self._actor.run()))

    @dispatch(actor.ActorKilled)
    async def handle(self, msg: actor.ActorKilled):
        logger.debug("[%s] ActorCell ActorKilled", self.context.ref)

        # Notify system that actor has stopped in order to remove it
        # and its descendants from the system and notify its parent if
        # it is being watched.
        # ActorCell.ActorKilled -> System.ActorStopped -> Dispatcher.RemoveActor -> Spawner.RemoveActor
        await self.context.system.tell(ActorStopped(ref=self.context.ref))

        raise Stop

    @dispatch(actor.ActorFailed)
    async def handle(self, msg: actor.ActorFailed):
        logger.debug("[%s] ActorCell ActorFailed", self.context.ref)

        # Notify system that actor has stopped in order to remove it
        # and its children from the system and notify its parent if
        # it is being watched.
        # ActorCell.ActorKilled -> System.ActorStopped -> Dispatcher.RemoveActor -> Spawner.RemoveActor
        await self.context.system.tell(ActorStopped(ref=self.context.ref))

        raise Stop

    @dispatch(actor.ActorStopped)
    async def handle(self, msg: actor.ActorStopped):
        logger.debug("[%s] ActorCell ActorStopped", self.context.ref)

        await self.context.system.tell(ActorStopped(ref=self.context.ref))

        raise Stop


@attr.s
class BootstrapActorCell:
    """
    Actor cell for executing the bootstrap
    actors of the system.
    """

    behaviour: Behaviour = attr.ib()
    context: actor.ActorContext = attr.ib(validator=instance_of(actor.ActorContext))

    async def run(self):
        try:
            act = actor.Actor(start=self.behaviour, context=self.context)

            # asyncio.shield allows the task to continue after a first cancelled error.
            # This is to prevent this task being cancelled when the ActorCell task is cancelled.
            task = asyncio.shield(
                asyncio.create_task(act.run(), name=f"actor-{self.context.ref.id}")
            )

            # Fail fast if we're a bootstrap actor as
            # we rely on these working correctly.
            result = await task
            await self.handle(result)
        except (asyncio.CancelledError, Stop):
            logger.debug("[%s] ActorCell stopping...", self.context.ref)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await self.context.timers.cancel_all()

    @dispatch(actor.ActorStopped)
    async def handle(self, msg: actor.ActorStopped):
        logger.debug("[%s] ActorCell ActorStopped", self.context.ref)

        raise Stop
