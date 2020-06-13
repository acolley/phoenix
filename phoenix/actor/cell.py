import asyncio
import attr
from attr.validators import instance_of, optional
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
    _timers: Optional[Timers] = attr.ib(
        init=False, validator=optional(instance_of(Timers)), default=None
    )

    async def run(self):
        self._timers = Timers(ref=self.context.ref, lock=asyncio.Lock())
        self._actor = actor.Actor(
            start=self.behaviour, context=self.context, timers=self._timers
        )

        try:
            self._task = asyncio.create_task(self._actor.run())

            while True:
                result = await self._task
                await self.handle(result)
        except (asyncio.CancelledError, Stop):
            logger.debug("[%s] ActorCell stopping...", self.context.ref)
            self._task.cancel()
            await self._timers.cancel_all()

    @dispatch(actor.ActorRestarted)
    async def handle(self, msg: actor.ActorRestarted):
        logger.debug("[%s] ActorCell ActorRestarted", self.context.ref)
        await self._timers.cancel_all()
        self._timers = Timers(ref=self.context.ref, lock=asyncio.Lock())
        self._actor = actor.Actor(
            start=msg.behaviour, context=self.context, timers=self._timers
        )
        self._task = asyncio.create_task(self._actor.run())

    @dispatch(actor.ActorKilled)
    async def handle(self, msg: actor.ActorKilled):
        logger.debug("[%s] ActorCell ActorKilled", self.context.ref)
        # TODO: Notify parent

        # Notify system that actor has stopped in order to remove it
        # and its descendants from the system and notify its parent if
        # it is being watched.
        # ActorCell.ActorKilled -> System.ActorStopped -> Dispatcher.ActorStopped -> Spawner.ActorStopped
        await self.context.system.tell(ActorStopped(ref=self.context.ref))

        raise Stop

    @dispatch(actor.ActorFailed)
    async def handle(self, msg: actor.ActorFailed):
        logger.debug("[%s] ActorCell ActorFailed", self.context.ref)
        # TODO: Notify parent

        # Notify system that actor has stopped in order to remove it
        # and its children from the system and notify its parent if
        # it is being watched.
        # ActorCell.ActorKilled -> System.ActorStopped -> Dispatcher.ActorStopped -> Spawner.ActorStopped
        # FIXME: children stopped by the system should not need to notify the system that they were stopped.
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
        timers = Timers(ref=self.context.ref, lock=asyncio.Lock())
        act = actor.Actor(start=self.behaviour, context=self.context, timers=timers)

        task = asyncio.create_task(act.run())

        # Fail fast if we're a bootstrap actor as
        # we rely on these working correctly.
        await task
