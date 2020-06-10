import asyncio
import attr
from attr.validators import instance_of, optional
from multipledispatch import dispatch
from typing import Optional

from phoenix.actor import actor
from phoenix.actor.timers import Timers
from phoenix.behaviour import Behaviour


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
    _timers: Optional[Timers] = attr.ib(init=False, validator=optional(instance_of(Timers)), default=None)

    async def run(self):
        self._timers = Timers(ref=self.context.ref, lock=asyncio.Lock())
        self._actor = actor.Actor(start=self.behaviour, context=self.context, timers=self._timers)

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

    @dispatch(actor.ActorRestarted)
    async def handle(self, msg: actor.ActorRestarted):
        await self._timers.cancel_all()
        self._timers = Timers(ref=self.context.ref, lock=asyncio.Lock())
        self._actor = actor.Actor(start=msg.behaviour, context=self.context, timers=self._timers)
        self._task = asyncio.create_task(self._actor.run())
    
    @dispatch(actor.ActorKilled)
    async def handle(self, msg: actor.ActorKilled):
        # TODO: Notify parent
        # NOTE: is this thread-safe???
        for child in self.context.children:
            await child.stop()

    @dispatch(actor.ActorFailed)
    async def handle(self, msg: actor.ActorFailed):
        # TODO: Notify parent
        # NOTE: is this thread-safe???
        for child in self.context.children:
            await child.stop()
