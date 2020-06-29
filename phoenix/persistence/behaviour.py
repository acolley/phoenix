import asyncio
import attr
from attr.validators import instance_of
import json
import logging
from multipledispatch import dispatch
from typing import Callable, Coroutine, Generic, Optional, TypeVar

from phoenix import registry
from phoenix.persistence import effect, persister

logger = logging.getLogger(__name__)


S = TypeVar("S")
C = TypeVar("C")
E = TypeVar("E")


@attr.s(frozen=True)
class Persist(Generic[S, C, E]):
    id: str = attr.ib(validator=instance_of(str))
    empty_state: S = attr.ib()
    command_handler: Callable[[S, C], Coroutine[effect.Effect, None, None]] = attr.ib()
    event_handler: Callable[[S, E], Coroutine[S, None, None]] = attr.ib()
    encode: Callable[[E], dict] = attr.ib()
    decode: Callable[[dict], E] = attr.ib()
    on_lifecycle: Optional[Callable] = attr.ib(default=None)

    def with_on_lifecycle(self, on_lifecycle: Callable):
        return attr.evolve(self, on_lifecycle=on_lifecycle)

    async def execute(self, context):
        logger.debug("[%s] Executing %s", context.ref, self.__class__.__name__)

        # Persister might take some time to register
        while True:
            response = await context.registry.ask(
                lambda reply_to: registry.Find(reply_to=reply_to, key="persister")
            )
            if isinstance(response, registry.NotFound):
                await asyncio.sleep(1)
            else:
                persister_ref = next(iter(response.refs))
                break

        dispatcher_namespace = {}

        @dispatch(object, effect.Persist, int, namespace=dispatcher_namespace)
        async def execute_effect(state, eff: effect.Persist, offset: int):
            logger.debug("[%s] Persisting events.", context.ref)
            events = [self.encode(x) for x in eff.events]
            events = [
                persister.Event(topic_id=topic_id, data=json.dumps(data))
                for topic_id, data in events
            ]
            reply = await persister_ref.ask(
                lambda reply_to: persister.Persist(
                    reply_to=reply_to, id=self.id, events=events, offset=offset
                )
            )

            if eff.reply:
                await execute_effect(state, eff.reply, offset)

            for event in eff.events:
                state = await self.event_handler(state, event)

            return state, reply.offset + 1

        @dispatch(object, effect.NoEffect, int, namespace=dispatcher_namespace)
        async def execute_effect(state, eff: effect.NoEffect, offset: int):
            return state, offset

        @dispatch(object, effect.Reply, int, namespace=dispatcher_namespace)
        async def execute_effect(state, eff: effect.Reply, offset: int):
            await eff.reply_to.tell(eff.msg)
            return state, offset

        state = self.empty_state
        reply = await persister_ref.ask(
            lambda reply_to: persister.Load(reply_to=reply_to, id=self.id)
        )
        if isinstance(reply, persister.Loaded):
            logger.debug("[%s] Recovering from persisted events.", context.ref)
            for event in reply.events:
                data = json.loads(event.data)
                event = self.decode(topic_id=event.topic_id, data=data)
                state = await self.event_handler(state, event)
            offset = reply.offset + 1
        else:
            offset = 0

        while True:
            msg = await context.ref.inbox.async_q.get()

            eff = await self.command_handler(state, msg)
            state, offset = await execute_effect(state, eff, offset)

            context.ref.inbox.async_q.task_done()