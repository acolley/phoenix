import abc
import asyncio
import attr
from attr.validators import instance_of, optional
from functools import partial
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
class Retention:
    number_of_events: int = attr.ib(validator=instance_of(int))
    keep_n_snapshots: int = attr.ib(validator=instance_of(int))


@attr.s(frozen=True)
class Persist(Generic[S, C, E]):
    id: str = attr.ib(validator=instance_of(str))
    empty_state: S = attr.ib()
    command_handler: Callable[[S, C], Coroutine[effect.Effect, None, None]] = attr.ib()
    event_handler: Callable[[S, E], Coroutine[S, None, None]] = attr.ib()
    encode: Callable[[E], dict] = attr.ib()
    decode: Callable[[dict], E] = attr.ib()
    on_lifecycle: Optional[Callable] = attr.ib(default=None)
    retention: Optional[Retention] = attr.ib(
        validator=optional(instance_of(Retention)), default=None
    )

    def with_on_lifecycle(self, on_lifecycle: Callable) -> "Persist":
        return attr.evolve(self, on_lifecycle=on_lifecycle)

    def with_retention(self, number_of_events: int, keep_n_snapshots: int) -> "Persist":
        return attr.evolve(
            self,
            retention=Retention(
                number_of_events=number_of_events, keep_n_snapshots=keep_n_snapshots
            ),
        )

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
                lambda reply_to: persister.PersistEvents(
                    reply_to=reply_to, entity_id=self.id, events=events, offset=offset
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

        # Check for a snapshot
        state = await persister_ref.ask(
            lambda reply_to: persister.LoadLatestSnapshot(
                reply_to=reply_to, entity_id=self.id
            )
        )
        if isinstance(state, persister.Success):
            logger.debug("[%s] Recovering from snapshot.", context.ref)
            state = await asyncio.get_event_loop().run_in_executor(
                None, partial(json.loads, state.value)
            )
            state = await asyncio.get_event_loop().run_in_executor(
                None, partial(self.decode, topic_id=state.topic_id, data=state.data)
            )
            offset = state.offset
            last_state_offset = state.offset
        else:
            state = self.empty_state
            offset = None
            last_state_offset = None

        # Load remaining events (if any)
        events = await persister_ref.ask(
            lambda reply_to: persister.LoadEvents(
                reply_to=reply_to, entity_id=self.id, after=offset
            )
        )
        if isinstance(events, persister.Events):
            logger.debug("[%s] Recovering from persisted events.", context.ref)
            for event in events.events:
                data = await asyncio.get_event_loop().run_in_executor(
                    None, partial(json.loads, event.data)
                )
                event = self.decode(topic_id=event.topic_id, data=data)
                state = await self.event_handler(state, event)
            offset = events.offset + 1

        if offset is None:
            offset = 0

        while True:
            msg = await context.ref.inbox.async_q.get()

            eff = await self.command_handler(state, msg)
            state, offset = await execute_effect(state, eff, offset)

            if self.retention:
                if last_state_offset is None:
                    pass
                else:
                    pass

            context.ref.inbox.async_q.task_done()
