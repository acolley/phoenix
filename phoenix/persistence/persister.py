import attr
from attr.validators import instance_of
from multipledispatch import dispatch
from typing import Any, Iterable

from phoenix import behaviour
from phoenix.behaviour import Behaviour
from phoenix.persistence.store import Event
from phoenix.ref import Ref
from phoenix import registry


@attr.s
class Persist:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: str = attr.ib(validator=instance_of(str))
    events: Iterable[Event] = attr.ib()
    offset: int = attr.ib(validator=instance_of(int))


@attr.s
class Persisted:
    offset: int = attr.ib(validator=instance_of(int))


@attr.s
class Load:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: str = attr.ib(validator=instance_of(str))


@attr.s
class Loaded:
    events: Iterable[Event] = attr.ib()
    offset: int = attr.ib(validator=instance_of(int))


class Persister:
    @staticmethod
    def start(store) -> Behaviour:
        async def f(context):
            await store.create_schema()
            await context.registry.tell(registry.Register(key="persister", ref=context.ref))
            return Persister.active(store)
        return behaviour.setup(f)
    
    @staticmethod
    def active(store) -> Behaviour:
        @dispatch(Persist)
        async def handle(msg: Persist):
            offset = await store.persist(entity_id=msg.id, events=msg.events, offset=msg.offset)
            await msg.reply_to.tell(Persisted(offset=offset))
            return behaviour.same()

        @dispatch(Load)
        async def handle(msg: Load):
            (events, offset) = await store.load(entity_id=msg.id)
            await msg.reply_to.tell(Loaded(events=events, offset=offset))
            return behaviour.same()

        async def f(msg):
            return await handle(msg)

        return behaviour.receive(f)
