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


@attr.s
class NotFound:
    pass


class Persister:
    @staticmethod
    def start(store_factory) -> Behaviour:
        async def f(context):
            store = store_factory()
            await store.create_schema()
            return Persister.active(store)

        return behaviour.setup(f)

    @staticmethod
    def active(store) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(Persist, namespace=dispatch_namespace)
        async def handle(msg: Persist):
            offset = await store.persist(
                entity_id=msg.id, events=msg.events, offset=msg.offset
            )
            await msg.reply_to.tell(Persisted(offset=offset))
            return behaviour.same()

        @dispatch(Load, namespace=dispatch_namespace)
        async def handle(msg: Load):
            (events, offset) = await store.load(entity_id=msg.id)
            if events:
                await msg.reply_to.tell(Loaded(events=events, offset=offset))
            else:
                await msg.reply_to.tell(NotFound())
            return behaviour.same()

        async def f(msg):
            return await handle(msg)

        return behaviour.receive(f)
