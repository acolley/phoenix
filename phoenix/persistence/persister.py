import attr
from attr.validators import instance_of, optional
from multipledispatch import dispatch
from typing import Any, Generic, Iterable, Optional, TypeVar

from phoenix import behaviour
from phoenix.behaviour import Behaviour
from phoenix.persistence.event import Event
from phoenix.persistence.snapshot import Snapshot
from phoenix.ref import Ref
from phoenix import registry


@attr.s
class PersistEvents:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    entity_id: str = attr.ib(validator=instance_of(str))
    events: Iterable[Event] = attr.ib()
    offset: int = attr.ib(validator=instance_of(int))


T = TypeVar("T")


@attr.s
class Success(Generic[T]):
    value: T = attr.ib()


@attr.s
class LoadEvents:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    entity_id: str = attr.ib(validator=instance_of(str))
    after: Optional[int] = attr.ib(validator=optional(instance_of(int)), default=None)


@attr.s
class Events:
    events: Iterable[Event] = attr.ib()
    offset: int = attr.ib(validator=instance_of(int))


@attr.s
class NotFound:
    pass


@attr.s
class PersistSnapshot:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    snapshot: Snapshot = attr.ib(validator=instance_of(Snapshot))


@attr.s
class LoadLatestSnapshot:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    entity_id: str = attr.ib(validator=instance_of(str))


class Persister:
    @staticmethod
    def start(event_store_factory, snapshot_store_factory) -> Behaviour:
        async def f(context):
            event_store = event_store_factory()
            await event_store.create_schema()
            snapshot_store = snapshot_store_factory()
            await snapshot_store.create_schema()
            await context.registry.tell(
                registry.Register(key="persister", ref=context.ref)
            )
            return Persister.active(event_store, snapshot_store)

        return behaviour.setup(f)

    @staticmethod
    def active(event_store, snapshot_store) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(PersistEvents, namespace=dispatch_namespace)
        async def handle(msg: PersistEvents):
            offset = await event_store.persist(
                entity_id=msg.entity_id, events=msg.events, offset=msg.offset
            )
            await msg.reply_to.tell(Success(offset))
            return behaviour.same()

        @dispatch(LoadEvents, namespace=dispatch_namespace)
        async def handle(msg: LoadEvents):
            (events, offset) = await event_store.load(
                entity_id=msg.entity_id, offset=msg.after
            )
            if events:
                await msg.reply_to.tell(Events(events=events, offset=offset))
            else:
                await msg.reply_to.tell(NotFound())
            return behaviour.same()

        @dispatch(PersistSnapshot, namespace=dispatch_namespace)
        async def handle(msg: PersistSnapshot):
            await snapshot_store.persist(msg.snapshot)
            await msg.reply_to.tell(Success(None))
            return behaviour.same()

        @dispatch(LoadLatestSnapshot, namespace=dispatch_namespace)
        async def handle(msg: LoadLatestSnapshot):
            snapshot = await snapshot_store.load_latest(entity_id=msg.entity_id)
            if snapshot is None:
                await msg.reply_to.tell(NotFound())
            else:
                await msg.reply_to.tell(Success(snapshot))
            return behaviour.same()

        async def f(msg):
            return await handle(msg)

        return behaviour.receive(f)
