import asyncio
import attr
from attr.validators import deep_iterable, instance_of
import concurrent.futures
from functools import partial
import logging
from multipledispatch import dispatch
from sqlalchemy import create_engine, and_
from sqlalchemy_aio import ASYNCIO_STRATEGY
from typing import Iterable, Optional, Set, Tuple

from phoenix import behaviour, routers
from phoenix.behaviour import Behaviour
from phoenix.persistence.db import events_table, event_tags_table, metadata
from phoenix.persistence.persistence_id import PersistenceId
from phoenix.ref import Ref

logger = logging.getLogger(__name__)


@attr.s
class PersistEvent:
    topic_id: str = attr.ib(validator=instance_of(str))
    data: str = attr.ib(validator=instance_of(str))
    tags: Set[str] = attr.ib(
        validator=deep_iterable(
            member_validator=instance_of(str), iterable_validator=instance_of(set)
        )
    )


@attr.s
class Event:
    topic_id: str = attr.ib(validator=instance_of(str))
    data: str = attr.ib(validator=instance_of(str))


@attr.s
class Persist:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: PersistenceId = attr.ib(validator=instance_of(PersistenceId))
    events: Iterable[Event] = attr.ib()
    offset: int = attr.ib(validator=instance_of(int))


@attr.s
class Persisted:
    offset: int = attr.ib(validator=instance_of(int))


@attr.s
class Load:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: PersistenceId = attr.ib(validator=instance_of(PersistenceId))


@attr.s
class Loaded:
    events: Iterable[Event] = attr.ib()
    offset: int = attr.ib(validator=instance_of(int))


@attr.s
class NotFound:
    pass


def _persist(db_url: str, id: PersistenceId, offset: int, events: Iterable[Event]):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        with conn.begin():
            for i, event in enumerate(events):
                result = conn.execute(
                    events_table.insert().values(
                        id=None,
                        type=id.type_,
                        entity_id=id.entity_id,
                        offset=offset + i,
                        topic_id=event.topic_id,
                        data=event.data,
                    )
                )
                [event_id] = result.inserted_primary_key
                for tag in event.tags:
                    conn.execute(
                        event_tags_table.insert().values(
                            id=None, event_id=event_id, tag=tag,
                        )
                    )
    return offset + i


class Writer:
    @staticmethod
    def start(db_url: str) -> Behaviour:
        async def setup(context):
            async def recv(msg: Persist):
                with concurrent.futures.ProcessPoolExecutor() as pool:
                    result = await asyncio.get_event_loop().run_in_executor(
                        context.executor,
                        partial(
                            _persist,
                            db_url=db_url,
                            id=msg.id,
                            offset=msg.offset,
                            events=msg.events,
                        ),
                    )
                await msg.reply_to.tell(Persisted(result))
                return behaviour.same()

            return behaviour.receive(recv)

        return behaviour.setup(setup)


class Reader:
    @staticmethod
    def start(engine) -> Behaviour:
        async def recv(msg: Load):
            async with engine.connect() as conn:
                async with conn.begin():
                    query = (
                        events_table.select()
                        .where(
                            and_(
                                events_table.c.type == msg.id.type_,
                                events_table.c.entity_id == msg.id.entity_id,
                            )
                        )
                        .order_by(events_table.c.offset.asc())
                    )
                    events = await conn.execute(query)
                    events = await events.fetchall()

            if events:
                offset = events[-1].offset
            else:
                offset = -1

            events = [Event(topic_id=x.topic_id, data=x.data) for x in events]
            if events:
                await msg.reply_to.tell(Loaded(events=events, offset=offset))
            else:
                await msg.reply_to.tell(NotFound())

            return behaviour.same()

        return behaviour.receive(recv)


class SqliteStore:
    """
    Event-sourcing sqlite data store.
    """

    @staticmethod
    def start(db_url: str) -> Behaviour:
        async def setup(context):
            engine = create_engine(db_url, strategy=ASYNCIO_STRATEGY)
            result = await engine.execute("PRAGMA journal_mode=WAL")
            result, *_ = await result.fetchone()
            if result != "wal":
                raise ValueError("sqlite database does not support WAL mode.")
            await engine.execute("PRAGMA default_cache_size=200000")
            await engine.execute("PRAGMA page_size=4096")
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: metadata.create_all(bind=engine.sync_engine)
            )
            # Split sqlite store into a single writer and
            # multiple readers due to lack of support for
            # concurrent writing.
            writer = await context.spawn(
                partial(Writer.start, db_url), "sqlite-store-writer"
            )
            reader_pool = routers.pool(4)(partial(Reader.start, engine))
            reader = await context.spawn(reader_pool, "sqlite-store-reader")
            return SqliteStore.active(writer, reader)

        return behaviour.setup(setup)

    @staticmethod
    def active(writer, reader) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(Persist, namespace=dispatch_namespace)
        async def handle(msg: Persist):
            await writer.tell(msg)
            return behaviour.same()

        @dispatch(Load, namespace=dispatch_namespace)
        async def handle(msg: Load):
            await reader.tell(msg)
            return behaviour.same()

        async def recv(msg):
            return await handle(msg)

        return behaviour.receive(recv)
