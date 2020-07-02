import attr
from attr.validators import instance_of, optional
import json
from sqlalchemy import create_engine, and_
from sqlalchemy_aio import ASYNCIO_STRATEGY
from typing import Any, Iterable, Optional, Tuple

from phoenix.persistence.db import events_table
from phoenix.persistence.persistence_id import PersistenceId
from phoenix.ref import Ref


@attr.s(frozen=True)
class Event:
    persistence_id: PersistenceId = attr.ib(validator=instance_of(PersistenceId))
    offset: int = attr.ib(validator=instance_of(int))
    sequence_number: int = attr.ib(validator=instance_of(int))
    event: Any = attr.ib()


@attr.s(frozen=True)
class Events:
    events: Iterable[Event] = attr.ib()


@attr.s(frozen=True)
class ListEvents:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    after_offset: Optional[int] = attr.ib(validator=optional(instance_of(int)))


@attr.s(frozen=True)
class ListEventsByTag:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    after_offset: Optional[int] = attr.ib(validator=optional(instance_of(int)))
    tag: str = attr.ib(validator=instance_of(str))


@attr.s(frozen=True)
class ListEventsByPersistenceId:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    after_sequence_number: Optional[int] = attr.ib(validator=optional(instance_of(int)))
    persistence_id: PersistenceId = attr.ib(validator=instance_of(PersistenceId))


class SqliteReadJournal:
    """
    Load events in sequence from an event-sourced data store.
    """

    @staticmethod
    def start(db_url: str, decode) -> Behaviour:
        async def setup(context):
            engine = create_engine(db_url, strategy=ASYNCIO_STRATEGY)
            return SqliteReadJournal.active(engine, decode)

        return behaviour.setup(setup)

    @staticmethod
    def active(engine, decode) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(ListEvents, namespace=dispatch_namespace)
        async def handle(msg: ListEvents):
            async with self.engine.connect() as conn:
                async with conn.begin():
                    query = (
                        events_table.select()
                        .order_by(events_table.c.id.asc())
                        .limit(100)
                    )
                    if msg.after_offset is not None:
                        query = query.where(events_table.c.id > msg.after_offset)
                    events = await conn.execute(query)
                    events = await events.fetchall()
            events = [
                Event(
                    persistence_id=PersistenceId(
                        type_=event.type, entity_id=event.entity_id
                    ),
                    event=decode(topic_id=event.topic_id, data=json.loads(event.data)),
                    offset=event.id,
                    sequence_number=event.offset,
                )
                for event in events
            ]
            await msg.reply_to.tell(Events(events))
            return behaviour.same()

        @dispatch(ListEventsByTag, namespace=dispatch_namespace)
        async def handle(msg: ListEventsByTag):
            async with self.engine.connect() as conn:
                async with conn.begin():
                    query = (
                        events_table.select()
                        .order_by(events_table.c.id.asc())
                        .limit(100)
                    )
                    if msg.after_offset is not None:
                        query = query.where(events_table.c.id > msg.after_offset)
                    query = query.join(
                        event_tags_table.c.entity_id == events_table.id
                    ).where(event_tags_table.c.tag == msg.tag)
                    events = await conn.execute(query)
                    events = await events.fetchall()
            events = [
                Event(
                    persistence_id=PersistenceId(
                        type_=event.type, entity_id=event.entity_id
                    ),
                    event=decode(topic_id=event.topic_id, data=json.loads(event.data)),
                    offset=event.id,
                    sequence_number=event.offset,
                )
                for event in events
            ]
            await msg.reply_to.tell(Events(events))
            return behaviour.same()

        @dispatch(ListEventsByPersistenceId, namespace=dispatch_namespace)
        async def handle(msg: ListEventsByPersistenceId):
            async with self.engine.connect() as conn:
                async with conn.begin():
                    query = (
                        events_table.select()
                        .where(
                            and_(
                                events_table.c.type == msg.persistence_id.type_,
                                events_table.c.entity_id
                                == msg.persistence_id.entity_id,
                            )
                        )
                        .order_by(events_table.c.id.asc())
                        .limit(100)
                    )
                    if msg.after_sequence_number is not None:
                        query = query.where(
                            events_table.c.offset > msg.after_sequence_number
                        )

                    events = await conn.execute(query)
                    events = await events.fetchall()
            events = [
                Event(
                    persistence_id=PersistenceId(
                        type_=event.type, entity_id=event.entity_id
                    ),
                    event=decode(topic_id=event.topic_id, data=json.loads(event.data)),
                    offset=event.id,
                    sequence_number=event.offset,
                )
                for event in events
            ]
            await msg.reply_to.tell(Events(events))
            return behaviour.same()

        async def recv(msg):
            return await handle(msg)

        return behaviour.receive(recv)
