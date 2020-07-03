import asyncio
import attr
from attr.validators import instance_of, optional
import json
from multipledispatch import dispatch
from sqlalchemy import create_engine, and_
from sqlalchemy_aio import ASYNCIO_STRATEGY
from typing import Any, Iterable, Optional, Tuple

from phoenix import behaviour
from phoenix.behaviour import Behaviour
from phoenix.persistence.db import events_table, event_tags_table
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



# FIXME: Journal querying can be a bottleneck.
# 1. Make journal querying faster
#     * Reduce event limit to be queried at once.
#     * Sqlite is a bottleneck.
# 2. Timeouts to reduce knock-on effects.
#     * Has the side-effect that the journal still has to
#       process the message, so ultimately delays in state
#       update are inevitable.
#         * Stream processing with backpressure...
# 3. Provide timer messages that are only scheduled after
# the previous one has been processed.
# 4. Make message processing push rather than pull-based, i.e.
# actor registers for new messages with the journal.
#     * Pro: journal can fairly distribute messages between
#           subscribers.
#     * Con: if actor dies messages will still be sent unnecessarily.
#         * Watch subscribed actors in order to remove them on death.
# 5. Journal bounded mailbox will apply backpressure to producers.
# 6. Persist state and offset of processors to speed up recovery.
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
            async with engine.connect() as conn:
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
            events = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: [
                    Event(
                        persistence_id=PersistenceId(
                            type_=event.type, entity_id=event.entity_id
                        ),
                        event=decode(
                            topic_id=event.topic_id, data=json.loads(event.data)
                        ),
                        offset=event.id,
                        sequence_number=event.offset,
                    )
                    for event in events
                ],
            )
            await msg.reply_to.tell(Events(events))
            return behaviour.same()

        @dispatch(ListEventsByTag, namespace=dispatch_namespace)
        async def handle(msg: ListEventsByTag):
            async with engine.connect() as conn:
                async with conn.begin():
                    query = (
                        events_table.select()
                        .select_from(
                            events_table.join(
                                event_tags_table,
                                event_tags_table.c.event_id == events_table.c.id,
                            )
                        )
                        .where(event_tags_table.c.tag == msg.tag)
                        .order_by(events_table.c.id.asc())
                        .limit(100)
                    )
                    if msg.after_offset is not None:
                        query = query.where(events_table.c.id > msg.after_offset)
                    events = await conn.execute(query)
                    events = await events.fetchall()
            events = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: [
                    Event(
                        persistence_id=PersistenceId(
                            type_=event.type, entity_id=event.entity_id
                        ),
                        event=decode(
                            topic_id=event.topic_id, data=json.loads(event.data)
                        ),
                        offset=event.id,
                        sequence_number=event.offset,
                    )
                    for event in events
                ],
            )
            await msg.reply_to.tell(Events(events))
            return behaviour.same()

        @dispatch(ListEventsByPersistenceId, namespace=dispatch_namespace)
        async def handle(msg: ListEventsByPersistenceId):
            async with engine.connect() as conn:
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
            events = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: [
                    Event(
                        persistence_id=PersistenceId(
                            type_=event.type, entity_id=event.entity_id
                        ),
                        event=decode(
                            topic_id=event.topic_id, data=json.loads(event.data)
                        ),
                        offset=event.id,
                        sequence_number=event.offset,
                    )
                    for event in events
                ],
            )
            await msg.reply_to.tell(Events(events))
            return behaviour.same()

        async def recv(msg):
            return await handle(msg)

        return behaviour.receive(recv)
