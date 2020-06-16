import asyncio
import attr
from attr.validators import instance_of
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, UniqueConstraint
from sqlalchemy.schema import CreateTable
from typing import Iterable, Optional


metadata = MetaData()
events_table = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("entity_id", String),
    Column("offset", Integer),
    Column("topic_id", Text),
    Column("data", Text),
    UniqueConstraint("entity_id", "offset"),
)


@attr.s
class Event:
    topic_id: str = attr.ib(validator=instance_of(str))
    data: str = attr.ib(validator=instance_of(str))


@attr.s
class SqlAlchemyStore:
    engine = attr.ib()

    async def create_schema(self):
        await asyncio.get_event_loop().run_in_executor(None, lambda: metadata.create_all(bind=self.engine.sync_engine))

    async def persist(self, entity_id: str, events: Iterable[Event], offset: int) -> int:
        async with self.engine.connect() as conn:
            async with conn.begin():
                for i, event in enumerate(events):
                    await conn.execute(
                        events_table.insert().values(
                            id=None,
                            entity_id=entity_id,
                            offset=offset + i,
                            topic_id=event.topic_id,
                            data=event.data,
                        )
                    )
        return offset + i

    async def load(
        self, entity_id: str, offset: Optional[int] = None
    ) -> Iterable[Event]:
        async with self.engine.connect() as conn:
            async with conn.begin():
                query = events_table.select(events_table.c.entity_id == entity_id)
                if offset is not None:
                    query = query.select(events_table.c.offset > offset)
                events = await conn.execute(query)
                events = await events.fetchall()
        if events:
            offset = events[-1].offset
        else:
            offset = -1
        return ([Event(topic_id=x.topic_id, data=x.data) for x in events], offset)
