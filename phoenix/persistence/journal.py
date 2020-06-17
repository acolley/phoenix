import attr
from attr.validators import instance_of
from typing import Any, Iterable, Optional, Tuple

from phoenix.persistence.db import events_table


@attr.s
class Event:
    entity_id: str = attr.ib(validator=instance_of(str))
    event: Any = attr.ib()


@attr.s
class SqlAlchemyReadJournal:
    """
    Load events in sequence from an event-sourced data store.
    """
    engine = attr.ib()
    decode = attr.ib()

    async def load(self, offset: Optional[int] = None, limit: int = 100) -> Tuple[Iterable[Any], int]:
        if limit > 100:
            raise ValueError("Maximum limit of 100.")

        async with self.engine.connect() as conn:
            async with conn.begin():
                query = events_table.select()
                if offset is not None:
                    query = events_table.select(events_table.c.id > offset)
                query = query.limit(limit)
                events = await conn.execute(query)
                events = await events.fetchall()
        events = [Event(entity_id=x.entity_id, event=self.decode(topic_id=x.topic_id, data=x.data)) for x in events]
        if events:
            offset = events[-1].id
        else:
            offset = -1
        return (events, offset)
