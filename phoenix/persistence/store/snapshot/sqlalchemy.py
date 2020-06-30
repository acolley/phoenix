import asyncio
import attr
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, UniqueConstraint
from sqlalchemy.schema import CreateTable
from typing import Iterable, Optional, Tuple

from phoenix.persistence.snapshot import Snapshot


metadata = MetaData()
snapshot_table = Table(
    "snapshots",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("entity_id", String),
    Column("offset", Integer),
    Column("topic_id", Text),
    Column("data", Text),
    UniqueConstraint("entity_id", "offset"),
)


@attr.s
class SqlAlchemySnapshotStore:

    engine = attr.ib()

    async def create_schema(self):
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: metadata.create_all(bind=self.engine.sync_engine)
        )

    async def store(self, snapshot: Snapshot):
        async with self.engine.connect() as conn:
            async with conn.begin():
                await conn.execute(
                    snapshot_table.insert().values(
                        id=None,
                        entity_id=snapshot.entity_id,
                        offset=snapshot.offset,
                        topic_id=snapshot.topic_id,
                        data=snapshot.data,
                    )
                )

    async def load_latest(self, entity_id: str) -> Optional[Snapshot]:
        """
        Load the latest snapshot for the given entity_id. This is
        the snapshot with the highest offset.
        """
        async with self.engine.connect() as conn:
            async with conn.begin():
                query = snapshot_table.select(
                    snapshot_table.c.entity_id == entity_id
                ).order_by(snapshot_table.c.offset.desc())
                snapshot = await conn.execute(query)
                snapshot = await snapshot.fetchone()
        return snapshot
