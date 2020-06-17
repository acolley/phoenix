from sqlalchemy import Column, Integer, MetaData, String, Table, Text, UniqueConstraint
from sqlalchemy.schema import CreateTable


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
