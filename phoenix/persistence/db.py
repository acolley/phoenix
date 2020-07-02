from datetime import datetime
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.schema import CreateTable


metadata = MetaData()
events_table = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("timestamp", DateTime, default=datetime.utcnow),
    Column("type", String),
    Column("entity_id", String),
    Column("offset", Integer),
    Column("topic_id", Text),
    Column("data", Text),
    UniqueConstraint("type", "entity_id", "offset"),
)

event_tags_table = Table(
    "event_tags",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("event_id", Integer, ForeignKey("events.id")),
    Column("tag", String),
    UniqueConstraint("event_id", "tag"),
)
