import attr
from attr.validators import instance_of


@attr.s
class Snapshot:
    topic_id: str = attr.ib(validator=instance_of(str))
    entity_id: str = attr.ib(validator=instance_of(str))
    offset: int = attr.ib(validator=instance_of(int))
    data: str = attr.ib(validator=instance_of(str))
