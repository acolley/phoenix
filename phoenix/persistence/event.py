import attr
from attr.validators import instance_of


@attr.s
class Event:
    topic_id: str = attr.ib(validator=instance_of(str))
    data: str = attr.ib(validator=instance_of(str))
