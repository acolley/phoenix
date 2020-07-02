import attr
from attr.validators import instance_of


@attr.s(frozen=True)
class PersistenceId:
    type_: str = attr.ib(validator=instance_of(str))
    entity_id: str = attr.ib(validator=instance_of(str))
