import attr
from attr.validators import instance_of


@attr.s(frozen=True)
class Address:
    host: str = attr.ib(validator=instance_of(str))
    port: int = attr.ib(validator=instance_of(int))
