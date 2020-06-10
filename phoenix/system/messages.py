import attr
from attr.validators import instance_of, optional
from typing import Optional

from phoenix.behaviour import Behaviour
from phoenix.ref import Ref


@attr.s(frozen=True)
class SpawnActor:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: str = attr.ib(validator=instance_of(str))
    behaviour: Behaviour = attr.ib()
    dispatcher: Optional[str] = attr.ib(validator=optional(instance_of(str)))
    parent: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class ActorSpawned:
    cell = attr.ib()
