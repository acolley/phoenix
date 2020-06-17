import attr
from attr.validators import instance_of, optional
from typing import Any, Optional

from phoenix.behaviour import Behaviour
from phoenix.ref import Ref


class Confirmation:
    pass


@attr.s(frozen=True)
class SpawnSystemActor:
    """
    Spawn a system-level actor.
    """

    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: str = attr.ib(validator=instance_of(str))
    behaviour: Behaviour = attr.ib()
    parent: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class SpawnActor:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: str = attr.ib(validator=instance_of(str))
    behaviour: Behaviour = attr.ib()
    dispatcher: Optional[str] = attr.ib(validator=optional(instance_of(str)))
    parent: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class ActorSpawned:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class StopActor:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class ActorStopped:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class WatchActor:
    """
    Watching an Actor registers the watcher (the parent)
    to receive a notification message when the watched (the child)
    is stopped.
    """

    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    ref: Ref = attr.ib(validator=instance_of(Ref))
    parent: Ref = attr.ib(validator=instance_of(Ref))
    message: Any = attr.ib()
