import attr
from attr.validators import instance_of
from typing import Callable

from phoenix.behaviour import Behaviour
from phoenix.ref import Ref


@attr.s
class SpawnActor:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    id: str = attr.ib(validator=instance_of(str))
    behaviour: Callable[[], Behaviour] = attr.ib()
    parent: Ref = attr.ib(validator=instance_of(Ref))
    mailbox = attr.ib()


@attr.s
class ActorSpawned:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class StopActor:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorStopped:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class RemoveActor:
    """
    Remove an Actor that has already been killed.

    This will fail if the Actor is still running.
    """

    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorRemoved:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class Shutdown:
    """
    Shutdown the dispatcher, stopping all actors
    that were dispatched through it.
    """

    reply_to: Ref = attr.ib(validator=instance_of(Ref))
