import attr
from attr.validators import deep_iterable, instance_of, optional
from typing import Any, Iterable, Optional, Union

from phoenix.ref import Ref


@attr.s(frozen=True)
class Reply:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    msg: Any = attr.ib()


def reply(reply_to: Ref, msg: Any) -> Reply:
    return Reply(reply_to=reply_to, msg=msg)


@attr.s(frozen=True)
class Persist:
    events: Iterable[Any] = attr.ib()
    reply: Optional[Reply] = attr.ib(
        validator=optional(instance_of(Reply)), default=None
    )

    def then_reply(self, reply_to: Ref, msg: Any) -> "Persist":
        return attr.evolve(self, reply=Reply(reply_to, msg))


def persist(events: Iterable[Any]) -> Persist:
    return Persist(events=events)


@attr.s
class NoEffect:
    pass


def none() -> NoEffect:
    return NoEffect()


Effect = Union[NoEffect, Persist, Reply]
