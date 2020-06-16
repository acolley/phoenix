import attr
from attr.validators import deep_iterable, instance_of
from typing import Any, Iterable, Union

from phoenix.ref import Ref


@attr.s(frozen=True)
class Persist:
    events: Iterable[Any] = attr.ib()


def persist(events: Iterable[Any]) -> Persist:
    return Persist(events=events)


@attr.s(frozen=True)
class Reply:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    msg: Any = attr.ib()


def reply(reply_to: Ref, msg: Any) -> Reply:
    return Reply(reply_to=reply_to, msg=msg)


@attr.s
class NoEffect:
    pass


def none() -> NoEffect:
    return NoEffect()


Effect = Union[Persist, Reply]
