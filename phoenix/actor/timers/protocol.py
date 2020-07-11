import asyncio
import attr
from attr.validators import instance_of
from typing import Any


@attr.s(frozen=True)
class FixedDelayEnvelope:
    msg: Any = attr.ib()
    event: asyncio.Event = attr.ib(validator=instance_of(asyncio.Event))
