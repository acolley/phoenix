import attr
from attr.validators import instance_of
from multipledispatch import dispatch
from typing import Any

from phoenix import behaviour
from phoenix.behaviour import Behaviour


@attr.s(frozen=True)
class TopicPattern:
    pattern: str = attr.ib(validator=instance_of(str))

    @pattern.validator
    def check(self, attribute: str, value: str):
        pass

    def matches(self, topic: "Topic") -> bool:
        return self.pattern == topic


# @attr.s(frozen=True)
# class Subscribe:
#     ref: Ref = attr.ib(valid)


# @attr.s(frozen=True)
# class Publish:
#     topic: str = attr.ib(validator=instance_of(str))
#     msg: Any = attr.ib()


# class Channel:
#     @staticmethod
#     def start() -> Behaviour:
#         async def recv(msg):
#             return await handle(msg)
#         return behaviour.receive(recv)
