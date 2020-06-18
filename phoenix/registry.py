import attr
from attr.validators import deep_iterable, instance_of
from multipledispatch import dispatch
from pyrsistent import m, s
from typing import Iterable, Mapping

from phoenix import behaviour
from phoenix.behaviour import Behaviour
from phoenix.ref import Ref


@attr.s
class Register:
    key: str = attr.ib(validator=instance_of(str))
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class Find:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    key: str = attr.ib(validator=instance_of(str))


@attr.s
class Listing:
    refs: Iterable[Ref] = attr.ib(
        validator=deep_iterable(member_validator=instance_of(Ref))
    )


@attr.s
class NotFound:
    key: str = attr.ib(validator=instance_of(str))


class Registry:
    @staticmethod
    def start() -> Behaviour:
        async def f(context):
            return Registry.active(m())

        return behaviour.setup(f)

    @staticmethod
    def active(registered: Mapping[str, Iterable[Ref]]) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(Find, namespace=dispatch_namespace)
        async def handle(msg: Find):
            refs = registered.get(msg.key)
            if refs is None:
                await msg.reply_to.tell(NotFound(key=msg.key))
            else:
                await msg.reply_to.tell(Listing(refs=refs))
            return behaviour.same()

        @dispatch(Register, namespace=dispatch_namespace)
        async def handle(msg: Register):
            refs = registered.get(msg.key, s())
            return Registry.active(registered.set(msg.key, refs.add(msg.ref)))

        async def f(msg):
            return await handle(msg)

        return behaviour.receive(f)
