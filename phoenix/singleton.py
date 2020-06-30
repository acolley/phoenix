"""
Manages a set of unique actors.
"""
import attr
from attr.validators import instance_of
from multipledispatch import dispatch
from pyrsistent import m
from typing import Any, Callable, Mapping

from phoenix import behaviour
from phoenix.actor.context import ActorContext
from phoenix.behaviour import Behaviour
from phoenix.ref import Ref


@attr.s
class Envelope:
    type_: str = attr.ib(validator=instance_of(str))
    id: str = attr.ib(validator=instance_of(str))
    msg: Any = attr.ib()


@attr.s
class TypeNotKnown:
    pass


@attr.s
class ActorStopped:
    type_: str = attr.ib(validator=instance_of(str))
    id: str = attr.ib(validator=instance_of(str))


@attr.s
class Entity:
    type_: str = attr.ib(validator=instance_of(str))
    entity_id: str = attr.ib(validator=instance_of(str))


# TODO: support passivating persistent actors for resource conservation


class Singleton:
    @staticmethod
    def start(actor_factory: Mapping[str, Callable[[str], Behaviour]]) -> Behaviour:
        async def f(context):
            return Singleton.active(
                context=context, actor_factory=actor_factory, actors=m()
            )

        return behaviour.setup(f)

    @staticmethod
    def active(
        context: ActorContext,
        actor_factory: Mapping[str, Callable[[str], Behaviour]],
        actors: Mapping[str, Mapping[str, Ref]],
    ):
        dispatch_namespace = {}

        @dispatch(Envelope, namespace=dispatch_namespace)
        async def handle(msg: Envelope):
            try:
                factory = actor_factory[msg.type_]
            except KeyError:
                await msg.reply_to.tell(TypeNotKnown())
                return behaviour.same()

            actors_of_type = actors.get(msg.type_, m())
            try:
                actor_ref = actors_of_type[msg.id]
            except KeyError:
                actor_ref = await context.spawn(
                    factory(Entity(type_=msg.type_, entity_id=msg.id)),
                    f"{msg.type_}-{msg.id}",
                )
                await context.watch(actor_ref, ActorStopped(msg.type_, msg.id))
                actors_of_type = actors_of_type.set(msg.id, actor_ref)

            # FIXME: potential race condition. If this actor
            # is exiting and it is not supervised for restart
            # this message will never arrive or be responded to.
            # TODO: re-route deadletters to here and re-forward
            # them once the actor is restarted?
            await actor_ref.tell(msg.msg)
            return Singleton.active(
                context=context,
                actor_factory=actor_factory,
                actors=actors.set(msg.type_, actors_of_type),
            )

        @dispatch(ActorStopped, namespace=dispatch_namespace)
        async def handle(msg: ActorStopped):
            actors_of_type = actors[msg.type_].remove(msg.id)
            return Singleton.active(
                context=context,
                actor_factory=actor_factory,
                actors=actors.set(msg.type_, actors_of_type),
            )

        async def recv(msg):
            return await handle(msg)

        return behaviour.receive(recv)
