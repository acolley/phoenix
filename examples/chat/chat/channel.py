import attr
from attr.validators import deep_iterable, instance_of
from datetime import datetime
from multipledispatch import dispatch
from phoenix import behaviour
from phoenix.behaviour import Behaviour
from phoenix.persistence import effect
from phoenix.ref import Ref
from pyrsistent import PVector, v
from typing import Iterable, Optional

from chat.encoding import decode, encode


@attr.s(frozen=True)
class Create:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class Delete:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class Say:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    at: datetime = attr.ib(validator=instance_of(datetime))
    by: str = attr.ib(validator=instance_of(str))
    text: str = attr.ib(validator=instance_of(str))


@attr.s(frozen=True)
class ListMessages:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))


@attr.s(frozen=True)
class Created:
    pass


@attr.s(frozen=True)
class Deleted:
    pass


@attr.s(frozen=True)
class MessageCreated:
    at: datetime = attr.ib(validator=instance_of(datetime))
    by: str = attr.ib(validator=instance_of(str))
    text: str = attr.ib(validator=instance_of(str))


@attr.s(frozen=True)
class Confirmation:
    pass


@attr.s(frozen=True)
class Message:
    at: datetime = attr.ib(validator=instance_of(datetime))
    by: str = attr.ib(validator=instance_of(str))
    text: str = attr.ib(validator=instance_of(str))


@attr.s(frozen=True)
class Messages:
    messages: Iterable[Message] = attr.ib()


@attr.s(frozen=True)
class State:
    messages: Iterable[Message] = attr.ib(
        validator=deep_iterable(
            member_validator=instance_of(Message),
            iterable_validator=instance_of(PVector),
        ),
        default=v(),
    )


class Channel:
    @staticmethod
    def start(id: str) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(type(None), Create, namespace=dispatch_namespace)
        async def handle_command(state: None, cmd: Create) -> effect.Effect:
            return effect.persist([Created()]).then_reply(
                reply_to=cmd.reply_to, msg=Confirmation()
            )

        @dispatch(State, Delete, namespace=dispatch_namespace)
        async def handle_command(state: State, cmd: Delete) -> effect.Effect:
            return effect.persist([Deleted()]).then_reply(
                reply_to=cmd.reply_to, msg=Confirmation()
            )

        @dispatch(State, Say, namespace=dispatch_namespace)
        async def handle_command(state: State, cmd: Say) -> effect.Effect:
            return effect.persist(
                [MessageCreated(at=cmd.at, by=cmd.by, text=cmd.text)]
            ).then_reply(reply_to=cmd.reply_to, msg=Confirmation())

        @dispatch(State, ListMessages, namespace=dispatch_namespace)
        async def handle_command(state: State, cmd: ListMessages) -> effect.Effect:
            return effect.reply(reply_to=cmd.reply_to, msg=Messages(state.messages))

        @dispatch(type(None), Created, namespace=dispatch_namespace)
        async def handle_event(state: None, evt: Created) -> Optional[State]:
            return State()

        @dispatch(State, Deleted, namespace=dispatch_namespace)
        async def handle_event(state: State, evt: Deleted) -> Optional[State]:
            return None

        @dispatch(State, MessageCreated, namespace=dispatch_namespace)
        async def handle_event(state: State, evt: MessageCreated) -> Optional[State]:
            return attr.evolve(
                state,
                messages=state.messages.append(
                    Message(at=evt.at, by=evt.by, text=evt.text)
                ),
            )

        return behaviour.persist(
            id=id,
            empty_state=None,
            command_handler=handle_command,
            event_handler=handle_event,
            encode=encode,
            decode=decode,
        ).with_retention(number_of_events=5, keep_n_snapshots=1)
