import aetcd3
from multimethod import multimethod
from typing import Any, Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context, ExitReason
from phoenix.dataclasses import dataclass


@dataclass
class State:
    context: Context
    events: Any
    cancel: Any
    notify: ActorId


@dataclass
class Watch:
    pass


async def start(
    context: Context, etcd: aetcd3.Etcd3Client, key: str, notify: ActorId
) -> Actor:
    events, cancel = await etcd.watch(key)
    return Actor(
        state=State(context=context, events=events, cancel=cancel, notify=notify),
        handler=handle,
        on_exit=handle,
    )


@multimethod
async def handle(state: State, msg: Watch) -> Tuple[Behaviour, State]:
    async for event in state.events:
        await state.context.cast(state.notify, event)
        await state.context.cast(state.context.actor_id, Watch())
        return Behaviour.done, state


@multimethod
async def handle(state: State, msg: ExitReason):
    await state.cancel()
