from functools import partial
from multimethod import multimethod
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.dataclasses import dataclass


@dataclass
class State:
    context: Context
    store: Dict[Any, Any]


@dataclass
class Put:
    reply_to: ActorId
    key: Any
    value: Any


@dataclass
class Get:
    reply_to: ActorId
    key: Any


@dataclass
class Remove:
    reply_to: ActorId
    key: Any


@dataclass
class GetValues:
    reply_to: ActorId


class KeyExists(Exception):
    pass


async def start(context: Context) -> Actor:
    return Actor(state=State(context=context, store={}), handler=handle)


@multimethod
async def handle(state: State, msg: Put) -> Tuple[Behaviour, State]:
    if msg.key in state.store:
        await state.context.cast(msg.reply_to, KeyExists(msg.key))
        return Behaviour.done, state
    state.store[msg.key] = msg.value
    await state.context.cast(msg.reply_to, None)
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: Get) -> Tuple[Behaviour, State]:
    await state.context.cast(msg.reply_to, state.store.get(msg.key))
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: GetValues) -> Tuple[Behaviour, State]:
    values = list(state.store.values())
    await state.context.cast(msg.reply_to, values)
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: Remove) -> Tuple[Behaviour, State]:
    try:
        del state.store[msg.key]
    except KeyError as e:
        await state.context.cast(msg.reply_to, e)
    else:
        await state.context.cast(msg.reply_to, None)
    return Behaviour.done, state


K = TypeVar("K")
V = TypeVar("V")


@dataclass
class Registry(Generic[K, V]):
    actor_id: ActorId
    context: Context

    async def put(self, key: K, value: V):
        resp = await self.context.call(
            self.actor_id, partial(Put, key=key, value=value)
        )
        if isinstance(resp, Exception):
            raise resp

    async def get(self, key: K) -> Optional[V]:
        return await self.context.call(self.actor_id, partial(Get, key=key))

    async def values(self) -> List[V]:
        return await self.context.call(self.actor_id, GetValues)

    async def remove(self, key: K):
        resp = await self.context.call(self.actor_id, partial(Remove, key=key))
        if isinstance(resp, Exception):
            raise resp
