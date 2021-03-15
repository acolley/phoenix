"""
Receive messages from a Connection and send
them to another actor.
"""
import asyncio
from multimethod import multimethod
from typing import Any, Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context, ExitReason
from phoenix.connection import Connection
from phoenix.dataclasses import dataclass


@dataclass
class State:
    context: Context
    conn: Connection
    listener: ActorId


@dataclass
class Receive:
    pass


async def start(context: Context, conn: Connection, listener: ActorId) -> Actor:
    return Actor(state=State(context=context, conn=conn, listener=listener), handler=handle, on_exit=handle)


@multimethod
async def handle(state: State, msg: Receive) -> Tuple[Behaviour, State]:
    message = await state.conn.recv()
    await state.context.cast(state.listener, message)
    await state.context.cast(state.context.actor_id, Receive())
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: ExitReason):
    await state.conn.close()
