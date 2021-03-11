from abc import ABC, abstractmethod
import asyncio
from asyncio import Queue
from collections import defaultdict
from enum import Enum
from functools import partial
import logging
from multimethod import multimethod
import threading
import uuid
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, Union

from phoenix import receiver
from phoenix.actor import Actor, ActorId, Behaviour, Context, ExitReason
from phoenix.cluster.protocol import (
    ClusterNodeShutdown,
    Join,
    Leave,
    Rejected,
    RemoteActorMessage,
)
from phoenix.connection import Connection
from phoenix.dataclasses import dataclass
from phoenix.system.protocol import ClusterDisconnected, NoSuchActor

logger = logging.getLogger(__name__)


@dataclass
class State:
    context: Context
    conn: Connection


@dataclass
class Shutdown:
    reply_to: ActorId


@dataclass
class Disconnected:
    pass


class RejectedError(Exception):
    pass


async def start(context: Context, host: str, port: int) -> Actor:
    reader, writer = await asyncio.open_connection(host=host, port=port)
    conn = Connection(reader=reader, writer=writer)

    await conn.send(Join(context.actor_id.system_id))
    resp = await conn.recv()
    if isinstance(resp, Rejected):
        raise RejectedError

    recv = await context.spawn(
        partial(receiver.start, conn=conn, listener=context.actor_id),
        name=f"{context.actor_id.value}.Receiver",
    )
    await context.link(context.actor_id, recv)
    await context.cast(recv, receiver.Receive())

    # async def recv():
    #     while True:
    #         msg = await conn.recv()
    #         await context.cast(context.actor_id, msg)

    # task = asyncio.create_task(recv())

    return Actor(state=State(context=context, conn=conn), handler=handle)


@multimethod
async def handle(state: State, msg: Shutdown) -> Tuple[Behaviour, State]:
    await state.conn.send(Leave())
    await state.context.cast(msg.reply_to, None)
    return Behaviour.stop, state


@multimethod
async def handle(state: State, msg: ClusterNodeShutdown) -> Tuple[Behaviour, State]:
    return Behaviour.stop, state


@multimethod
async def handle(state: State, msg: RemoteActorMessage) -> Tuple[Behaviour, State]:
    if msg.actor_id.system_id == state.context.actor_id.system_id:
        try:
            await state.context.cast(actor_id=msg.actor_id, msg=msg.msg)
        except NoSuchActor:
            logger.debug(str(msg), exc_info=True)
    else:
        await state.conn.send(msg)
    return Behaviour.done, state


@dataclass
class ClusterConnection:
    actor_id: ActorId
    context: Context

    async def send(self, actor_id: ActorId, msg: Any):
        await self.context.cast(
            self.actor_id, RemoteActorMessage(actor_id=actor_id, msg=msg)
        )

    async def shutdown(self):
        await self.context.call(self.actor_id, Shutdown)
