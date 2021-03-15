import asyncio
from functools import partial
import logging
from multimethod import multimethod
from typing import Optional, Tuple

from phoenix import receiver
from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.cluster.protocol import (
    Accepted,
    ClusterNodeShutdown,
    Join,
    Leave,
    Rejected,
    RemoteActorMessage,
)
from phoenix.connection import Connection
from phoenix.dataclasses import dataclass
from phoenix.registry import KeyExists, Registry
from phoenix.retry import retry
from phoenix.system.system import NoSuchActor

logger = logging.getLogger(__name__)


@dataclass
class Joining:
    context: Context
    conn: Connection
    systems: Registry[str, ActorId]


@dataclass
class Member:
    context: Context
    conn: Connection
    systems: Registry[str, ActorId]
    name: str


@dataclass
class Shutdown:
    reply_to: ActorId


class Disconnected(Exception):
    pass


class Leaving(Exception):
    pass


async def start(context: Context, conn: Connection, systems: ActorId) -> Actor:
    """
    Args:
        systems (ActorId): Registry for system names to ActorIds.
    """

    # # TODO: reject connection after timeout if no Join has been received

    recv = await context.spawn(
        partial(receiver.start, conn=conn, listener=context.actor_id),
        name=f"{context.actor_id.value}.Receiver",
    )
    await context.link(context.actor_id, recv)
    await context.cast(recv, receiver.Receive())

    return Actor(
        state=Joining(
            context=context,
            conn=conn,
            systems=Registry(actor_id=systems, context=context),
        ),
        handler=handle,
    )


@multimethod
async def handle(state: Joining, msg: Join) -> Tuple[Behaviour, Optional[Member]]:
    try:
        await retry(exc=NoSuchActor)(
            lambda: state.systems.put(msg.name, state.context.actor_id)
        )
    except KeyExists:
        await state.conn.send(Rejected())
        return Behaviour.stop, None
    else:
        await state.conn.send(Accepted())
        return Behaviour.done, Member(
            context=state.context,
            conn=state.conn,
            systems=state.systems,
            name=msg.name,
        )


@multimethod
async def handle(state: Member, msg: RemoteActorMessage) -> Tuple[Behaviour, Member]:
    if msg.actor_id.system_id == state.name:
        await state.conn.send(msg)
    else:
        system = await state.systems.get(msg.actor_id.system_id)
        if system is None:
            logger.warning("No such RemoteActorSystem: %s", msg.actor_id.system_id)
            return Behaviour.done, state
        await state.context.cast(system, msg)

    return Behaviour.done, state


@multimethod
async def handle(state: Member, msg: Shutdown) -> Tuple[Behaviour, Member]:
    await state.conn.send(ClusterNodeShutdown())
    await state.context.cast(msg.reply_to, None)
    return Behaviour.stop, state


@multimethod
async def handle(state: Member, msg: Leave) -> Tuple[Behaviour, Member]:
    return Behaviour.stop, state
