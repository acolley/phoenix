import asyncio
from asyncio import Queue
from asyncio.base_events import Server
from enum import Enum
from functools import partial
import logging
from multimethod import multimethod
import pickle
import struct
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

from phoenix import registry
from phoenix.actor import Actor, ActorId, Behaviour, Context, Down, ExitReason
from phoenix.cluster import remote_actor_system
from phoenix.cluster.protocol import (
    Accepted,
    ClusterNodeShutdown,
    Join,
    Leave,
    Rejected,
    RemoteActorMessage,
    SystemExists,
)
from phoenix.connection import Connection
from phoenix.dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class State:
    context: Context
    systems: List[ActorId]
    system_registry: registry.Registry[str, ActorId]
    server: Server


@dataclass
class Joining:
    """
    Joining the cluster.
    """

    context: Context
    systems: List[ActorId]
    system_registry: registry.Registry[str, ActorId]
    server: Server


@dataclass
class Leader:
    """
    Node is acting as the cluster Leader.

    The cluster Leader is responsible for node lifecycle
    management.
    """

    context: Context
    systems: List[ActorId]
    system_registry: registry.Registry[str, ActorId]
    server: Server


@dataclass
class Follower:
    """
    Node is acting as a cluster Follower.

    Cluster Followers can route messages to and from
    registered actor systems from and to
    """

    context: Context
    systems: List[ActorId]
    system_registry: registry.Registry[str, ActorId]
    server: Server


@dataclass
class Connected:
    conn: Connection


@dataclass
class Shutdown:
    reply_to: ActorId


async def start(context: Context, host: str, port: int) -> Actor:
    system_registry = await context.spawn(
        registry.start, name="Registry.RemoteActorSystems"
    )
    await context.link(context.actor_id, system_registry)

    async def on_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        host = writer.get_extra_info("peername")[0]
        port = writer.get_extra_info("peername")[1]
        logger.debug("RemoteActorSystem connected from %s:%s", host, port)
        conn = Connection(reader=reader, writer=writer)
        await context.cast(context.actor_id, Connected(conn=conn))

    server = await asyncio.start_server(on_connected, host=host, port=port)
    logger.debug("Cluster Node listening at %s:%s", host, port)

    return Actor(
        state=State(
            context=context,
            host=host,
            port=port,
            systems=[],
            system_registry=registry.Registry(
                actor_id=system_registry, context=context
            ),
            server=server,
        ),
        handler=handle,
        on_exit=handle,
    )


@multimethod
async def handle(state: State, msg: Connected) -> Tuple[Behaviour, State]:
    system = await state.context.spawn(
        partial(
            remote_actor_system.start,
            conn=msg.conn,
            systems=state.system_registry.actor_id,
        ),
        name=f"RemoteActorSystem.{uuid.uuid1()}",
    )
    await state.context.watch(system)
    state.systems.append(system)
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: Down) -> Tuple[Behaviour, State]:
    state.systems.remove(msg.actor_id)
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: ExitReason):
    state.server.close()
    await state.server.wait_closed()


@multimethod
async def handle(state: State, msg: Shutdown) -> Tuple[Behaviour, State]:
    for system in state.systems:
        logger.debug("Shutdown %s", system)
        await state.context.call(system, remote_actor_system.Shutdown)
    await state.context.cast(msg.reply_to, None)
    return Behaviour.stop, state
