import asyncio
from functools import partial
import pytest
from typing import Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.connection import Connection
from phoenix.dataclasses import dataclass
from phoenix.cluster import node
from phoenix.cluster.protocol import (
    Accepted,
    ClusterNodeShutdown,
    Join,
    RemoteActorMessage,
)
from phoenix.retry import retry
from phoenix.system.system import ActorSystem


@dataclass
class Echo:
    msg: str


@pytest.mark.asyncio
async def test_node(event_loop, unused_tcp_port: int):
    cluster = ActorSystem("cluster")
    await cluster.start()
    node_id = await cluster.spawn(
        partial(node.start, host="localhost", port=unused_tcp_port),
        name="ClusterNode",
    )

    system1_conn = Connection(
        *(
            await retry()(
                lambda: asyncio.wait_for(
                    asyncio.open_connection(host="localhost", port=unused_tcp_port),
                    timeout=2,
                )
            )
        )
    )
    system2_conn = Connection(
        *(
            await retry()(
                lambda: asyncio.wait_for(
                    asyncio.open_connection(host="localhost", port=unused_tcp_port),
                    timeout=2,
                )
            )
        )
    )

    await system1_conn.send(Join("system1"))
    reply = await system1_conn.recv()
    assert reply == Accepted()

    await system2_conn.send(Join("system2"))
    reply = await system2_conn.recv()
    assert reply == Accepted()

    await system1_conn.send(
        RemoteActorMessage(
            actor_id=ActorId("system2", "test"), msg=Echo(msg="hello there")
        )
    )

    reply = await system2_conn.recv()
    assert reply == RemoteActorMessage(
        actor_id=ActorId("system2", "test"), msg=Echo(msg="hello there")
    )

    await cluster.call(node_id, node.Shutdown)

    reply = await system1_conn.recv()
    assert reply == ClusterNodeShutdown()
    reply = await system2_conn.recv()
    assert reply == ClusterNodeShutdown()

    await cluster.shutdown()


@dataclass
class Message:
    reply_to: ActorId
    msg: str


@pytest.mark.asyncio
async def test_call(unused_tcp_port: int):
    cluster = ActorSystem("cluster")
    await cluster.start()
    node_id = await cluster.spawn(
        partial(node.start, host="localhost", port=unused_tcp_port),
        name="ClusterNode",
    )

    class Pong:
        @staticmethod
        async def start(context: Context) -> Actor:
            return Actor(state=context, handler=Pong.handle)

        @staticmethod
        async def handle(state: Context, msg: Message) -> Tuple[Behaviour, Context]:
            assert msg.msg == "what ho!"
            await state.cast(msg.reply_to, msg.msg)
            return Behaviour.done, state

    system1 = ActorSystem("system1", ("localhost", unused_tcp_port))
    await system1.start()
    receiver = await system1.spawn(Pong.start, name="Receiver")

    received = asyncio.Event()

    class Ping:
        @staticmethod
        async def start(context: Context, remote: ActorId) -> Actor:
            msg = await retry(max_retries=3)(
                lambda: asyncio.wait_for(
                    context.call(remote, partial(Message, msg="what ho!")), timeout=5
                )
            )
            assert msg == "what ho!"
            received.set()
            return Actor(state=context, handler=Ping.handle)

        @staticmethod
        async def handle(state: Context, msg: None) -> Tuple[Behaviour, Context]:
            return Behaviour.done, state

    system2 = ActorSystem("system2", ("localhost", unused_tcp_port))
    await system2.start()
    await system2.spawn(partial(Ping.start, remote=receiver), name="Ping")

    await received.wait()

    await cluster.call(node_id, node.Shutdown)

    await system1.shutdown()
    await system2.shutdown()

    await cluster.shutdown()
