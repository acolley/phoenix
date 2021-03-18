import asyncio
from functools import partial
import logging
from typing import Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.cluster.etcd import node
from phoenix.cluster.etcd.node import Send
from phoenix.cluster.protocol import RemoteActorMessage
from phoenix.dataclasses import dataclass
from phoenix.system.system import ActorSystem, ClusterConfig


@dataclass
class Message:
    reply_to: ActorId
    msg: str


async def echo_start(context: Context) -> Actor:
    return Actor(state=context, handler=echo_handle)


async def echo_handle(state: Context, msg: Message) -> Tuple[Behaviour, Context]:
    print(msg.msg)
    await asyncio.sleep(3)
    await state.cast(msg.reply_to, Message(reply_to=state.actor_id, msg=msg.msg))
    return Behaviour.done, state


async def async_main():
    system1 = ActorSystem(
        "system1", ClusterConfig(server=("localhost", 8080), remote=("localhost", 2379))
    )
    await system1.start()

    echo_system1 = await system1.spawn(echo_start, name="Echo")

    system2 = ActorSystem(
        "system2", ClusterConfig(server=("localhost", 8081), remote=("localhost", 2379))
    )
    await system2.start()

    echo_system2 = await system2.spawn(echo_start, name="Echo")

    await asyncio.sleep(2)

    await system1.cast(echo_system2, Message(reply_to=echo_system1, msg="Hello There!"))

    await asyncio.gather(system1.run_forever(), system2.run_forever())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("hpack").setLevel(logging.WARNING)
    asyncio.run(async_main())
