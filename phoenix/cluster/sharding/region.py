import aetcd3
from typing import Dict

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.dataclasses import dataclass


@dataclass
class Coordinating:
    """
    This region is coordinating the allocation
    of shards to regions.
    """

    context: Context
    etcd: aetcd3.Etcd3Client
    shards: Dict[int, ActorId]


@dataclass
class Following:
    """
    This region responds to shard allocation
    requests and forwards messages to other
    regions.
    """

    context: Context
    coordinator: ActorId
    shards: Dict[int, ActorId]


async def start(context: Context, etcd: Tuple[str, int]) -> Actor:
    etcd = aetcd3.client(host=etcd[0], port=etcd[1])
    return Actor(state=)
