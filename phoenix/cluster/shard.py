import aetcd3
from multimethod import multimethod
from typing import Any, Callable, List, Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context, ExitReason
from phoenix.dataclasses import dataclass
from phoenix.dynamic_supervisor import ChildSpec, DynamicSupervisor

"""
{
    "system1": {"entity1", "entity2"},
    "system2": {"entity3", "entity4"},
}
SystemRemoved("system2")
{
    "system1": {"entity1", "entity2", "entity3", "entity4"},
}
SystemJoined("system2")
{
    "system1": {"entity1", "entity2"},
    "system2": {"entity3", "entity4"},
}
SystemJoined("system3")
{
    "system1": {"entity1", "entity2"},
    "system2": {"entity3"},
    "system3": {"entity4"},
}
"""

"""
/shards/ActorSystem.ClusterShard@system1
/shards/ActorSystem.ClusterShard@system2
/shards/ActorSystem.ClusterShard@system3
"""


@dataclass
class State:
    context: Context
    shard_count: int
    etcd: aetcd3.Etcd3Client
    spec_for: Callable[[Any], ChildSpec]
    supervisor: DynamicSupervisor
    shards: List[ActorId]
    entities: Dict[Any, ActorId]


@dataclass
class Envelope:
    entity_id: Any
    msg: Any


async def start(
    context: Context,
    shard_count: int,
    etcd: Tuple[str, int],
    spec_for: Callable[[Any], ChildSpec],
) -> Actor:
    etcd = aetcd3.client(host=etcd[0], port=etcd[1])

    supervisor = DynamicSupervisor.new()

    # TODO: get shard nodes
    # TODO: watch for shard changes
    # TODO: rebalance on shard change

    return Actor(
        state=State(
            context=context,
            shard_count=shard_count,
            etcd=etcd,
            spec_for=spec_for,
            supervisor=supervisor,
            shards=[],
            entities={},
        )
    )


@multimethod
async def handle(state: State, msg: Envelope) -> Tuple[Behaviour, State]:
    index = hash(msg.entity_id) % state.shard_count
    shard = state.shards[index]
    if state.context.actor_id.system_id == shard.system_id:
        try:
            entity = state.entities[msg.entity_id]
        except KeyError:
            spec = state.spec_for(msg.entity_id)
            entity = await state.supervisor.start_child(spec)
            state.entities[msg.entity_id] = entity
        await context.cast(entity, msg.msg)
    else:
        pass
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: ExitReason):
    await state.etcd.close()


@dataclass
class ClusterShard:
    pass
