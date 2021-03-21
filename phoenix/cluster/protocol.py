from functools import partial
from typing import Any

from phoenix.actor import ActorId, Context
from phoenix.dataclasses import dataclass


@dataclass
class Join:
    name: str


@dataclass
class Accepted:
    pass


@dataclass
class Rejected:
    pass


@dataclass
class Leave:
    pass


@dataclass(frozen=True)
class RemoteActorMessage:
    actor_id: ActorId
    msg: Any


@dataclass
class Send:
    msg: RemoteActorMessage


@dataclass
class WatchRemoteActor:
    reply_to: ActorId
    watcher: ActorId
    watched: ActorId


@dataclass
class ClusterNodeShutdown:
    pass


class ServerNotRunning(Exception):
    pass


class SystemExists(Exception):
    pass


@dataclass
class ClusterNode:
    actor_id: ActorId
    context: Context

    async def send(self, actor_id: ActorId, msg: Any):
        await self.context.cast(
            self.actor_id, Send(RemoteActorMessage(actor_id=actor_id, msg=msg))
        )

    async def watch(self, watcher: ActorId, watched: ActorId):
        return await self.context.call(
            self.actor_id, partial(WatchRemoteActor, watcher=watcher, watched=watched)
        )
