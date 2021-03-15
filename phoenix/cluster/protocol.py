from typing import Any

from phoenix.actor import ActorId
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
class ClusterNodeShutdown:
    pass


class ServerNotRunning(Exception):
    pass


class SystemExists(Exception):
    pass
