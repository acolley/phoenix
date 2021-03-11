from phoenix.dataclasses import dataclass


@dataclass
class ClusterDisconnected:
    """
    Disconnected from a cluster node.
    """

    host: str
    port: int


class NoSuchActor(Exception):
    pass


class ActorExists(Exception):
    pass
