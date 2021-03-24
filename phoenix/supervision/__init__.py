from enum import Enum

from phoenix.actor import ActorStart, ActorSpawnOptions
from phoenix.dataclasses import dataclass


class RestartStrategy(Enum):
    one_for_one = 0


class RestartWhen(Enum):
    """
    Specifies what the Supervisor considers to
    be an abnormal termination.
    """

    permanent = 0
    """
    Always restarted.
    """
    temporary = 1
    """
    Never restarted.
    """
    transient = 2
    """
    Only restarted if termination was abnormal.
    An abnormal termination is when an Actor exits
    with an ExitReason other than Shutdown or Stop.
    """


@dataclass
class ChildSpec:
    start: ActorStart
    options: ActorSpawnOptions
    restart_when: RestartWhen
