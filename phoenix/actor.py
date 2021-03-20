from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from phoenix.dataclasses import dataclass


@dataclass(frozen=True)
class ActorId:
    system_id: str
    value: str

    def __str__(self) -> str:
        return f"{self.value}@{self.system_id}"


@dataclass(frozen=True)
class TimerId:
    value: str


class Behaviour(Enum):
    done = 0
    stop = 1


class Context(ABC):
    @abstractmethod
    async def cast(self, actor_id: ActorId, msg: Any):
        raise NotImplementedError

    @abstractmethod
    async def send_after(self, dest: ActorId, msg, delay: float) -> TimerId:
        raise NotImplementedError

    @abstractmethod
    async def call(self, actor_id: ActorId, msg: Any):
        raise NotImplementedError

    @abstractmethod
    async def spawn(self, start, name=None) -> ActorId:
        raise NotImplementedError

    @abstractmethod
    async def watch(self, actor_id: ActorId):
        raise NotImplementedError

    @abstractmethod
    async def link(self, a: ActorId, b: ActorId):
        raise NotImplementedError


ActorStart = Callable[[Context], Coroutine]
ActorHandler = Callable[[Any, Any], Coroutine]
ActorSpawnOptions = Dict[str, str]
ActorFactory = Tuple[ActorStart, ActorSpawnOptions]


@dataclass
class Shutdown:
    pass


@dataclass
class Stop:
    pass


@dataclass
class Error:
    exc: Exception


ExitReason = Union[Shutdown, Stop, Error]


M = TypeVar("M")
S = TypeVar("S")


@dataclass
class Actor(Generic[M, S]):
    state: S
    handler: Callable[[S, M], Coroutine]
    on_exit: Optional[Callable[[S, ExitReason], Coroutine]] = None


@dataclass
class Down:
    """
    Message sent to a watcher when the watched exits.
    """

    actor_id: ActorId
    reason: ExitReason
