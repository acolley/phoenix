from enum import Enum
from functools import partial
import logging
from multimethod import multimethod
from typing import List, Tuple

from phoenix.actor import (
    Actor,
    ActorId,
    ActorFactory,
    ActorSpawnOptions,
    ActorStart,
    Behaviour,
    Context,
    Down,
    ExitReason,
    Shutdown,
    Stop,
)
from phoenix.connection import Connection
from phoenix.dataclasses import dataclass

logger = logging.getLogger(__name__)


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


@dataclass
class ChildInfo:
    actor_id: ActorId


@dataclass
class Uninitialised:
    context: Context


@dataclass
class Supervising:
    context: Context
    specs: List[ChildSpec]
    children: List[ActorId]
    restarts: List[int]
    strategy: RestartStrategy


@dataclass
class Init:
    """
    Synchronously initialise the Supervisor
    enabling clients to wait until children
    have all been spawned.
    """

    reply_to: ActorId
    children: List[ChildSpec]
    strategy: RestartStrategy


@dataclass
class WhichChildren:
    reply_to: ActorId


@dataclass
class Restart:
    """
    Internal message indicating that an actor
    should be restarted.
    """

    actor_id: ActorId
    reason: ExitReason


async def start(context: Context) -> Actor:
    return Actor(state=Uninitialised(context=context), handler=handle)


@multimethod
async def handle(state: Uninitialised, msg: Init) -> Tuple[Behaviour, Supervising]:
    children = []
    restarts = []
    for spec in msg.children:
        child = await state.context.spawn(spec.start, **spec.options)
        await state.context.watch(child)
        children.append(child)
        restarts.append(0)
    await state.context.cast(msg.reply_to, None)
    return (
        Behaviour.done,
        Supervising(
            context=state.context,
            specs=msg.children,
            children=children,
            restarts=restarts,
            strategy=msg.strategy,
        ),
    )


@multimethod
async def handle(state: Uninitialised, msg: WhichChildren) -> Tuple[Behaviour, Uninitialised]:
    await state.context.cast(msg.reply_to, [])
    return Behaviour.done, state


@multimethod
async def handle(state: Supervising, msg: WhichChildren) -> Tuple[Behaviour, Supervising]:
    children = [ChildInfo(actor_id=actor_id) for actor_id in state.children]
    await state.context.cast(msg.reply_to, children)
    return Behaviour.done, state


@multimethod
async def handle(state: Supervising, msg: Down) -> Supervising:
    index = state.children.index(msg.actor_id)
    restart_when = state.specs[index].restart_when
    if restart_when == RestartWhen.permanent or (
        restart_when == RestartWhen.transient and msg.reason not in [Shutdown(), Stop()]
    ):
        if state.strategy == RestartStrategy.one_for_one:
            backoff = 2 ** state.restarts[index]
            logger.debug(
                "[%s] Supervisor Child Down: [%s]. Reason: [%s]. Restarting in %f seconds.",
                str(state.context.actor_id),
                msg.actor_id,
                str(msg.reason),
                backoff,
            )
            await state.context.cast_after(
                state.context.actor_id,
                Restart(actor_id=msg.actor_id, reason=msg.reason),
                backoff,
            )
        else:
            raise ValueError(f"Unsupported RestartStrategy: {state.strategy}")
    else:
        state.children[index] = None
        logger.debug(
            "[%s] Supervisor Child Down: [%s]. Reason: [%s]. Not restarting.",
            str(state.context.actor_id),
            msg.actor_id,
            str(msg.reason),
        )
    return Behaviour.done, state


@multimethod
async def handle(state: Supervising, msg: Restart) -> Tuple[Behaviour, Supervising]:
    index = state.children.index(msg.actor_id)
    spec = state.specs[index]
    logger.debug(
        "[%s] Supervisor restarting child: [%s]",
        str(state.context.actor_id),
        msg.actor_id,
    )
    child = await state.context.spawn(spec.start, **spec.options)
    await state.context.watch(child)
    state.children[index] = child
    state.restarts[index] += 1
    return Behaviour.done, state


@dataclass
class Supervisor:
    actor_id: ActorId
    context: Context

    @classmethod
    async def new(cls, context, name=None) -> "Supervisor":
        actor_id = await context.spawn(
            start,
            name=name,
        )
        return Supervisor(actor_id=actor_id, context=context)

    async def init(self, children: List[ChildSpec], strategy: RestartStrategy):
        await self.context.call(
            self.actor_id, partial(Init, children=children, strategy=strategy)
        )

    async def which_children(self) -> List[ChildInfo]:
        return await self.context.call(self.actor_id, WhichChildren)
