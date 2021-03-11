from abc import ABC, abstractmethod
import asyncio
from asyncio import Queue
from collections import defaultdict
from enum import Enum
from functools import partial
import logging
from multimethod import multimethod
import threading
import uuid
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, Union

from phoenix.actor import Actor, ActorId, ActorSpawnOptions, ActorStart, Behaviour, Context, Down, ExitReason, Shutdown, Stop
from phoenix.cluster.protocol import (
    Accepted,
    ClusterNodeShutdown,
    Join,
    Leave,
    Rejected,
    RemoteActorMessage,
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
class State:
    context: Context
    specs: List[ChildSpec]
    children: List[ActorId]
    restarts: List[int]


@dataclass
class StartChild:
    reply_to: ActorId
    spec: ChildSpec


@dataclass
class Restart:
    """
    Internal message indicating that an actor
    should be restarted.
    """

    actor_id: ActorId
    reason: ExitReason


async def start(context: Context) -> Actor:
    return Actor(state=State(
        context=context, specs=[], children=[], restarts=[]
    ), handler=handle)


@multimethod
async def handle(state: State, msg: StartChild) -> Tuple[Behaviour, State]:
    child = await state.context.spawn(msg.spec.start, **msg.spec.options)
    await state.context.watch(child)
    state.specs.append(msg.spec)
    state.children.append(child)
    state.restarts.append(0)
    await state.context.cast(msg.reply_to, child)
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: Down) -> State:
    index = state.children.index(msg.actor_id)
    restart_when = state.specs[index].restart_when
    if restart_when == RestartWhen.permanent or (restart_when == RestartWhen.transient and msg.reason not in [Shutdown(), Stop()]):
        backoff = 2 ** state.restarts[index]
        await state.context.cast_after(
            state.context.actor_id,
            Restart(actor_id=msg.actor_id, reason=msg.reason),
            backoff,
        )
    else:
        logger.debug(
            "[%s] Supervisor Child Down: [%s]. Reason: [%s]. Not restarting.",
            str(state.context.actor_id),
            msg.actor_id,
            str(msg.reason),
        )
        del state.specs[index]
        del state.children[index]
        del state.restarts[index]
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: Restart) -> Tuple[Behaviour, State]:
    index = state.children.index(msg.actor_id)
    spec = state.specs[index]
    logger.debug(
        "[%s] Restarting child: [%s]; Reason: [%s]",
        str(state.context.actor_id),
        msg.actor_id,
        str(msg.reason),
    )
    child = await state.context.spawn(spec.start, **spec.options)
    await state.context.watch(child)
    state.children[index] = child
    state.restarts[index] += 1
    return Behaviour.done, state


@dataclass
class DynamicSupervisor:
    """
    Supervisor for children that are started dynamically.
    """

    actor_id: ActorId
    context: Context

    @classmethod
    async def new(cls, context: Context, name=None) -> "DynamicSupervisor":
        actor_id = await context.spawn(start, name=name)
        return DynamicSupervisor(actor_id=actor_id, context=context)

    async def start_child(
        self, spec: ChildSpec
    ) -> ActorId:
        return await self.context.call(
            self.actor_id,
            partial(StartChild, spec=spec),
        )
