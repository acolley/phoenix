import attr
from attr.validators import instance_of
from enum import Enum
from functools import partial
import logging
from multipledispatch import dispatch
from typing import Callable, Tuple

from phoenix import ActorId, Behaviour, Down, Error, ExitReason, Shutdown, Stop

logger = logging.getLogger(__name__)


class RestartStrategy(Enum):
    one_for_one = 0


@attr.s
class Supervisor:
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    ctx = attr.ib()

    @attr.s
    class Uninitialised:
        ctx = attr.ib()

    @attr.s
    class Supervising:
        ctx = attr.ib()
        factories = attr.ib()
        children = attr.ib()
        restarts = attr.ib()
        strategy = attr.ib()

    @attr.s
    class Init:
        reply_to: ActorId = attr.ib(validator=instance_of(ActorId))
        children = attr.ib()
        strategy = attr.ib()

    @attr.s
    class Restart:
        """
        Internal message indicating that an actor
        should be restarted.
        """

        actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
        reason: ExitReason = attr.ib(validator=instance_of((Shutdown, Stop, Error)))

    @classmethod
    async def new(cls, ctx, name=None) -> "Supervisor":
        actor_id = await ctx.spawn(cls.start, cls.handle, name=name)
        return Supervisor(actor_id=actor_id, ctx=ctx)

    @staticmethod
    async def start(ctx):
        return Supervisor.Uninitialised(ctx=ctx)

    @staticmethod
    @dispatch(Uninitialised, Init)
    async def handle(state: Uninitialised, msg: Init) -> Supervising:
        children = []
        restarts = []
        for start, handle, kwargs in msg.children:
            child = await state.ctx.spawn(start, handle, **kwargs)
            state.ctx.watch(child)
            children.append(child)
            restarts.append(0)
        await state.ctx.cast(msg.reply_to, None)
        return Behaviour.done, Supervisor.Supervising(
            ctx=state.ctx,
            factories=msg.children,
            children=children,
            restarts=restarts,
            strategy=msg.strategy,
        )

    @staticmethod
    @dispatch(Supervising, Down)
    async def handle(state: Supervising, msg: Down) -> Supervising:
        index = state.children.index(msg.actor_id)
        if state.strategy == RestartStrategy.one_for_one:
            backoff = 2 ** state.restarts[index]
            await state.ctx.cast_after(
                state.ctx.actor_id,
                Supervisor.Restart(actor_id=msg.actor_id, reason=msg.reason),
                backoff,
            )
        else:
            raise ValueError(f"Unsupported RestartStrategy: {state.strategy}")
        return Behaviour.done, state

    @staticmethod
    @dispatch(Supervising, Restart)
    async def handle(state: Supervising, msg: Restart) -> Tuple[Behaviour, Supervising]:
        index = state.children.index(msg.actor_id)
        start, handle, kwargs = state.factories[index]
        logger.debug(
            "[%s] Supervisor restarting child: [%s]; Reason: [%s]",
            str(state.ctx.actor_id),
            msg.actor_id,
            str(msg.reason),
        )
        child = await state.ctx.spawn(start, handle, **kwargs)
        state.ctx.watch(child)
        state.children[index] = child
        state.restarts[index] += 1
        return Behaviour.done, state

    async def init(self, children, strategy):
        await self.ctx.call(
            self.actor_id, partial(self.Init, children=children, strategy=strategy)
        )
