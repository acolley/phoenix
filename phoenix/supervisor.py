import attr
from attr.validators import instance_of
from enum import Enum
import logging
from multipledispatch import dispatch

from phoenix import ActorId, Behaviour, Down

logger = logging.getLogger(__name__)


class Strategy(Enum):
    one_for_one = 0


@attr.s
class Supervisor:
    ctx = attr.ib()
    id: ActorId = attr.ib(validator=instance_of(ActorId))

    @attr.s
    class Uninitialised:
        ctx = attr.ib()

    @attr.s
    class Supervising:
        ctx = attr.ib()
        factories = attr.ib()
        children = attr.ib()
        strategy = attr.ib()

    @attr.s
    class Init:
        children = attr.ib()
        strategy = attr.ib()

    @staticmethod
    async def start(ctx):
        async def _start(ctx):
            return Supervisor.Uninitialised(ctx=ctx)

        return await ctx.spawn(_start, Supervisor.handle)

    @staticmethod
    @dispatch(Uninitialised, Init)
    async def handle(state: Uninitialised, msg: Init) -> Supervising:
        children = []
        for start, handle in msg.children:
            child = await state.ctx.spawn(start, handle)
            state.ctx.watch(child)
            children.append(child)
        return Behaviour.done, Supervisor.Supervising(
            ctx=state.ctx,
            factories=msg.children,
            children=children,
            strategy=msg.strategy,
        )

    @staticmethod
    @dispatch(Supervising, Down)
    async def handle(state: Supervising, msg: Down) -> Supervising:
        index = state.children.index(msg.id)
        if state.strategy == Strategy.one_for_one:
            start, handle = state.factories[index]
            logger.debug(
                "[%s] Supervisor restarting child: [%s]; Reason: [%s]",
                str(state.ctx.id),
                msg.id,
                str(msg.reason),
            )
            child = await state.ctx.spawn(start, handle)
            state.ctx.watch(child)
            state.children[index] = child
        else:
            raise ValueError(f"Unsupported Strategy: {state.strategy}")
        return Behaviour.done, state

    async def init(self, children, strategy):
        await self.ctx.cast(self.id, self.Init(children=children, strategy=strategy))
