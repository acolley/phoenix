import attr
from enum import Enum
from multipledispatch import dispatch


class Strategy(Enum):
    one_for_one = 0


@attr.s
class Supervisor:
    ref = attr.ib()

    @staticmethod
    def start(ctx):
        async def _start(ctx):
            pass
        return 

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
    @dispatch(Uninitialised, Init)
    async def handle(state: Uninitialised, msg: Init) -> Supervising:
        children = []
        for child_start, child_handle in msg.children:
            child = state.ctx.spawn(child_start, child_handle)
            state.ctx.watch(child)
            children.append(child)
        return Supervisor.Supervising(ctx=state.ctx, children=children, strategy=msg.strategy)

    @staticmethod
    @dispatch(Supervising, Down)
    async def handle(state: Supervising, msg: Down) -> Supervising:
        state.children.remove(msg.id)
        if state.strategy == Strategy.one_for_one:
            raise NotImplementedError
        else:
            raise ValueError(f"Unsupported Strategy: {state.strategy}")

    async def init(self, children, strategy):
        await self.ref.cast(Init(children=children, strategy=strategy))
