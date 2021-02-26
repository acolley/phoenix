import attr


class Strategy(Enum):
    one_for_one = 0


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


@dispatch(Uninitialised, Init)
async def handle(state: Uninitialised, msg: Init) -> Supervising:
    children = {}
    for child_start, child_handle in msg.children:
        child = state.ctx.spawn(child_start, child_handle)
        state.ctx.monitor(child)
    return Supervising(ctx=state.ctx, children=children, strategy=msg.strategy)


@dispatch(Supervising, Down)
async def handle(state: Supervising, msg: Down) -> Supervising:
    child = state.children.pop(msg.ref_id)
    if state.strategy == Strategy.one_for_one:
        raise NotImplementedError
    else:
        raise ValueError(f"Unsupported Strategy: {state.strategy}")


@attr.s
class Supervisor:
    ref = attr.ib()

    @staticmethod
    def start(ctx):
        async def _start(ctx):
            pass
        return 

    async def init(self, children, strategy):
        await self.ref.cast(Init(children=children, strategy=strategy))
