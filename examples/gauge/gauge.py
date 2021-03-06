import curio
import attr
from attr.validators import instance_of
import logging
from multipledispatch import dispatch
import sys
from typing import Tuple

from phoenix import (
    ActorId,
    ActorSystem,
    Behaviour,
    Down,
    RestartStrategy,
    Supervisor,
    retry,
)


@attr.s
class Gauge:
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    ctx = attr.ib()

    @attr.s
    class State:
        ctx = attr.ib()
        n = attr.ib()

    @attr.s
    class Inc:
        pass

    @attr.s
    class Dec:
        pass

    @attr.s
    class Read:
        reply_to: ActorId = attr.ib()

    @classmethod
    async def new(cls, ctx, name=None) -> "Gauge":
        actor_id = await ctx.spawn(cls.start, cls.handle, name=name)
        return cls(ctx=ctx, actor_id=actor_id)

    @staticmethod
    async def start(ctx) -> "Gauge":
        return Gauge.State(ctx, 0)

    @staticmethod
    @dispatch(State, Inc)
    async def handle(state: State, msg: Inc) -> Tuple[Behaviour, State]:
        raise ValueError
        state.n += 1
        return Behaviour.done, state

    @staticmethod
    @dispatch(State, Dec)
    async def handle(state: State, msg: Dec) -> Tuple[Behaviour, State]:
        state.n -= 1
        return Behaviour.done, state

    @staticmethod
    @dispatch(State, Read)
    async def handle(state: State, msg: Read) -> Tuple[Behaviour, State]:
        await state.ctx.cast(msg.reply_to, state.n)
        return Behaviour.done, state

    async def inc(self):
        await self.ctx.cast(self.actor_id, self.Inc())

    async def dec(self):
        await self.ctx.cast(self.actor_id, self.Dec())

    async def read(self) -> int:
        return await self.ctx.call(self.actor_id, self.Read)


async def main_async():
    system = ActorSystem()
    task = await curio.spawn(system.run())

    supervisor = await Supervisor.new(system, name="Supervisor.Gauge")
    await supervisor.init(
        children=[(Gauge.start, Gauge.handle, dict(name="Gauge"))],
        strategy=RestartStrategy.one_for_one,
    )
    gauge = Gauge(actor_id=ActorId("Gauge"), ctx=system)
    await gauge.inc()
    await gauge.dec()
    # Timeout should be based on the average time it takes
    # to process this specific request.
    value = await retry(max_retries=5)(lambda: curio.timeout_after(3, gauge.read()))
    print(value)

    await curio.sleep(5)
    await system.shutdown()


def main():
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.DEBUG)
    curio.run(main_async())
