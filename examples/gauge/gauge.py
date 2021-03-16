import asyncio
import attr
from attr.validators import instance_of
import logging
from multimethod import multimethod
import sys
from typing import Tuple

from phoenix.actor import (
    Actor,
    ActorId,
    Behaviour,
    Context,
    Down,
)
from phoenix.dataclasses import dataclass
from phoenix.retry import retry
from phoenix.supervisor import ChildSpec, RestartStrategy, RestartWhen, Supervisor
from phoenix.system.system import ActorSystem


@dataclass
class State:
    ctx: Context
    n: int


@dataclass
class Inc:
    pass


@dataclass
class Dec:
    pass


@dataclass
class Read:
    reply_to: ActorId


async def start(ctx) -> Actor:
    return Actor(state=State(ctx, 0), handler=handle)


@multimethod
async def handle(state: State, msg: Inc) -> Tuple[Behaviour, State]:
    raise ValueError
    state.n += 1
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: Dec) -> Tuple[Behaviour, State]:
    state.n -= 1
    return Behaviour.done, state


@multimethod
async def handle(state: State, msg: Read) -> Tuple[Behaviour, State]:
    await state.ctx.cast(msg.reply_to, state.n)
    return Behaviour.done, state


@dataclass
class Gauge:
    actor_id: ActorId
    ctx: Context

    @classmethod
    async def new(cls, ctx, name=None) -> "Gauge":
        actor_id = await ctx.spawn(start, name=name)
        return cls(ctx=ctx, actor_id=actor_id)

    async def inc(self):
        await self.ctx.cast(self.actor_id, Inc())

    async def dec(self):
        await self.ctx.cast(self.actor_id, Dec())

    async def read(self) -> int:
        return await self.ctx.call(self.actor_id, Read)


async def main_async():
    system = ActorSystem("system")
    task = asyncio.create_task(system.run())

    supervisor = await Supervisor.new(
        system,
        name="Supervisor.Gauge",
    )
    await supervisor.init(
        children=[ChildSpec(start=start, options=dict(name="Gauge"), restart_when=RestartWhen.permanent)],
        strategy=RestartStrategy.one_for_one,
    )
    gauge = Gauge(actor_id=ActorId(system_id="system", value="Gauge"), ctx=system)
    await gauge.inc()
    await gauge.dec()
    # Timeout should be based on the average time it takes
    # to process this specific request.
    value = await retry(max_retries=5)(
        lambda: asyncio.wait_for(gauge.read(), timeout=3)
    )
    print(value)

    await asyncio.sleep(5)
    await system.shutdown()


def main():
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.DEBUG)
    asyncio.run(main_async())
