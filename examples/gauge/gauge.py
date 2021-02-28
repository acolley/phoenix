import asyncio
import attr
from attr.validators import instance_of
import logging
from multipledispatch import dispatch
from phoenix import ActorId, ActorSystem, Behaviour, Down
import sys
from typing import Tuple


@attr.s
class Gauge:
    ctx = attr.ib()
    id: ActorId = attr.ib(validator=instance_of(ActorId))

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
    def start(cls, ctx) -> "Gauge":
        async def _start(ctx):
            return cls.State(ctx, 0)
        return cls(ctx=ctx, id=ctx.spawn(_start, Gauge.handle, name="gauge"))

    @staticmethod
    @dispatch(State, Inc)
    async def handle(state: State, msg: Inc) -> Tuple[Behaviour, State]:
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
        await self.ctx.cast(self.id, self.Inc())
    
    async def dec(self):
        await self.ctx.cast(self.id, self.Dec())
    
    async def read(self) -> int:
        return await self.ctx.call(self.id, self.Read)


@attr.s
class Application:
    ctx = attr.ib()
    id: ActorId = attr.ib(validator=instance_of(ActorId))

    @attr.s
    class State:
        ctx = attr.ib()
        gauge: Gauge = attr.ib(validator=instance_of(Gauge))
    
    @attr.s
    class IncGauge:
        pass

    @attr.s
    class ReadGauge:
        reply_to: ActorId = attr.ib(validator=instance_of(ActorId))

    @classmethod
    def start(cls, ctx) -> "Application":
        async def _start(ctx):
            gauge = Gauge.start(ctx)
            return cls.State(ctx=ctx, gauge=gauge)
        return cls(ctx=ctx, id=ctx.spawn(_start, Application.handle, name="application"))
    
    @staticmethod
    @dispatch(State, IncGauge)
    async def handle(state: State, msg: IncGauge):
        await state.gauge.inc()
        return Behaviour.done, state
    
    @staticmethod
    @dispatch(State, ReadGauge)
    async def handle(state: State, msg: ReadGauge):
        resp = await state.gauge.read()
        await state.ctx.cast(msg.reply_to, resp)
        return Behaviour.done, state
    
    @staticmethod
    @dispatch(State, Down)
    async def handle(state: State, msg: Down):
        gauge = Gauge.start(state.ctx)
        return Behaviour.done, Gauge.State(ctx=state.ctx, gauge=gauge)
    
    async def inc_gauge(self):
        await self.ctx.cast(self.id, self.IncGauge())
    
    async def read_gauge(self) -> int:
        return await self.ctx.call(self.id, self.ReadGauge)
    

async def main_async():
    system = ActorSystem()
    task = asyncio.create_task(system.run())
    app = Application.start(system)
    await app.inc_gauge()
    print(await app.read_gauge())
    await system.shutdown()


def main():
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.DEBUG)
    asyncio.run(main_async())
