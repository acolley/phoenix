import asyncio
import attr
from attr.validators import instance_of
import logging
from multipledispatch import dispatch
from phoenix import ActorRef, ActorSystem, Behaviour
import sys
from typing import Tuple


@attr.s
class Gauge:
    ref = attr.ib()

    @attr.s
    class State:
        n = attr.ib()

    @attr.s
    class Inc:
        pass

    @attr.s
    class Dec:
        pass

    @attr.s
    class Read:
        reply_to: ActorRef = attr.ib()

    @classmethod
    def start(cls, ctx) -> "Gauge":
        async def _start(ctx):
            return cls.State(0)
        return cls(ctx.spawn(_start, Gauge.handle))

    @staticmethod
    @dispatch(State, Inc)
    async def handle(state: State, msg: Inc) -> Tuple[Behaviour, State]:
        print(f"Increment: {state}")
        return Behaviour.stop, Gauge.State(state.n + 1)

    @staticmethod
    @dispatch(State, Dec)
    async def handle(state: State, msg: Dec) -> Tuple[Behaviour, State]:
        return Behaviour.stop, Gauge.State(state.n - 1)

    @staticmethod
    @dispatch(State, Read)
    async def handle(state: State, msg: Read) -> Tuple[Behaviour, State]:
        await msg.reply_to.cast(state.n)
        return Behaviour.stop, state

    async def inc(self):
        await self.ref.cast(self.Inc())
    
    async def dec(self):
        await self.ref.cast(self.Dec())
    
    async def read(self) -> int:
        return await self.ref.call(self.Read)


@attr.s
class Application:
    ref = attr.ib()

    @attr.s
    class State:
        ctx = attr.ib()
        gauge: Gauge = attr.ib(validator=instance_of(Gauge))
    
    @attr.s
    class IncGauge:
        pass

    @classmethod
    def start(cls, ctx) -> "Application":
        async def _start(ctx):
            gauge = Gauge.start(ctx)
            return cls.State(ctx=ctx, gauge=gauge)
        return cls(ctx.spawn(_start, Application.handle))
    
    @staticmethod
    @dispatch(State, IncGauge)
    async def handle(state: State, msg: IncGauge):
        await state.gauge.inc()
        return Behaviour.done, state
    
    @staticmethod
    @dispatch(State, Down)
    async def handle(state: State, msg: Down):
        gauge = Gauge.start(state.ctx)
        return Gauge.State(ctx=state.ctx, gauge=gauge)
    
    async def inc_gauge(self):
        await self.ref.cast(self.IncGauge())
    

async def main_async():
    system = ActorSystem()
    app = Application.start(system)
    await app.inc_gauge()
    await system.run()


def main():
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.DEBUG)
    asyncio.run(main_async())
