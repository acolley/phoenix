import asyncio
import attr
import logging
from multipledispatch import dispatch
from phoenix import ActorRef, ActorSystem, Behaviour
import sys
from typing import Tuple


@attr.s
class State:
    n = attr.ib()

class Inc:
    pass

class Dec:
    pass

@attr.s
class Read:
    reply_to: ActorRef = attr.ib()


@attr.s
class Gauge:
    ref = attr.ib()

    @classmethod
    def start(cls, ctx) -> "Gauge":
        async def _start(ctx):
            return State(0)
        return cls(ctx.spawn(_start, Gauge.handle))

    @staticmethod
    @dispatch(State, Inc)
    async def handle(state: State, msg: Inc) -> Tuple[Behaviour, State]:
        print(f"Increment: {state}")
        return Behaviour.stop, State(state.n + 1)

    @staticmethod
    @dispatch(State, Dec)
    async def handle(state: State, msg: Dec) -> Tuple[Behaviour, State]:
        return Behaviour.stop, State(state.n - 1)

    @staticmethod
    @dispatch(State, Read)
    async def handle(state: State, msg: Read) -> Tuple[Behaviour, State]:
        await msg.reply_to.cast(state.n)
        return Behaviour.stop, state

    async def inc(self):
        await self.ref.cast(Inc())
    
    async def dec(self):
        await self.ref.cast(Dec())
    
    async def read(self) -> int:
        return await self.ref.call(Read)


@attr.s
class Application:
    ref = attr.ib()

    @classmethod
    def start(cls, ctx) -> "Application":
        async def _start(ctx):
            gauge = Gauge.start(ctx)
            await gauge.inc()
            return None
        return cls(ctx.spawn(_start, Application.handle))
    
    @staticmethod
    def handle(state: None, msg):
        return Behaviour.stop, state
    

async def main_async():
    system = ActorSystem()
    Application.start(system)
    await system.run()


def main():
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.DEBUG)
    asyncio.run(main_async())
