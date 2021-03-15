import asyncio
import logging
from phoenix import ActorId, ActorSystem, Behaviour, Context
from phoenix.dataclasses import dataclass
import sys
from typing import Tuple


class Factorial:
    @dataclass
    class Calculate:
        reply_to: ActorId
        n: int

    @staticmethod
    async def start(context: Context) -> Context:
        return context

    @staticmethod
    async def handle(state: Context, msg: Calculate) -> Tuple[Behaviour, Context]:
        def fac(n: int) -> int:
            if n == 1:
                return n
            elif n < 1:
                raise ValueError(n)
            else:
                return n * fac(n - 1)

        result = fac(msg.n)
        await state.cast(msg.reply_to, result)
        return Behaviour.done, state


async def async_main():
    system = ActorSystem("factorial")
    await system.start()
    await system.connect(host=sys.argv[1], port=int(sys.argv[2]))
    await system.spawn(Factorial.start, Factorial.handle, name="Factorial")
    await system.run_forever()


def main():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    asyncio.run(async_main())
