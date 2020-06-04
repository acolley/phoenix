import abc
import asyncio
from asyncio import Queue, Task
import attr
from functools import partial
import logging
from multipledispatch import dispatch
from pyrsistent import PRecord, field
import random
import traceback
from typing import Any, Callable, Optional, Union

from phoenix import behaviour, routers
from phoenix.actor import Ref
from phoenix.behaviour import Behaviour
from phoenix.system import system


class Greeter:
    def __init__(self, greeting: str):
        self.greeting = greeting
        self.count = 0

    def __call__(self) -> Behaviour:
        async def f(message):
            self.count += 1
            print(f"{self.greeting} {message}")
            if self.count >= 5:
                raise Exception("Oh noooooo!!")
            await asyncio.sleep(random.uniform(0, 10))
            return behaviour.same()
        return behaviour.restart(behaviour.receive(f))


class Ping:
    def __init__(self):
        self.pong = None
    
    def __call__(self) -> Behaviour:
        async def f(pong: Ref) -> Behaviour:
            self.pong = pong
            await self.pong.tell("ping")
            return self.ping()

        return behaviour.receive(f)
    
    def ping(self) -> Behaviour:
        async def f(message: str) -> Behaviour:
            print(message)
            await self.pong.tell("ping")
            await asyncio.sleep(random.uniform(0, 10))
            return behaviour.same()

        return behaviour.receive(f)


class Pong:
    def __init__(self):
        self.ping = None
    
    def __call__(self) -> Behaviour:
        async def f(ping: Ref) -> Behaviour:
            self.ping = ping
            return self.pong()

        return behaviour.receive(f)
    
    def pong(self) -> Behaviour:
        async def f(message: str) -> Behaviour:
            print(message)
            await self.ping.tell("pong")
            await asyncio.sleep(random.uniform(0, 10))
            return behaviour.same()

        return behaviour.receive(f)


class EchoMsg:

    def __init__(self, reply_to, message):
        self.reply_to = reply_to
        self.message = message


class PingPong:

    def __init__(self):
        self.ping = None
        self.pong = None
    
    def __call__(self) -> Behaviour:
        async def f(spawn):
            self.greeter = await spawn(lambda: Greeter("Hello"))
            for i in range(100):
                await self.greeter.tell(str(i))
            self.ping = await spawn(Ping)
            self.pong = await spawn(Pong)
            await self.ping.tell(self.pong)
            await self.pong.tell(self.ping)

            def worker() -> Behaviour:
                async def f(message):
                    await message.reply_to.tell(message.message)
                    return behaviour.same()
                return behaviour.receive(f)
            
            router = routers.pool(5)(worker)
            echo = await spawn(router)
            replies = await asyncio.gather(
                echo.ask(partial(EchoMsg, message="Echooooo")),
                echo.ask(partial(EchoMsg, message="Meeeeeee")),
            )
            print(replies)

            return behaviour.ignore()

        return behaviour.setup(f)


def main():
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(system(PingPong), debug=True)
