import abc
import asyncio
from asyncio import Queue, Task
import attr
from attr.validators import instance_of
from datetime import timedelta
from functools import partial
import logging
from multipledispatch import dispatch
from pyrsistent import PRecord, field
import random
import traceback
from typing import Any, Callable, Optional, Union

from phoenix import behaviour, routers
from phoenix.behaviour import Behaviour
from phoenix.ref import Ref
from phoenix.system.system import system


class Greeter:
    class Timeout:
        pass

    @staticmethod
    def start(greeting: str) -> Behaviour:
        return Greeter.init(greeting, 0)

    @staticmethod
    def init(greeting: str, count: int) -> Behaviour:
        async def f(timers):
            await timers.start_fixed_delay_timer(Greeter.Timeout, timedelta(seconds=1))
            return Greeter.active(greeting, count)

        return behaviour.restart(behaviour.schedule(f)).with_backoff(
            lambda n: min(2 ** n, 10)
        )

    @staticmethod
    def active(greeting: str, count: int) -> Behaviour:
        async def f(message: Greeter.Timeout):
            print(f"{greeting} {count}")
            if count > 1:
                raise Exception("Boooooom!!!")
            return Greeter.active(greeting, count + 1)

        return behaviour.receive(f)


class Ping:
    @staticmethod
    def start() -> Behaviour:
        async def f(context):
            return Ping.wait_for_pong(context)

        return behaviour.setup(f)

    @staticmethod
    def wait_for_pong(context) -> Behaviour:
        async def f(pong: Ref) -> Behaviour:
            await pong.tell(context.ref)
            return Ping.ping(pong)

        return behaviour.receive(f)

    @staticmethod
    def ping(pong: Ref) -> Behaviour:
        async def f(message: str) -> Behaviour:
            print(message)
            await pong.tell("ping")
            await asyncio.sleep(1)
            return behaviour.same()

        return behaviour.receive(f)


class Pong:
    @staticmethod
    def start() -> Behaviour:
        async def f(ping: Ref) -> Behaviour:
            await ping.tell("pong")
            return Pong.pong(ping)

        return behaviour.receive(f)

    @staticmethod
    def pong(ping: Ref) -> Behaviour:
        async def f(message: str) -> Behaviour:
            print(message)
            await ping.tell("pong")
            await asyncio.sleep(1)
            return behaviour.same()

        return behaviour.receive(f)


@attr.s
class EchoMsg:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))
    message: str = attr.ib(validator=instance_of(str))


class PingPong:
    @attr.s
    class GreeterStopped:
        pass

    @staticmethod
    def start() -> Behaviour:
        async def f(context):
            greeter = await context.spawn(Greeter.start("Hello"), "Greeter")
            await context.watch(greeter, PingPong.GreeterStopped())
            ping = await context.spawn(Ping.start(), "Ping")
            pong = await context.spawn(Pong.start(), "Pong")
            await ping.tell(pong)

            def worker() -> Behaviour:
                async def f(message):
                    await message.reply_to.tell(message.message)
                    return behaviour.same()

                return behaviour.receive(f)

            router = routers.pool(5)(worker())
            echo = await context.spawn(router, "Router")
            replies = await asyncio.gather(
                echo.ask(partial(EchoMsg, message="Echooooo")),
                echo.ask(partial(EchoMsg, message="Meeeeeee")),
            )
            print(replies)

            await context.stop(echo)

            return PingPong.active(context)

        return behaviour.restart(behaviour.setup(f))

    @staticmethod
    def active(context) -> Behaviour:
        async def f(msg):
            logging.info("Creating a new greeter")
            await context.spawn(Greeter.start("Hello 2"), "Greeter")
            return behaviour.same()

        return behaviour.receive(f)


def main():
    # logging.basicConfig(level=logging.DEBUG)
    asyncio.run(system(PingPong.start()), debug=True)
