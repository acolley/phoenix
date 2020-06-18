import abc
import asyncio
from asyncio import Queue, Task
import attr
from attr.validators import instance_of
import cattr
from datetime import timedelta
from functools import partial
import importlib
import logging
from multipledispatch import dispatch
from pathlib import Path
from pyrsistent import PRecord, field
import random
import traceback
from typing import Any, Callable, Optional, Union

from phoenix import behaviour, routers
from phoenix.behaviour import Behaviour
from phoenix.persistence import effect
from phoenix.ref import Ref
from phoenix.system.system import system


def encode(event: Any) -> (str, dict):
    klass = event.__class__
    topic_id = f"{klass.__module__}#{getattr(klass, '__qualname__', klass.__name__)}"
    return topic_id, cattr.unstructure(event)


def decode(topic_id: str, data: dict) -> Any:
    module_name, _, class_name = topic_id.partition("#")
    module = importlib.import_module(module_name)
    # TODO: support nested classes
    klass = getattr(module, class_name)
    return cattr.structure(data, klass)


@attr.s
class Count:
    n: int = attr.ib(validator=instance_of(int))


@attr.s
class Counted:
    n: int = attr.ib(validator=instance_of(int))


class Counter:
    @staticmethod
    def start(id="counter") -> Behaviour:
        async def command_handler(state: int, cmd: Count) -> effect.Effect:
            print(state)
            return effect.persist([Counted(n=cmd.n)])

        async def event_handler(state: int, evt: Counted) -> int:
            return state + evt.n

        return behaviour.persist(
            id=id,
            empty_state=0,
            command_handler=command_handler,
            event_handler=event_handler,
            encode=encode,
            decode=decode,
        )


class ReadSideProcessor:
    class Timeout:
        pass

    @attr.s
    class Processed:
        offset: int = attr.ib(validator=instance_of(int))

    @staticmethod
    def start(db: Path, read, journal) -> Behaviour:
        async def f(timers):
            try:
                offset = int(db.read_text())
            except FileNotFoundError:
                offset = None
            await timers.start_fixed_rate_timer(
                ReadSideProcessor.Timeout(), timedelta(60)
            )
            return ReadSideProcessor.active(db, read, journal, offset)

        return behaviour.schedule(f)

    @staticmethod
    def active(db: Path, read, journal, offset: Optional[int]) -> Behaviour:
        async def f(msg: ReadSideProcessor.Timeout):
            (events, offset) = await journal.load(offset=offset)
            async with read.connect() as conn:
                async with conn.begin():
                    for event in events:
                        await apply(conn, event.entity_id, event.event)
            return ReadSideProcessor.active(
                db=db, read=read, journal=journal, offset=offset
            )

        return behaviour.receive(f)


class Greeter:
    class Timeout:
        pass

    @staticmethod
    def start(greeting: str) -> Behaviour:
        return Greeter.init(greeting, 0)

    @staticmethod
    def init(greeting: str, count: int) -> Behaviour:
        async def f(timers):
            await timers.start_fixed_rate_timer(Greeter.Timeout(), timedelta(seconds=1))
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
            counter = await context.spawn(Counter.start(), "Counter")
            await counter.tell(Count(2))
            await counter.tell(Count(3))
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

            router = routers.pool(
                2, routers.ConsistentHashing(lambda msg: msg.message)
            )(worker())
            echo = await context.spawn(router, "Router")
            replies = await asyncio.gather(
                echo.ask(partial(EchoMsg, message="Echooooo")),
                echo.ask(partial(EchoMsg, message="Meeeeeee")),
            )
            print(replies)

            await context.stop(echo)

            return PingPong.active(context)
            # return behaviour.ignore()

        return behaviour.restart(behaviour.setup(f))

    @staticmethod
    def active(context) -> Behaviour:
        async def f(msg):
            logging.info("Creating a new greeter")
            await context.spawn(Greeter.start("Hello 2"), "Greeter")
            return behaviour.same()

        return behaviour.receive(f)


def main():
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(system(PingPong.start()), debug=True)
