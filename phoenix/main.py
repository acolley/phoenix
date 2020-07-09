import abc

# from aiohttp import web
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
from pyrsistent import m
import random
import traceback
from typing import Any, Callable, Optional, Union

from phoenix import behaviour, routers, singleton
from phoenix.behaviour import Behaviour, RestartStrategy
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
class HandleRequest:
    reply_to = attr.ib()


class Handler:
    @staticmethod
    def start() -> Behaviour:
        async def recv(msg):
            await msg.reply_to.tell("Hello World!")
            return behaviour.same()

        return behaviour.receive(recv)


class Http:
    @staticmethod
    def start(host: str, port: int) -> Behaviour:
        async def setup(context):
            handler = await context.spawn(Handler.start())

            async def hello(request):
                message = await handler.ask(
                    lambda reply_to: HandleRequest(reply_to=reply_to), timeout=10
                )
                return web.Response(text=message)

            app = web.Application()
            app.add_routes([web.get("/", hello)])
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, host, port)
            await site.start()

            async def cleanup(evt):
                print("Kill the server!!")
                await runner.cleanup()
                return behaviour.same()

            return behaviour.ignore().with_on_lifecycle(cleanup)

        return behaviour.setup(setup)


class Timeout:
    pass


@attr.s
class Batcher:
    @staticmethod
    def start() -> Behaviour:
        async def setup(context):
            await context.timers.start_fixed_delay_timer(
                Timeout(), timedelta(seconds=10)
            )

            async def stash(buffer):
                dispatch_namespace = {}

                @dispatch(int, namespace=dispatch_namespace)
                async def handle(msg: int):
                    buffer.stash(msg)
                    return behaviour.same()

                @dispatch(Timeout, namespace=dispatch_namespace)
                async def handle(msg: Timeout):
                    return buffer.unstash(Batcher.active())

                async def recv(msg):
                    return await handle(msg)

                return behaviour.receive(recv)

            return behaviour.stash(stash, capacity=10)

        return behaviour.setup(setup)

    @staticmethod
    def active() -> Behaviour:
        async def recv(msg):
            print(msg)
            return behaviour.same()

        return behaviour.receive(recv)


@attr.s
class Count:
    n: int = attr.ib(validator=instance_of(int))


@attr.s
class Counted:
    n: int = attr.ib(validator=instance_of(int))


@attr.s
class GetCount:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))


class Counter:
    @staticmethod
    def start(id: str) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(int, Count, namespace=dispatch_namespace)
        async def command_handler(state: int, cmd: Count) -> effect.Effect:
            return effect.persist([Counted(n=cmd.n)])

        @dispatch(int, GetCount, namespace=dispatch_namespace)
        async def command_handler(state: int, cmd: GetCount) -> effect.Effect:
            return effect.reply(cmd.reply_to, state)

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
        async def f(context):
            await context.timers.start_fixed_rate_timer(
                Greeter.Timeout(), timedelta(seconds=1)
            )
            return Greeter.active(greeting, count)

        return behaviour.restart(behaviour.setup(f)).with_backoff(
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


class Echo:
    @staticmethod
    def start() -> behaviour.Behaviour:
        async def recv(msg):
            m, reply_to = msg
            await reply_to.tell(m)
            return behaviour.same()

        return behaviour.receive(recv)


class PingPong:
    @attr.s
    class GreeterStopped:
        pass

    @staticmethod
    def start() -> Behaviour:
        async def f(context):
            # http = await context.spawn(Http.start("localhost", 8080))

            batcher = await context.spawn(Batcher.start, "batcher")
            for i in range(10):
                await batcher.tell(i)

            return behaviour.ignore()

            # single = await context.spawn(
            #     singleton.Singleton.start(m(counter=Counter.start))
            # )
            # await single.tell(
            #     singleton.Envelope(type_="counter", id="counter-0", msg=Count(2))
            # )
            # await single.tell(
            #     singleton.Envelope(type_="counter", id="counter", msg=Count(2))
            # )

            # print(
            #     await single.ask(
            #         lambda reply_to: singleton.Envelope(
            #             type_="counter", id="counter", msg=GetCount(reply_to=reply_to)
            #         )
            #     )
            # )

            # greeter = await context.spawn(Greeter.start("Hello"), "Greeter")
            # await context.watch(greeter, PingPong.GreeterStopped())
            # ping = await context.spawn(Ping.start(), "Ping")
            # pong = await context.spawn(Pong.start(), "Pong")
            # await ping.tell(pong)

            # def worker() -> Behaviour:
            #     async def f(message):
            #         await message.reply_to.tell(message.message)
            #         return behaviour.same()

            #     return behaviour.receive(f)

            # router = routers.pool(
            #     2, routers.ConsistentHashing(lambda msg: msg.message)
            # )(worker())
            # echo = await context.spawn(router, "Router")
            # replies = await asyncio.gather(
            #     echo.ask(partial(EchoMsg, message="Echooooo")),
            #     echo.ask(partial(EchoMsg, message="Meeeeeee")),
            # )
            # print(replies)

            # await context.stop(echo)

            # return PingPong.active(context)

        return behaviour.supervise(behaviour.setup(f)).on_failure(
            when=lambda e: isinstance(e, Exception), strategy=RestartStrategy(),
        )

    @staticmethod
    def active(context) -> Behaviour:
        async def f(msg):
            logging.info("Creating a new greeter")
            await context.spawn(Greeter.start("Hello 2"), "Greeter")
            return behaviour.same()

        return behaviour.receive(f)


async def async_main():
    sys = await system(Echo.start)
    msg = await sys.ask(lambda reply_to: ("Hello", reply_to))
    print(msg)
    await sys.run()


def main():
    logging.basicConfig(level=logging.DEBUG)
    # Issue with default Python 3.8 windows event loop?
    # https://stackoverflow.com/questions/62412754/python-asyncio-errors-oserror-winerror-6-the-handle-is-invalid-and-runtim
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(async_main(), debug=True)
