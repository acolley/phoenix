from aiohttp import web
import asyncio
import attr
from attr.validators import instance_of
from functools import partial
import logging
from multimethod import multimethod
import sys

from phoenix import router
from phoenix.actor import Actor, ActorId, Context, ExitReason
from phoenix.dataclasses import dataclass
from phoenix.supervisor import RestartStrategy, Supervisor
from phoenix.system.system import ActorSystem

from webapi import handler


@dataclass
class State:
    runner: web.AppRunner
    router_id: ActorId


async def start(context: Context, host: str, port: int) -> Actor:
    router_id = await context.spawn(
        partial(
            router.start,
            workers=4,
            start=handler.start,
        ),
        name="HttpApi.RequestHandlers",
    )
    context.link(context.actor_id, router_id)

    async def hello(request):
        return await context.call(router_id, partial(handler.Hello, request=request))

    app = web.Application()
    app.add_routes([web.get("/", hello)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return Actor(
        state=State(runner=runner, router_id=router_id), handler=handle, on_exit=handle
    )


@multimethod
async def handle(state: State, msg: ExitReason):
    await state.runner.cleanup()


@dataclass
class HttpApi:
    actor_id: ActorId
    context: Context

    @classmethod
    async def new(cls, context, host: str, port: int, name=None) -> "HttpApi":
        actor_id = await context.spawn(partial(start, host=host, port=port), name=name)
        return cls(actor_id=actor_id, context=context)


async def main_async():
    system = ActorSystem("system")
    await system.start()

    api = await HttpApi.new(system, "localhost", 8080, name="HttpApi")

    try:
        await system.run_forever()
    except KeyboardInterrupt:
        await system.shutdown()


def main():
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.DEBUG)
    asyncio.run(main_async())
