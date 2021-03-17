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
from phoenix.retry import retry
from phoenix.supervisor import ChildSpec, RestartStrategy, RestartWhen, Supervisor
from phoenix.system.system import ActorSystem

from webapi import handler


@dataclass
class State:
    runner: web.AppRunner


async def start(context: Context, host: str, port: int) -> Actor:
    supervisor = await Supervisor.new(
        context=context,
        name="Supervisor.HttpApi.RequestHandlers",
    )
    await supervisor.init(
        children=[
            ChildSpec(
                start=partial(router.start, workers=4, start=handler.start),
                options=dict(name="HttpApi.RequestHandlers"),
                restart_when=RestartWhen.permanent,
            ),
        ],
        strategy=RestartStrategy.one_for_one,
    )
    await context.link(context.actor_id, supervisor.actor_id)

    router_id = ActorId(system_id=context.actor_id.system_id, value="HttpApi.RequestHandlers")

    async def hello(request):
        return await retry(max_retries=1)(lambda: asyncio.wait_for(context.call(router_id, partial(handler.Hello, request=request)), timeout=3))

    app = web.Application()
    app.add_routes([web.get("/", hello)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return Actor(
        state=State(runner=runner), handler=handle, on_exit=handle
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
