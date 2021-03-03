from aiohttp import web
import asyncio
import attr
from attr.validators import instance_of
from functools import partial
import logging
import sys

from phoenix import ActorId, ActorSystem
from phoenix.supervisor import RestartStrategy, Supervisor

from webapi.handler import RequestHandler


@attr.s
class HttpApi:
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    context = attr.ib()

    @attr.s
    class State:
        runner: web.AppRunner = attr.ib(validator=instance_of(web.AppRunner))
        handler: RequestHandler = attr.ib(validator=instance_of(RequestHandler))

    @classmethod
    async def new(cls, context, host: str, port: int, name=None) -> "HttpApi":
        actor_id = await context.spawn(
            partial(cls.start, host=host, port=port), cls.handle, name=name
        )
        return cls(actor_id=actor_id, context=context)

    @staticmethod
    async def start(context, host: str, port: int) -> State:
        supervisor = await Supervisor.new(
            context, name="Supervisor.HttpApi.RequestHandler"
        )
        await supervisor.init(
            children=[
                (
                    RequestHandler.start,
                    RequestHandler.handle,
                    dict(name="HttpApi.RequestHandler"),
                )
            ],
            strategy=RestartStrategy.one_for_one,
        )

        handler = RequestHandler(
            actor_id=ActorId("HttpApi.RequestHandler"), context=context
        )

        async def hello(request):
            return await handler.hello(request)

        app = web.Application()
        app.add_routes([web.get("/", hello)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        return HttpApi.State(runner=runner, handler=handler)

    @staticmethod
    async def handle(state: State, msg):
        raise NotImplementedError

    @staticmethod
    async def cleanup(state: State):
        pass


async def main_async():
    system = ActorSystem()
    task = asyncio.create_task(system.run())

    api = await HttpApi.new(system, "localhost", 8080, name="HttpApi")

    await asyncio.sleep(60)
    await system.shutdown()


def main():
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=logging.DEBUG)
    asyncio.run(main_async())
