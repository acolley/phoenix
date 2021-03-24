from aiohttp import web
import asyncio
from functools import partial
import logging
from multimethod import multimethod
import pickle

from phoenix import router
from phoenix.actor import Actor, ActorId, Context, ExitReason
from phoenix.dataclasses import dataclass
from phoenix.retry import retry
from phoenix.supervision.supervisor import (
    ChildSpec,
    RestartStrategy,
    RestartWhen,
    Supervisor,
)

logger = logging.getLogger(__name__)


@dataclass
class State:
    runner: web.AppRunner


async def start(context: Context, host: str, port: int) -> Actor:
    async def create_message(request):
        msg = await request.read()
        msg = pickle.loads(msg)
        await context.cast(msg.actor_id, msg.msg)
        return web.Response(body="")

    app = web.Application()
    app.add_routes([web.post("/messages", create_message)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return Actor(state=State(runner=runner), handler=handle, on_exit=handle)


@multimethod
async def handle(state: State, msg: ExitReason):
    await state.runner.cleanup()
