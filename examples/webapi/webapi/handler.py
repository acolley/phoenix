from aiohttp.web import Request, Response
import attr
from attr.validators import instance_of
from functools import partial
from typing import Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.dataclasses import dataclass


@dataclass
class State:
    context: Context


@dataclass
class Hello:
    reply_to: ActorId
    request: Request


async def start(context: Context) -> Actor:
    return Actor(state=State(context=context), handler=handle)


async def handle(state: State, msg: Hello) -> Tuple[Behaviour, State]:
    await state.context.cast(msg.reply_to, Response(text="Hello world"))
    return Behaviour.done, state


@dataclass
class RequestHandler:
    actor_id: ActorId
    context: Context

    @classmethod
    async def new(cls, context: Context, name=None) -> "RequestHandler":
        actor_id = await context.spawn(start, name=name)
        return cls(actor_id=actor_id, context=context)

    async def hello(self, request: Request) -> Response:
        return await self.context.call(self.actor_id, partial(Hello, request=request))
