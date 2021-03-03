from aiohttp.web import Request, Response
import attr
from attr.validators import instance_of
from functools import partial
from multipledispatch import dispatch
from typing import Tuple

from phoenix import ActorId, Behaviour


@attr.s
class RequestHandler:
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    context = attr.ib()

    @attr.s
    class State:
        context = attr.ib()

    @attr.s
    class Hello:
        reply_to: ActorId = attr.ib(validator=instance_of(ActorId))
        request: Request = attr.ib(validator=instance_of(Request))

    @classmethod
    async def new(cls, context, name=None) -> "RequestHandler":
        actor_id = await context.spawn(cls.start, cls.handle, name=name)
        return cls(actor_id=actor_id, context=context)

    @staticmethod
    async def start(context) -> State:
        return RequestHandler.State(context=context)

    @staticmethod
    @dispatch(State, Hello)
    async def handle(state: State, msg: Hello) -> Tuple[Behaviour, State]:
        await state.context.cast(msg.reply_to, Response(text="Hello world"))
        return Behaviour.done, state

    async def hello(self, request: Request) -> Response:
        return await self.context.call(
            self.actor_id, partial(self.Hello, request=request)
        )
