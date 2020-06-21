import aiohttp
from aiohttp import web
import asyncio
import attr
from attr.validators import instance_of
import cattr
from datetime import datetime, timedelta
import logging
from phoenix import behaviour, singleton
from phoenix.behaviour import Behaviour
from phoenix.persistence import effect
from phoenix.ref import Ref
from phoenix.system.system import system
from pyrsistent import m
import pytz
from typing import Any

from chat import channel


@attr.s
class SubscriberReceived:
    msg: Any = attr.ib()


@attr.s(frozen=True)
class SubscriberFinished:
    event: asyncio.Event = attr.ib(validator=instance_of(asyncio.Event))


class MessageSubscriber:
    """
    Read side processor for channel messages
    that keeps a client notified of new messages.
    """
    @staticmethod
    def start(ws) -> Behaviour:
        async def setup(context):
            await context.pipe_to_self(ws.receive, lambda msg: SubscriberReceived(msg))
            async def recv(msg):
                print(msg.msg.data)
                # await context.pipe_to_self(ws.receive, lambda msg: SubscriberReceived(msg))
                return behaviour.stop()
            return behaviour.receive(recv)
        return behaviour.setup(setup)


class Api:
    @staticmethod
    def start(host: str, port: int, entities: Ref) -> Behaviour:
        async def setup(context):
            async def create_channel(request):
                reply = await entities.ask(
                    lambda reply_to: singleton.Envelope(
                        type_="channel",
                        id=request.match_info["id"],
                        msg=channel.Create(reply_to=reply_to),
                    ),
                    timeout=timedelta(seconds=10),
                )
                if isinstance(reply, channel.Confirmation):
                    return web.Response(text=request.match_info["id"])
                raise web.HTTPBadRequest

            async def delete_channel(request):
                reply = await entities.ask(
                    lambda reply_to: singleton.Envelope(
                        type_="channel",
                        id=request.match_info["id"],
                        msg=channel.Delete(reply_to=reply_to),
                    ),
                    timeout=timedelta(seconds=10),
                )
                if isinstance(reply, channel.Confirmation):
                    return web.Response(text=request.match_info["id"])
                raise web.HTTPBadRequest

            async def create_message(request):
                body = await request.text()
                # await request.json()
                reply = await entities.ask(
                    lambda reply_to: singleton.Envelope(
                        type_="channel",
                        id=request.match_info["id"],
                        msg=channel.Say(
                            reply_to=reply_to,
                            at=datetime.utcnow().replace(tzinfo=pytz.utc),
                            by="unknown",
                            text=body,
                        ),
                    ),
                    timeout=timedelta(seconds=10),
                )
                if isinstance(reply, channel.Confirmation):
                    return web.Response(text=request.match_info["id"])
                raise web.HTTPBadRequest
            
            async def list_messages(request):
                reply = await entities.ask(
                    lambda reply_to: singleton.Envelope(
                        type_="channel",
                        id=request.match_info["id"],
                        msg=channel.ListMessages(
                            reply_to=reply_to,
                        ),
                    ),
                    timeout=timedelta(seconds=10),
                )
                if isinstance(reply, channel.Messages):
                    messages = [cattr.unstructure(msg) for msg in reply.messages]
                    return web.json_response({"messages": messages})
                raise web.HTTPBadRequest
            
            async def subscribe(request):
                ws = web.WebSocketResponse()
                await ws.prepare(request)

                # FIXME: we can't return from this function until
                # the request has been finished (i.e. closed) otherwise
                # aiohttp will close our connection as it thinks it is done.
                subscriber = await context.spawn(MessageSubscriber.start(ws))

                # MessageSubscriber must be in the same thread in order for this to work.
                finished = asyncio.Event()
                await context.watch(subscriber, SubscriberFinished(finished))
                await finished.wait()

                print("finished")

                return ws

            app = web.Application()
            app.add_routes(
                [
                    web.post("/channels/{id}", create_channel),
                    web.delete("/channels/{id}", delete_channel),
                    web.post("/channels/{id}/messages", create_message),
                    web.get("/channels/{id}/messages", list_messages),
                    web.get("/channels/{id}/messages/subscribe", subscribe),
                ]
            )
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, host, port)
            await site.start()

            async def cleanup(evt):
                await runner.cleanup()
                return behaviour.same()

            async def recv(msg):
                event = msg.event
                event.set()
                return behaviour.same()

            return behaviour.receive(recv).with_on_lifecycle(cleanup)

        return behaviour.setup(setup)


class Application:
    @staticmethod
    def start() -> Behaviour:
        async def setup(context):
            entities = await context.spawn(
                singleton.Singleton.start(m(**{"channel": channel.Channel.start,}))
            )
            await context.spawn(Api.start("localhost", 8080, entities))
            return behaviour.ignore()

        return behaviour.setup(setup)


def server():
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(system(Application.start()))


def client():
    async def _run():
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect("http://localhost:8080/channels/hello/messages/subscribe") as ws:
                while True:
                    await ws.send_str("Hello there!!!")
                    await asyncio.sleep(5)
    asyncio.run(_run())
