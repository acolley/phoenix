import aiohttp
from aiohttp import web
import asyncio
import attr
from attr.validators import instance_of
import cattr
from datetime import datetime, timedelta
from functools import partial
import logging
from multipledispatch import dispatch
from phoenix import behaviour, singleton
from phoenix.behaviour import Behaviour
from phoenix.persistence import effect
from phoenix.persistence.journal import SqlAlchemyReadJournal
from phoenix.result import Failure, Success
from phoenix.ref import Ref
from phoenix.system.system import system
from pyrsistent import m
import pytz
from sqlalchemy import create_engine
from sqlalchemy_aio import ASYNCIO_STRATEGY
from typing import Any, Iterable, Union

from chat import channel
from chat.encoding import decode

logger = logging.getLogger(__name__)


@attr.s
class WebSocketMessageReceived:
    result: Union[Success, Failure[Exception]] = attr.ib()


@attr.s
class Timeout:
    pass


@attr.s
class EventsLoaded:
    result = attr.ib()


@attr.s(frozen=True)
class SubscriberFinished:
    event: asyncio.Event = attr.ib(validator=instance_of(asyncio.Event))


class MessageSubscriber:
    """
    Read side processor for channel messages
    that keeps a client notified of new messages.
    """

    @staticmethod
    def start(
        channel_id: str, offset: int, journal, ws: web.WebSocketResponse
    ) -> Behaviour:
        async def setup(context):
            await context.timers.start_fixed_rate_timer(Timeout(), timedelta(seconds=5), initial_delay=timedelta(0))
            await context.pipe_to_self(
                ws.receive, lambda msg: WebSocketMessageReceived(msg)
            )

            dispatch_namespace = {}

            @dispatch(WebSocketMessageReceived, namespace=dispatch_namespace)
            async def handle(msg: WebSocketMessageReceived):
                if isinstance(msg.result, Failure):
                    logger.error(str(msg.result.value))
                    return behaviour.stop()

                if msg.result.value.type == aiohttp.WSMsgType.ERROR:
                    logger.error(
                        "[%s] WebSocket error: %s", context.ref, ws.exception()
                    )
                    return behaviour.stop()

                await context.pipe_to_self(
                    ws.receive, lambda msg: WebSocketMessageReceived(msg)
                )
                return behaviour.same()

            @dispatch(Timeout, namespace=dispatch_namespace)
            async def handle(msg: Timeout):
                await context.pipe_to_self(
                    partial(journal.load, offset=offset),
                    EventsLoaded,
                )
                return behaviour.same()
            
            @dispatch(EventsLoaded, namespace=dispatch_namespace)
            async def handle(msg: EventsLoaded):
                if isinstance(msg.result, Failure):
                    logger.error(str(msg.result.value))
                    return behaviour.stop()

                nonlocal offset
                if msg.result.value:
                    offset = msg.result.value[-1].offset
                events = [
                    event
                    for event in msg.result.value
                    if event.entity_id == channel_id
                    and isinstance(event.event, channel.MessageCreated)
                ]
                for event in events:
                    await ws.send_json(
                        {
                            "message": {
                                "at": event.event.at.isoformat(),
                                "by": event.event.by,
                                "text": event.event.text,
                            },
                            "offset": event.offset,
                        }
                    )
                return behaviour.same()

            async def recv(msg):
                return await handle(msg)

            return behaviour.receive(recv)

        return behaviour.setup(setup)


class Api:
    @staticmethod
    def start(host: str, port: int, journal, entities: Ref) -> Behaviour:
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
                        msg=channel.ListMessages(reply_to=reply_to,),
                    ),
                    timeout=timedelta(seconds=10),
                )
                if isinstance(reply, channel.Messages):
                    messages = [cattr.unstructure(msg) for msg in reply.messages]
                    return web.json_response({"messages": messages})
                raise web.HTTPBadRequest

            async def subscribe(request):
                offset = request.headers.get("X-CHAT-MESSAGE-OFFSET")
                ws = web.WebSocketResponse()
                await ws.prepare(request)

                subscriber = await context.spawn(
                    MessageSubscriber.start(
                        channel_id=request.match_info["id"],
                        offset=int(offset) if offset is not None else None,
                        journal=journal,
                        ws=ws,
                    )
                )

                # We can't return from this function until
                # the request has been finished (i.e. the websocket is closed)
                #  otherwise aiohttp will close our connection as it thinks it is done.
                # Therefore we create an event that is set from the Api
                # actor when it receives the SubscriberFinished message after
                # the MessageSubscriber actor has stopped.
                # MessageSubscriber must be in the same thread in order for this to work
                # due to the fact that we're using a non-threadsafe asyncio.Event.
                finished = asyncio.Event()
                await context.watch(subscriber, SubscriberFinished(finished))
                await finished.wait()

                return ws

            app = web.Application()
            app.add_routes(
                [
                    web.post("/channels/{id}", create_channel),
                    web.delete("/channels/{id}", delete_channel),
                    web.post("/channels/{id}/messages", create_message),
                    web.get("/channels/{id}/messages", list_messages),
                    web.get(
                        "/channels/{id}/messages/subscribe", subscribe
                    ),
                ]
            )
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, host, port)
            await site.start()

            async def cleanup(evt):
                await runner.cleanup()
                return behaviour.same()

            async def recv(msg: SubscriberFinished):
                event = msg.event
                event.set()
                return behaviour.same()

            return behaviour.receive(recv).with_on_lifecycle(cleanup)

        return behaviour.setup(setup)


class Application:
    @staticmethod
    def start() -> Behaviour:
        async def setup(context):
            journal = SqlAlchemyReadJournal(
                engine=create_engine("sqlite:///db", strategy=ASYNCIO_STRATEGY),
                decode=decode,
            )
            entities = await context.spawn(
                singleton.Singleton.start(m(**{"channel": channel.Channel.start}))
            )
            await context.spawn(Api.start("localhost", 8080, journal, entities))
            return behaviour.ignore()

        return behaviour.setup(setup)


def server():
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(system(Application.start()))


def client():
    async def _run():
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                "http://localhost:8080/channels/hello/messages/subscribe"
            ) as ws:
                await ws.send_json({"offset": None})
                async for msg in ws:
                    print(msg.data)

    asyncio.run(_run())
