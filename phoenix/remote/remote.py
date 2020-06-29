from aiohttp import ClientSession, web
import attr
from attr.validators import instance_of
import cattr
from datetime import timedelta
from multipledispatch import dispatch
from typing import Any
from yarl import URL

from phoenix import behaviour
from phoenix.address import Address
from phoenix.behaviour import Behaviour
from phoenix.path import ActorPath
from phoenix.ref import LocalRef
from phoenix.remote.ref import RemoteRef


@attr.s(frozen=True)
class Tell:
    path: ActorPath = attr.ib(validator=instance_of(ActorPath))
    msg: Any = attr.ib()


@attr.s(frozen=True)
class RemoteTell:
    path: ActorPath = attr.ib(validator=instance_of(ActorPath))
    msg: Any = attr.ib()


@attr.s(frozen=True)
class TellSelection:
    url: URL = attr.ib(validator=instance_of(URL))
    msg: Any = attr.ib()


class Remote:
    @staticmethod
    def start(address: Address) -> Behaviour:
        async def setup(context):
            # Encode/decode local Refs into/from RemoteRefs
            converter = cattr.Converter()
            converter.register_structure_hook(
                LocalRef, lambda ref, t: RemoteRef(path=ref, remote=context.ref)
            )
            converter.register_unstructure_hook(
                LocalRef, lambda ref: converter.unstructure(ref.path)
            )
            converter.register_structure_hook(
                URL, lambda url, t: str(url),
            )
            converter.register_unstructure_hook(URL, URL)

            dispatch_namespace = {}

            @dispatch(RemoteTell, namespace=dispatch_namespace)
            async def handle(msg: RemoteTell):
                local_ref = await context.system.ask(
                    lambda reply_to: messages.LocalRefFor(
                        reply_to=reply_to, path=msg.path
                    ),
                    timeout=timedelta(seconds=10),
                )
                await local_ref.tell(msg.msg)
                return behaviour.same()

            @dispatch(Tell, namespace=dispatch_namespace)
            async def handle(msg: Tell):
                cmd = RemoteTell(path=msg.path, msg=msg.msg)
                # TODO: timeout
                async with ClientSession() as session:
                    async with session.post(
                        f"http://{msg.path.address.host}:{msg.path.address.port}/tell",
                        json=cattr.unstructure(cmd),
                    ) as resp:
                        resp.raise_for_status()

                return behaviour.same()

            async def tell(request):
                cmd = await request.json()
                cmd = cattr.structure(cmd)

                await context.ref.tell(cmd)

            app = web.Application()
            app.add_routes([web.post("/tell", tell)])
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, address.host, address.port)
            await site.start()

            async def cleanup(evt):
                await runner.cleanup()
                return behaviour.same()

            async def recv(msg):
                return await handle(msg)

            return behaviour.receive(recv).with_on_lifecycle(cleanup)

        return behaviour.setup(setup)
