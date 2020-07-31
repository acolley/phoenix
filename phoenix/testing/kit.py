import asyncio
import attr
from datetime import timedelta
import janus
from multipledispatch import dispatch
import threading
from typing import Callable, Optional
import uuid

from phoenix import behaviour
from phoenix.ref import Ref
from phoenix.system.system import ActorSystem


@attr.s
class Spawn:
    reply_to: Ref = attr.ib()
    id: Optional[str] = attr.ib()
    factory = attr.ib()


class Tester:
    @staticmethod
    def start() -> behaviour.Behaviour:
        async def setup(context) -> behaviour.Behaviour:
            namespace = {}

            @dispatch(Spawn, namespace=namespace)
            async def handle(msg: Spawn) -> behaviour.Behaviour:
                ref = await context.spawn(msg.factory, id=msg.id)
                await msg.reply_to.tell(ref)
                return behaviour.same()

            async def recv(msg) -> behaviour.Behaviour:
                return await handle(msg)

            return behaviour.receive(recv)

        return behaviour.setup(setup)


@attr.s
class Probe:
    ref: Ref = attr.ib()

    async def expect_message(
        self, msg, timeout: timedelta = timedelta(seconds=10)
    ) -> bool:
        async def _check():
            while True:
                m = await self.ref.inbox.async_q.get()
                if msg == m:
                    return True

        try:
            return await asyncio.wait_for(_check(), timeout=timeout.total_seconds())
        except asyncio.TimeoutError:
            return False


class ActorTestKit:
    def __init__(
        self,
    ):
        self.system = None

    async def start(self):
        system = ActorSystem(Tester.start)
        await system.start()
        self.system = system

    async def spawn(self, factory, id: Optional[str] = None) -> Ref:
        return await self.system.ask(
            lambda reply_to: Spawn(reply_to=reply_to, id=id, factory=factory)
        )

    async def create_probe(self) -> Ref:
        ref = Ref(
            id=str(uuid.uuid1()), inbox=janus.Queue(), thread=threading.current_thread()
        )
        return Probe(ref=ref)

    async def shutdown(self):
        await self.system.shutdown()
