import asyncio
import attr
from attr.validators import instance_of, optional
import concurrent.futures
import janus
import logging
from multipledispatch import dispatch
from pyrsistent import v
import queue
import threading
import time
from typing import Any, List, Optional
import uuid

from phoenix import behaviour
from phoenix.actor import ActorCell, ActorContext
from phoenix.behaviour import (
    Behaviour,
    Ignore,
    Schedule,
    Receive,
    Restart,
    Same,
    Setup,
    Stop,
)
from phoenix.ref import Ref

logger = logging.getLogger(__name__)


# @attr.s
# class Actor:
#     parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
#     behaviour: Behaviour = attr.ib()
#     ref: Ref = attr.ib(validator=instance_of(Ref))
#     task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))


# @attr.s
# class Timers:
#     scheduler = attr.ib()
#     ref: Ref = attr.ib(validator=instance_of(Ref))

#     async def start_singleshot_timer(
#         self, message: Any, interval: timedelta, key: Optional[str] = None
#     ):
#         await self.scheduler.put(
#             StartSingleShotTimer(
#                 ref=self.ref, message=message, interval=interval, key=key
#             )
#         )

#     async def start_fixed_delay_timer(
#         self, message: Any, interval: timedelta, key: Optional[str] = None
#     ):
#         await self.scheduler.put(
#             StartFixedDelayTimer(
#                 ref=self.ref, message=message, interval=interval, key=key
#             )
#         )

#     async def cancel(self, key: str):
#         await self.scheduler.put(CancelTimer(ref=self.ref, key=key))


class Spawner:
    @attr.s
    class SpawnActor:
        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        id: str = attr.ib(validator=instance_of(str))
        behaviour: Behaviour = attr.ib()
        parent: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class ActorSpawned:
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @staticmethod
    def start() -> Behaviour:
        async def f(context: ActorContext):
            return Spawner.active(context.system)

        return behaviour.setup(f)

    @staticmethod
    def active(system: Ref) -> Behaviour:
        async def f(message: Spawner.SpawnActor):
            ref = Ref(id=message.id, inbox=janus.Queue())
            cell = ActorCell(
                behaviour=message.behaviour,
                context=ActorContext(
                    ref=ref,
                    parent=message.parent,
                    thread=threading.current_thread(),
                    loop=asyncio.get_event_loop(),
                    system=system,
                ),
            )
            asyncio.create_task(cell.run())
            await message.reply_to.tell(Spawner.ActorSpawned(ref=ref))
            return behaviour.same()

        return behaviour.receive(f)


class ThreadExecutor(threading.Thread):
    def __init__(self, dispatcher: Ref, system: Ref):
        super(ThreadExecutor, self).__init__()

        self.dispatcher = dispatcher
        self.system = system

    def run(self):
        logger.debug("Starting ThreadExecutor on thread %s", threading.current_thread())

        async def _run():

            spawner = ActorCell(
                behaviour=Spawner.start(),
                context=ActorContext(
                    ref=Ref(id=str(uuid.uuid1()), inbox=janus.Queue()),
                    parent=self.dispatcher,
                    thread=threading.current_thread(),
                    loop=asyncio.get_event_loop(),
                    system=self.system,
                ),
            )

            task = asyncio.create_task(spawner.run())

            await self.dispatcher.tell(
                SpawnerCreated(executor=self, ref=spawner.context.ref)
            )

            await task

        asyncio.run(_run())


@attr.s
class SpawnerCreated:
    executor: ThreadExecutor = attr.ib(validator=instance_of(ThreadExecutor))
    ref: Ref = attr.ib(validator=instance_of(Ref))


class ThreadDispatcher:
    @attr.s
    class SpawnActor:
        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        id: str = attr.ib(validator=instance_of(str))
        behaviour: Behaviour = attr.ib()
        parent: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class ActorSpawned:
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @staticmethod
    def start(max_threads: int) -> Behaviour:
        async def f(context: ActorContext):
            executor = ThreadExecutor(dispatcher=context.ref, system=context.system)
            executor.daemon = True
            executor.start()
            return ThreadDispatcher.waiting(executor, v())

        return behaviour.setup(f)

    @staticmethod
    def waiting(executor: ThreadExecutor, requests) -> Behaviour:
        async def f(message):
            if isinstance(message, SpawnerCreated):
                # now the spawner is ready, process all requests that were waiting
                for request in requests:
                    reply = await message.ref.ask(
                        lambda reply_to: Spawner.SpawnActor(
                            reply_to=reply_to,
                            id=request.id,
                            behaviour=request.behaviour,
                            parent=request.parent,
                        )
                    )
                    await request.reply_to.tell(ThreadDispatcher.ActorSpawned(ref=reply.ref))

                return ThreadDispatcher.active(executor=executor, spawner=message.ref)
            else:
                # spawner is not ready yet, so queue up the request
                return ThreadDispatcher.waiting(executor=executor, requests=requests.append(message))

        return behaviour.receive(f)

    @staticmethod
    def active(executor: ThreadExecutor, spawner: Ref) -> Behaviour:
        async def f(message: ThreadDispatcher.SpawnActor):
            reply = await spawner.ask(
                lambda reply_to: Spawner.SpawnActor(
                    reply_to=reply_to,
                    id=message.id,
                    behaviour=message.behaviour,
                    parent=message.parent,
                )
            )
            await message.reply_to.tell(ThreadDispatcher.ActorSpawned(ref=reply.ref))
            return behaviour.same()

        return behaviour.receive(f)
