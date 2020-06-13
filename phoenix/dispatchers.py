import asyncio
import attr
from attr.validators import instance_of, optional
import concurrent.futures
import contextlib
import janus
import logging
from multipledispatch import dispatch
from pyrsistent import m, v
import queue
import threading
import time
from typing import Any, List, Optional
import uuid

from phoenix import behaviour
from phoenix.actor import cell
from phoenix.actor.actor import ActorContext
from phoenix.actor.cell import ActorCell
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
from phoenix.system import messages

logger = logging.getLogger(__name__)


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

    @attr.s
    class CellStarted:
        task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))

    @attr.s
    class StopActor:
        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class ActorStopped:
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class RemoveActor:
        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class ActorRemoved:
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @staticmethod
    def start() -> Behaviour:
        async def f(context: ActorContext):
            return Spawner.active(context.system, m())

        return behaviour.setup(f)

    @staticmethod
    def active(system: Ref, actors) -> Behaviour:
        async def f(msg):
            # NOTE: function name of multipledispatch.dispatch target must be unique for the same type.
            @dispatch(Spawner.SpawnActor)
            async def spawner_handle(msg: Spawner.SpawnActor):
                ref = Ref(id=msg.id, inbox=janus.Queue())
                cell = ActorCell(
                    behaviour=msg.behaviour,
                    context=ActorContext(
                        ref=ref,
                        parent=msg.parent,
                        thread=threading.current_thread(),
                        loop=asyncio.get_event_loop(),
                        system=system,
                    ),
                )
                task = asyncio.create_task(cell.run())
                await msg.reply_to.tell(Spawner.ActorSpawned(ref=ref))
                return Spawner.active(system, actors.set(ref, task))

            @dispatch(Spawner.StopActor)
            async def spawner_handle(msg: Spawner.StopActor):
                task = actors[msg.ref]
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                await msg.reply_to.tell(Spawner.ActorStopped(ref=msg.ref))
                return Spawner.active(system, actors.remove(msg.ref))

            @dispatch(Spawner.RemoveActor)
            async def spawner_handle(msg: Spawner.RemoveActor):
                task = actors[msg.ref]
                # Task is finished or finishing so we wait for it...
                await task
                await msg.reply_to.tell(Spawner.ActorRemoved(msg.ref))
                return Spawner.active(system, actors.remove(msg.ref))

            return await spawner_handle(msg)

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
                    ref=Ref(id=f"spawner-{uuid.uuid1()}", inbox=janus.Queue()),
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

    @attr.s
    class StopActor:
        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class ActorStopped:
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class RemoveActor:
        """
        Remove an Actor that has already been killed.

        This will fail if the Actor is still running.
        """

        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @attr.s
    class ActorRemoved:
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
                # TODO: handle StopActor and ActorStopped requests
                # now the spawner is ready, process all requests that were waiting
                @dispatch(ThreadDispatcher.SpawnActor)
                async def _handle(msg: ThreadDispatcher.SpawnActor):
                    reply = await message.ref.ask(
                        lambda reply_to: Spawner.SpawnActor(
                            reply_to=reply_to,
                            id=msg.id,
                            behaviour=msg.behaviour,
                            parent=msg.parent,
                        )
                    )
                    await msg.reply_to.tell(
                        ThreadDispatcher.ActorSpawned(ref=reply.ref)
                    )

                @dispatch(ThreadDispatcher.StopActor)
                async def _handle(msg: ThreadDispatcher.StopActor):
                    await message.ref.ask(
                        lambda reply_to: Spawner.StopActor(
                            reply_to=reply_to, ref=msg.ref
                        )
                    )
                    await msg.reply_to.tell(ThreadDispatcher.ActorStopped(ref=msg.ref))

                @dispatch(ThreadDispatcher.RemoveActor)
                async def _handle(msg: ThreadDispatcher.RemoveActor):
                    await message.ref.ask(
                        lambda reply_to: Spawner.RemoveActor(
                            reply_to=reply_to, ref=msg.ref
                        )
                    )
                    await msg.reply_to.tell(ThreadDispatcher.ActorRemoved(ref=msg.ref))

                aws = [_handle(request) for request in requests]
                await asyncio.gather(*aws)
                return ThreadDispatcher.active(executor=executor, spawner=message.ref)
            else:
                # spawner is not ready yet, so queue up the request
                return ThreadDispatcher.waiting(
                    executor=executor, requests=requests.append(message)
                )

        return behaviour.receive(f)

    @staticmethod
    def active(executor: ThreadExecutor, spawner: Ref) -> Behaviour:
        @dispatch(ThreadDispatcher.SpawnActor)
        async def thread_dispatcher_handle(msg: ThreadDispatcher.SpawnActor):
            reply = await spawner.ask(
                lambda reply_to: Spawner.SpawnActor(
                    reply_to=reply_to,
                    id=msg.id,
                    behaviour=msg.behaviour,
                    parent=msg.parent,
                )
            )
            await msg.reply_to.tell(ThreadDispatcher.ActorSpawned(ref=reply.ref))

        @dispatch(ThreadDispatcher.StopActor)
        async def thread_dispatcher_handle(msg: ThreadDispatcher.StopActor):
            await spawner.ask(
                lambda reply_to: Spawner.StopActor(reply_to=reply_to, ref=msg.ref)
            )
            await msg.reply_to.tell(ThreadDispatcher.ActorStopped(ref=msg.ref))

        @dispatch(ThreadDispatcher.RemoveActor)
        async def thread_dispatcher_handle(msg: ThreadDispatcher.RemoveActor):
            await spawner.ask(
                lambda reply_to: Spawner.RemoveActor(reply_to=reply_to, ref=msg.ref)
            )
            await msg.reply_to.tell(ThreadDispatcher.ActorRemoved(ref=msg.ref))

        async def f(msg):
            await thread_dispatcher_handle(msg)
            return behaviour.same()

        return behaviour.receive(f)
