import asyncio
import janus
from multipledispatch import dispatch
from pyrsistent import m, PMap
import threading

from phoenix import behaviour
from phoenix.actor.cell import ActorCell
from phoenix.actor.context import ActorContext
from phoenix.actor.scheduler import Scheduler
from phoenix.behaviour import Behaviour
from phoenix.dispatchers.dispatcher import (
    ActorRemoved,
    ActorSpawned,
    ActorStopped,
    RemoveActor,
    Shutdown,
    SpawnActor,
    StopActor,
)
from phoenix.ref import Ref
from phoenix.result import Success


class CoroDispatcher:
    """
    Dispatches actors to the main thread as coroutines.
    """

    @staticmethod
    def start() -> Behaviour:
        async def f(context: ActorContext):
            return CoroDispatcher.active(context=context, actors=m())

        return behaviour.setup(f)

    @staticmethod
    def active(context: ActorContext, actors: PMap) -> Behaviour:
        dispatch_namespace = {}

        @dispatch(SpawnActor, namespace=dispatch_namespace)
        async def handle(msg: SpawnActor):
            ref = Ref(id=msg.id, inbox=msg.mailbox(), thread=threading.current_thread())
            cell = ActorCell(
                behaviour=msg.behaviour(),
                context=ActorContext(
                    ref=ref,
                    parent=msg.parent,
                    thread=threading.current_thread(),
                    loop=asyncio.get_event_loop(),
                    executor=context.executor,
                    system=context.system,
                    registry=context.registry,
                    timers=Scheduler(ref=ref, lock=asyncio.Lock()),
                ),
            )
            task = asyncio.get_event_loop().create_task(
                cell.run(), name=f"cell-{msg.id}"
            )
            await msg.reply_to.tell(ActorSpawned(ref=ref))
            return CoroDispatcher.active(context=context, actors=actors.set(ref, task))

        @dispatch(StopActor, namespace=dispatch_namespace)
        async def handle(msg: SpawnActor):
            task = actors[msg.ref]
            task.cancel()
            # ActorCell should not raise CancelledError
            await task
            await msg.reply_to.tell(ActorStopped(ref=msg.ref))
            return CoroDispatcher.active(context=context, actors=actors.remove(msg.ref))

        @dispatch(RemoveActor, namespace=dispatch_namespace)
        async def handle(msg: SpawnActor):
            task = actors[msg.ref]
            # Task is finished or finishing so we wait for it...
            await task
            await msg.reply_to.tell(ActorRemoved(msg.ref))
            return CoroDispatcher.active(context=context, actors=actors.remove(msg.ref))

        @dispatch(Shutdown, namespace=dispatch_namespace)
        async def handle(msg: Shutdown):
            for task in actors.values():
                task.cancel()
                await task
            await msg.reply_to.tell(Success(None))
            return behaviour.stop()

        async def f(msg):
            return await handle(msg)

        return behaviour.receive(f)
