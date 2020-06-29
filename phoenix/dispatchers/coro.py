import asyncio
import janus
from multipledispatch import dispatch
from pyrsistent import m, PMap
import threading

from phoenix import behaviour
from phoenix.actor.cell import ActorCell
from phoenix.actor.context import ActorContext
from phoenix.actor.timers import Timers
from phoenix.behaviour import Behaviour
from phoenix.dispatchers.dispatcher import (
    ActorRemoved,
    ActorSpawned,
    ActorStopped,
    RemoveActor,
    SpawnActor,
    StopActor,
)
from phoenix.ref import Ref


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
        async def coro_dispatcher_handle(msg: SpawnActor):
            ref = Ref(
                id=msg.id,
                path=msg.parent.path / msg.id,
                inbox=janus.Queue(),
                thread=threading.current_thread(),
            )
            cell = ActorCell(
                behaviour=msg.behaviour,
                context=ActorContext(
                    ref=ref,
                    parent=msg.parent,
                    thread=threading.current_thread(),
                    loop=asyncio.get_event_loop(),
                    system=context.system,
                    registry=context.registry,
                    timers=Timers(ref=ref, lock=asyncio.Lock()),
                ),
            )
            task = asyncio.create_task(cell.run())
            await msg.reply_to.tell(ActorSpawned(ref=ref))
            return CoroDispatcher.active(context=context, actors=actors.set(ref, task))

        @dispatch(StopActor, namespace=dispatch_namespace)
        async def coro_dispatcher_handle(msg: SpawnActor):
            task = actors[msg.ref]
            task.cancel()
            # ActorCell should not raise CancelledError
            await task
            await msg.reply_to.tell(ActorStopped(ref=msg.ref))
            return CoroDispatcher.active(context=context, actors=actors.remove(msg.ref))

        @dispatch(RemoveActor, namespace=dispatch_namespace)
        async def coro_dispatcher_handle(msg: SpawnActor):
            task = actors[msg.ref]
            # Task is finished or finishing so we wait for it...
            await task
            await msg.reply_to.tell(ActorRemoved(msg.ref))
            return CoroDispatcher.active(context=context, actors=actors.remove(msg.ref))

        async def f(msg):
            return await coro_dispatcher_handle(msg)

        return behaviour.receive(f)
