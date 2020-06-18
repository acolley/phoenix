import asyncio
import attr
from attr.validators import instance_of
import janus
from multipledispatch import dispatch
from pyrsistent import m
import threading

from phoenix import behaviour
from phoenix.actor.cell import ActorCell
from phoenix.actor.actor import ActorContext
from phoenix.behaviour import Behaviour
from phoenix.ref import Ref


class Executor:
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
            return Executor.active(context, m())

        return behaviour.setup(f)

    @staticmethod
    def active(context, actors) -> Behaviour:
        async def f(msg):
            dispatch_namespace = {}

            @dispatch(Executor.SpawnActor, namespace=dispatch_namespace)
            async def handle(msg: Executor.SpawnActor):
                ref = Ref(
                    id=msg.id, inbox=janus.Queue(), thread=threading.current_thread()
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
                    ),
                )
                task = asyncio.create_task(cell.run())
                await msg.reply_to.tell(Executor.ActorSpawned(ref=ref))
                return Executor.active(context, actors.set(ref, task))

            @dispatch(Executor.StopActor, namespace=dispatch_namespace)
            async def handle(msg: Executor.StopActor):
                task = actors[msg.ref]
                task.cancel()
                # ActorCell should not raise CancelledError
                await task
                await msg.reply_to.tell(Executor.ActorStopped(ref=msg.ref))
                return Executor.active(context, actors.remove(msg.ref))

            @dispatch(Executor.RemoveActor, namespace=dispatch_namespace)
            async def handle(msg: Executor.RemoveActor):
                task = actors[msg.ref]
                # Task is finished or finishing so we wait for it...
                await task
                await msg.reply_to.tell(Executor.ActorRemoved(msg.ref))
                return Executor.active(context, actors.remove(msg.ref))

            return await handle(msg)

        return behaviour.receive(f)
