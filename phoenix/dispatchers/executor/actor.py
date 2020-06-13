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
            return Executor.active(context.system, m())

        return behaviour.setup(f)

    @staticmethod
    def active(system: Ref, actors) -> Behaviour:
        async def f(msg):
            # NOTE: function name of multipledispatch.dispatch target must be unique for the same type.
            @dispatch(Executor.SpawnActor)
            async def spawner_handle(msg: Executor.SpawnActor):
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
                await msg.reply_to.tell(Executor.ActorSpawned(ref=ref))
                return Executor.active(system, actors.set(ref, task))

            @dispatch(Executor.StopActor)
            async def spawner_handle(msg: Executor.StopActor):
                task = actors[msg.ref]
                task.cancel()
                # ActorCell should not raise CancelledError
                await task
                await msg.reply_to.tell(Executor.ActorStopped(ref=msg.ref))
                return Executor.active(system, actors.remove(msg.ref))

            @dispatch(Executor.RemoveActor)
            async def spawner_handle(msg: Executor.RemoveActor):
                task = actors[msg.ref]
                # Task is finished or finishing so we wait for it...
                await task
                await msg.reply_to.tell(Executor.ActorRemoved(msg.ref))
                return Executor.active(system, actors.remove(msg.ref))

            return await spawner_handle(msg)

        return behaviour.receive(f)
