import asyncio
import attr
from attr.validators import deep_iterable, deep_mapping, instance_of
import janus
from multipledispatch import dispatch
from pyrsistent import m, PVector, v
import threading
from typing import Iterable, Mapping

from phoenix import behaviour
from phoenix.actor.actor import ActorContext
from phoenix.actor.cell import BootstrapActorCell
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
from phoenix.dispatchers.executor.actor import Executor
from phoenix.dispatchers.executor.thread import ThreadExecutor
from phoenix.dispatchers.worker import DispatcherWorker
from phoenix.ref import Ref
from phoenix.system import messages


@attr.s(frozen=True)
class Dispatcher:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))


@attr.s(frozen=True)
class State:
    max_threads: int = attr.ib(validator=instance_of(int))
    context: ActorContext = attr.ib(validator=instance_of(ActorContext))
    dispatchers: Iterable[Dispatcher] = attr.ib(
        validator=deep_iterable(
            member_validator=instance_of(Dispatcher),
            iterable_validator=instance_of(PVector),
        )
    )
    actor_dispatcher: Mapping[Ref, Ref] = attr.ib(
        validator=deep_mapping(
            key_validator=instance_of(Ref), value_validator=instance_of(Ref)
        )
    )
    index: int = attr.ib(validator=instance_of(int))

    @max_threads.validator
    def check(self, attribute: str, value: int):
        if value < 1:
            raise ValueError("max_threads must be greater than zero")

    @dispatchers.validator
    def check(self, attribute: str, value: Iterable[Dispatcher]):
        if len(value) > self.max_threads:
            raise ValueError("Length of dispatchers cannot be greater than max_threads")

    @index.validator
    def check(self, attribute: str, value: int):
        if value < 0:
            raise ValueError("index cannot be less than zero.")
        if value > len(self.dispatchers):
            raise ValueError("index cannot be greater than length of dispatchers.")


# TODO: restart threads and their actors if failure occurs.
# TODO: this does not schedule actors fairly across threads as they are stopped.
class ThreadDispatcher:
    """
    An Actor that dispatches actors to a pool of threads.
    """

    @staticmethod
    def start(max_threads: int) -> Behaviour:
        async def f(context: ActorContext):
            return ThreadDispatcher.active(
                State(
                    max_threads=max_threads,
                    context=context,
                    dispatchers=v(),
                    actor_dispatcher=m(),
                    index=0,
                )
            )

        return behaviour.setup(f)

    @staticmethod
    def active(state: State) -> Behaviour:

        dispatch_namespace = {}

        @dispatch(SpawnActor, namespace=dispatch_namespace)
        async def handle(msg: SpawnActor):
            nonlocal state
            try:
                dispatcher = state.dispatchers[state.index]
            except IndexError:
                ref = Ref(
                    id=f"{state.context.ref.id}-{state.index}",
                    inbox=janus.Queue(),
                    thread=threading.current_thread(),
                )
                cell = BootstrapActorCell(
                    behaviour=DispatcherWorker.start(),
                    context=ActorContext(
                        ref=ref,
                        parent=state.context.ref,
                        thread=threading.current_thread(),
                        loop=asyncio.get_event_loop(),
                        system=state.context.system,
                        registry=state.context.registry,
                        timers=Timers(ref=ref, lock=asyncio.Lock()),
                    ),
                )
                task = asyncio.create_task(cell.run())
                dispatcher = Dispatcher(ref=ref, task=task)
                dispatchers = state.dispatchers.append(dispatcher)
            else:
                dispatchers = state.dispatchers

            reply = await dispatcher.ref.ask(
                lambda reply_to: DispatcherWorker.SpawnActor(
                    reply_to=reply_to,
                    id=msg.id,
                    behaviour=msg.behaviour,
                    parent=msg.parent,
                )
            )

            state = attr.evolve(
                state,
                dispatchers=dispatchers,
                actor_dispatcher=state.actor_dispatcher.set(reply.ref, dispatcher.ref),
                index=(state.index + 1) % state.max_threads,
            )

            await msg.reply_to.tell(ActorSpawned(ref=reply.ref))

            return ThreadDispatcher.active(state)

        @dispatch(StopActor, namespace=dispatch_namespace)
        async def handle(msg: StopActor):
            nonlocal state

            dispatcher = state.actor_dispatcher[msg.ref]

            # FIXME: sends message to wrong dispatcher
            await dispatcher.ask(
                lambda reply_to: DispatcherWorker.StopActor(
                    reply_to=reply_to, ref=msg.ref
                )
            )

            state = attr.evolve(
                state, actor_dispatcher=state.actor_dispatcher.remove(msg.ref)
            )

            await msg.reply_to.tell(ActorStopped(ref=msg.ref))

            return ThreadDispatcher.active(state)

        @dispatch(RemoveActor, namespace=dispatch_namespace)
        async def handle(msg: RemoveActor):
            nonlocal state

            dispatcher = state.actor_dispatcher[msg.ref]

            await dispatcher.ask(
                lambda reply_to: DispatcherWorker.RemoveActor(
                    reply_to=reply_to, ref=msg.ref
                )
            )

            state = attr.evolve(
                state, actor_dispatcher=state.actor_dispatcher.remove(msg.ref)
            )

            await msg.reply_to.tell(ActorRemoved(ref=msg.ref))

            return ThreadDispatcher.active(state)

        async def f(msg):
            return await handle(msg)

        return behaviour.receive(f)
