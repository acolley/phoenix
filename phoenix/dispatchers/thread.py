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
from phoenix.behaviour import Behaviour
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
        for dispatcher in value:
            if dispatcher.ref not in self.actor_dispatcher.values():
                raise ValueError(
                    f"Dispatcher `{dispatcher.ref}` must be in actor_dispatcher mapping."
                )

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
        print(f"[{state.context.ref}] [{state.index}] {state.actor_dispatcher}")

        @dispatch(ThreadDispatcher.SpawnActor)
        async def thread_dispatcher_handle(msg: ThreadDispatcher.SpawnActor):
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
                    ),
                )
                task = asyncio.create_task(cell.run())
                dispatcher = Dispatcher(ref=ref, task=task)
                dispatchers = state.dispatchers.append(dispatcher)
            else:
                dispatchers = state.dispatchers

            print(f"[{state.context.ref}] [SpawnActor] {dispatcher.ref}")

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

            await msg.reply_to.tell(ThreadDispatcher.ActorSpawned(ref=reply.ref))

            return ThreadDispatcher.active(state)

        @dispatch(ThreadDispatcher.StopActor)
        async def thread_dispatcher_handle(msg: ThreadDispatcher.StopActor):
            nonlocal state

            print(f"[{state.context.ref}] [{msg}] {state.actor_dispatcher}")

            dispatcher = state.actor_dispatcher[msg.ref]

            print(
                f"[{state.context.ref}] [{msg}] dispatcher={dispatcher} {dispatcher.inbox}"
            )

            # FIXME: sends message to wrong dispatcher
            await dispatcher.ask(
                lambda reply_to: DispatcherWorker.StopActor(
                    reply_to=reply_to, ref=msg.ref
                )
            )

            state = attr.evolve(
                state, actor_dispatcher=state.actor_dispatcher.remove(msg.ref)
            )

            await msg.reply_to.tell(ThreadDispatcher.ActorStopped(ref=msg.ref))

            return ThreadDispatcher.active(state)

        @dispatch(ThreadDispatcher.RemoveActor)
        async def thread_dispatcher_handle(msg: ThreadDispatcher.RemoveActor):
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

            await msg.reply_to.tell(ThreadDispatcher.ActorRemoved(ref=msg.ref))

            return ThreadDispatcher.active(state)

        async def f(msg):
            return await thread_dispatcher_handle(msg)

        return behaviour.receive(f)
