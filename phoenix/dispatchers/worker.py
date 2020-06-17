import asyncio
import attr
from attr.validators import instance_of
from multipledispatch import dispatch
from pyrsistent import v

from phoenix import behaviour
from phoenix.actor.actor import ActorContext
from phoenix.behaviour import Behaviour
from phoenix.dispatchers.executor.actor import Executor
from phoenix.dispatchers.executor.thread import ExecutorCreated, ThreadExecutor
from phoenix.ref import Ref


class DispatcherWorker:
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
    def start() -> Behaviour:
        async def f(context: ActorContext):
            # TODO: restart threads and their actors if failure occurs
            thread = ThreadExecutor(dispatcher=context.ref, system=context.system)
            thread.daemon = True
            thread.start()
            return DispatcherWorker.waiting(
                context=context, thread=thread, requests=v()
            )

        return behaviour.setup(f)

    @staticmethod
    def waiting(context, thread: ThreadExecutor, requests) -> Behaviour:
        async def f(message):
            if isinstance(message, ExecutorCreated):
                # now the executor is ready, process all requests that were waiting
                @dispatch(DispatcherWorker.SpawnActor)
                async def _handle(msg: DispatcherWorker.SpawnActor):
                    reply = await message.ref.ask(
                        lambda reply_to: Executor.SpawnActor(
                            reply_to=reply_to,
                            id=msg.id,
                            behaviour=msg.behaviour,
                            parent=msg.parent,
                        )
                    )
                    await msg.reply_to.tell(
                        DispatcherWorker.ActorSpawned(ref=reply.ref)
                    )

                @dispatch(DispatcherWorker.StopActor)
                async def _handle(msg: DispatcherWorker.StopActor):
                    await message.ref.ask(
                        lambda reply_to: Executor.StopActor(
                            reply_to=reply_to, ref=msg.ref
                        )
                    )
                    await msg.reply_to.tell(DispatcherWorker.ActorStopped(ref=msg.ref))

                @dispatch(DispatcherWorker.RemoveActor)
                async def _handle(msg: DispatcherWorker.RemoveActor):
                    await message.ref.ask(
                        lambda reply_to: Executor.RemoveActor(
                            reply_to=reply_to, ref=msg.ref
                        )
                    )
                    await msg.reply_to.tell(DispatcherWorker.ActorRemoved(ref=msg.ref))

                aws = [_handle(request) for request in requests]
                await asyncio.gather(*aws)
                return DispatcherWorker.active(
                    context=context, thread=thread, executor=message.ref
                )
            else:
                # executor is not ready yet, so queue up the request
                return DispatcherWorker.waiting(
                    context=context, thread=thread, requests=requests.append(message)
                )

        return behaviour.receive(f)

    @staticmethod
    def active(context, thread: ThreadExecutor, executor: Ref) -> Behaviour:
        @dispatch(DispatcherWorker.SpawnActor)
        async def worker_dispatcher_handle(msg: DispatcherWorker.SpawnActor):
            print(f"[{context.ref}] [{msg}] executor={executor}")
            reply = await executor.ask(
                lambda reply_to: Executor.SpawnActor(
                    reply_to=reply_to,
                    id=msg.id,
                    behaviour=msg.behaviour,
                    parent=msg.parent,
                )
            )
            await msg.reply_to.tell(DispatcherWorker.ActorSpawned(ref=reply.ref))

        @dispatch(DispatcherWorker.StopActor)
        async def worker_dispatcher_handle(msg: DispatcherWorker.StopActor):
            print(
                f"[{context.ref}] [{msg}] executor={executor} inbox={context.ref.inbox}"
            )
            await executor.ask(
                lambda reply_to: Executor.StopActor(reply_to=reply_to, ref=msg.ref)
            )
            await msg.reply_to.tell(DispatcherWorker.ActorStopped(ref=msg.ref))

        @dispatch(DispatcherWorker.RemoveActor)
        async def worker_dispatcher_handle(msg: DispatcherWorker.RemoveActor):
            await executor.ask(
                lambda reply_to: Executor.RemoveActor(reply_to=reply_to, ref=msg.ref)
            )
            await msg.reply_to.tell(DispatcherWorker.ActorRemoved(ref=msg.ref))

        async def f(msg):
            await worker_dispatcher_handle(msg)
            return behaviour.same()

        return behaviour.receive(f)
