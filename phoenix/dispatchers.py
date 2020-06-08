import asyncio
import attr
from attr.validators import instance_of, optional
import concurrent.futures
import janus
import logging
from multipledispatch import dispatch
import queue
import threading
import time
from typing import Any, List, Optional
import uuid

from phoenix.actor import Ref
from phoenix.behaviour import Behaviour, Ignore, Schedule, Receive, Restart, Same, Setup, Stop

logger = logging.getLogger(__name__)


# @attr.s
# class Actor:
#     parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
#     behaviour: Behaviour = attr.ib()
#     ref: Ref = attr.ib(validator=instance_of(Ref))
#     task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))


@attr.s
class ActorFailed:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    exc: Exception = attr.ib(validator=instance_of(Exception))


@attr.s
class ActorStopped:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorKilled:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorSpawned:
    parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
    behaviour: Behaviour = attr.ib()
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorRestarted:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    behaviour: Restart = attr.ib(validator=instance_of(Restart))


class RestartActor(Exception):
    def __init__(self, behaviour: Restart):
        self.behaviour = behaviour


class StopActor(Exception):
    pass


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


@attr.s(frozen=True)
class ActorContext:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    """
    The Ref of this Actor.
    """

    parent: Ref = attr.ib(validator=instance_of(Ref))

    thread: threading.Thread = attr.ib(validator=instance_of(threading.Thread))
    """
    The thread that is executing this Actor.
    """

    loop: asyncio.AbstractEventLoop = attr.ib(validator=instance_of(asyncio.AbstractEventLoop))
    """
    The event loop that is executing this Actor.
    """

    dispatcher: Ref = attr.ib(validator=instance_of(Ref))
    """
    The dispatcher that spawned this Actor.
    """

    system: Ref = attr.ib(validator=instance_of(Ref))
    """
    The system that this Actor belongs to.
    """

    async def spawn(self, behaviour: Behaviour, id: Optional[str] = None, dispatcher: Optional[str] = None) -> Ref:
        id = id or str(uuid.uuid1())
        return await self.system.ask(system.SpawnActor(id=id, behaviour=behaviour, dispatcher=dispatcher, parent=self.ref))
    

@attr.s
class Actor:
    """
    Responsible for executing the behaviours returned
    by the actor's starting behaviour.
    """
    start: Behaviour = attr.ib()
    context: ActorContext = attr.ib(validator=instance_of(ActorContext))

    @start.validator
    def check(self, attribute: str, value: Behaviour):
        if not isinstance(value, (Ignore, Receive, Restart, Schedule, Setup)):
            raise ValueError(f"Invalid start behaviour: {value}")

    async def run(self):
        """
        Execute the actor in a coroutine.

        Returns terminating behaviours for the actor
        so that the supervisor for this actor knows
        how to react to a termination.
        """

        # A stack of actor behaviours.
        # The top of the stack determines
        # what the behaviour will be on
        # the next loop cycle.
        try:
            behaviours = [self.start]
            while behaviours:
                logger.debug("[%s] Main: %s", self.context.ref, behaviours)
                current = behaviours.pop()
                next_ = await self.execute(current)
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)
        except RestartActor as e:
            return ActorRestarted(ref=self.context.ref, behaviour=e.behaviour)
        except StopActor:
            return ActorStopped(ref=self.context.ref)
        except asyncio.CancelledError:
            # We were cancelled by the supervisor
            # TODO: Allow the actor to perform cleanup.
            return ActorKilled(ref=self.context.ref)
        except Exception as e:
            logger.debug("[%s] Behaviour raised unhandled exception", self.context.ref, exc_info=True)
            return ActorFailed(ref=self.context.ref, exc=e)

    @dispatch(Setup)
    async def execute(self, behaviour: Setup):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)

        next_ = await behaviour(self.context)
        return next_

    @dispatch(Receive)
    async def execute(self, behaviour: Receive):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        message = await self.context.ref.inbox.async_q.get()
        logger.debug("[%s] Message received: %s", self.context.ref, message)
        try:
            next_ = await behaviour(message)
        finally:
            self.context.ref.inbox.async_q.task_done()
        return next_

    @dispatch(Ignore)
    async def execute(self, behaviour: Ignore):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        try:
            self.context.ref.inbox.async_q.get_nowait()
        except asyncio.QueueEmpty:
            pass
        else:
            self.context.ref.inbox.async_q.task_done()
        return behaviour.same()

    @dispatch(Restart)
    async def execute(self, behaviour: Restart):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)

        behaviours = [behaviour.behaviour]
        while behaviours:
            logger.debug("[%s] Restart: %s", self.context.ref, behaviours)
            current = behaviours.pop()
            try:
                next_ = await self.execute(current)
            except StopActor:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                if behaviour.restarts >= behaviour.max_restarts:
                    raise

                logger.debug("Restart behaviour caught exception", exc_info=True)

                if behaviour.backoff:
                    await asyncio.sleep(behaviour.backoff(behaviour.restarts))

                raise RestartActor(attr.evolve(behaviour, restarts=behaviour.restarts + 1))
            else:
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)

    @dispatch(Schedule)
    async def execute(self, behaviour: Schedule):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        return await behaviour(Timers(self.context.ref))

    @dispatch(Stop)
    async def execute(self, behaviour: Stop):
        logger.debug("[%s] Executing %s", self.context.ref, behaviour)
        raise StopActor


@attr.s
class ActorCell:
    """
    Manage Actor lifecycle.
    """
    behaviour: Behaviour = attr.ib()
    context: ActorContext = attr.ib(validator=instance_of(ActorContext))
    _actor: Actor = attr.ib(init=False, validator=instance_of(Actor), default=None)
    _task: asyncio.Task = attr.ib(init=False, validator=instance_of(asyncio.Task), default=None)

    async def run(self):
        self._actor = Actor(start=self.behaviour, context=self.context)
        self._task = asyncio.create_task(self._actor.run())

        while True:
            result = await self._task
            await self.handle(result)
    
    @dispatch(ActorRestarted)
    async def handle(self, msg: ActorRestarted):
        self._task = asyncio.create_task(self._actor.run())

    @dispatch(ActorFailed)
    async def handle(self, msg: ActorFailed):
        actor = self._actors.pop(msg.ref.id)
        # TODO: Notify parent

        for child in actor.children:
            child.task.cancel()


async def bootstrap_execute_actor(id: str, behaviour: Behaviour, spawner: Optional[Ref] = None) -> Actor:
    ref = Ref(id=id, inbox=janus.Queue())
    actor = ActorExecutor(start=behaviour, ref=ref, spawner=spawner)
    task = asyncio.create_task(actor.run())
    return Actor(parent=None, behaviour=behaviour, ref=ref, task=task)


class Spawner:

    @attr.s
    class SpawnActor:
        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        id: str = attr.ib(validator=instance_of(str))
        behaviour: Behaviour = attr.ib()
    
    @attr.s
    class ActorSpawned:
        ref: Ref = attr.ib(validator=instance_of(Ref))

    @staticmethod
    def start(executor: asyncio.Queue) -> Behaviour:
        async def f(context):
            return Spawner.active(context.ref, executor)
        return behaviour.setup(f)

    @staticmethod
    def active(myref: Ref, executor: asyncio.Queue) -> Behaviour:
        async def f(message: Spawner.SpawnActor):
            actor = await bootstrap_execute_actor(id=message.id, behaviour=message.behaviour, spawner=myref)
            await executor.put(ThreadExecutor.ActorExecuted(actor=actor))
            await message.reply_to.tell(Spawner.ActorSpawned(ref=actor.ref))
            return behaviour.same()
        return behaviour.receive(f)


class ThreadExecutor(threading.Thread):

    @attr.s
    class ActorExecuted:
        actor: Actor = attr.ib(validator=instance_of(Actor))

    @attr.s
    class SpawnerCreated:
        executor: ThreadExecutor = attr.ib(validator=instance_of(ThreadExecutor))
        ref: Ref = attr.ib(validator=instance_of(Ref))

    def __init__(self, dispatcher: Ref):
        super(ThreadExecutor, self).__init__()

        self.dispatcher = dispatcher
        self._actors = {}

    def run(self):
        logger.debug("Starting ThreadExecutor on thread %s", threading.current_thread())
        async def _run():
            # Must be created here as we are in a new thread
            # and the queue will now be tied to this thread's
            # async loop.
            inbox = asyncio.Queue()

            # Bootstrap an actor whose job will be spawning
            # other actors into this thread.
            spawner = await bootstrap_execute_actor(id=str(uuid.uuid1()), behaviour=Spawner.start(inbox), spawner=None)
            self._actors[spawner.ref.id] = spawner
            await self.dispatcher.tell(self.SpawnerCreated(executor=self, ref=spawner.ref))

            while True:
                aws = [inbox.get()] + [x.task for x in self._actors.values()]
                for coro in asyncio.as_completed(aws):
                    msg = await coro
                    await self.handle(msg)
                    break
            
        asyncio.run(_run())
    
    @dispatch(ActorExecuted)
    async def handle(self, msg: ActorExecuted):
        self._actors[msg.actor.ref.id] = msg.actor
    
    @dispatch(ActorFailed)
    async def handle(self, msg: ActorFailed):
        actor = self._actors.pop(msg.ref.id)
        # TODO: Notify parent

        for child in actor.children:
            child.task.cancel()

        # Kill children
        # children = [x for x in actors.values() if x.parent is actor.ref]
        # for child in children:
        #     child.task.cancel()



class ThreadDispatcher:
    
    @attr.s
    class SpawnActor:
        reply_to: Ref = attr.ib(validator=instance_of(Ref))
        id: str = attr.ib(validator=instance_of(str))
        behaviour: Behaviour = attr.ib()
    
    @attr.s
    class ActorSpawned:
        ref: Ref = attr.ib(validator=instance_of(Ref))
    
    @attr.s
    class NotReady:
        """
        Not ready to handle spawn requests.
        """
        pass

    @staticmethod
    def start(max_threads: int) -> Behaviour:
        async def f(context):
            executor = ThreadExecutor(dispatcher=context.ref)
            executor.daemon = True
            executor.start()
            return ThreadDispatcher.waiting(executor)
        return behaviour.setup(f)
    
    @staticmethod
    def waiting(executor: ThreadExecutor) -> Behaviour:
        async def f(message):
            if isinstance(message, ThreadExecutor.SpawnerCreated):
                return ThreadDispatcher.active(executor=executor, spawner=message.ref)
            else:
                await message.reply_to.tell(ThreadDispatcher.NotReady())
                return behaviour.same()
        return behaviour.receive(f)
    
    @staticmethod
    def active(executor: ThreadExecutor, spawner: Ref) -> Behaviour:
        async def f(message: ThreadDispatcher.SpawnActor):
            reply = await spawner.ask(lambda reply_to: Spawner.SpawnActor(reply_to=reply_to, id=message.id, behaviour=message.behaviour))
            await message.reply_to.tell(ThreadDispatcher.ActorSpawned(ref=reply.ref))
            return behaviour.same()
        return behaviour.receive(f)


from datetime import timedelta
from phoenix import behaviour

class Greeter:
    class Timeout:
        pass

    @staticmethod
    def start(greeting: str) -> Behaviour:
        async def f(message):
            print(f"{greeting} {message}")
            await asyncio.sleep(1)
            return behaviour.same()
        return behaviour.receive(f)
        # return Greeter.init(greeting, 0)

    @staticmethod
    def init(greeting: str, count: int) -> Behaviour:
        async def f(timers):
            await timers.start_fixed_delay_timer(Greeter.Timeout, timedelta(seconds=1))
            return Greeter.active(greeting, count)

        return behaviour.restart(behaviour.schedule(f))

    @staticmethod
    def active(greeting: str, count: int) -> Behaviour:
        async def f(message: Greeter.Timeout):
            print(f"{greeting} {count}")
            if count > 5:
                raise Exception("Boooooom!!!")
            return Greeter.active(greeting, count + 1)

        return behaviour.receive(f)


class App:

    @staticmethod
    def start() -> Behaviour:
        async def f(context: ActorContext):
            greeter = await context.spawn(Greeter.start("Hello"))
            greeter.tell("Alasdair")
            return behaviour.ignore()
        return behaviour.setup(f)


async def main_async():
    dispatcher = await bootstrap_execute_actor(id="dispatcher", behaviour=ThreadDispatcher.start(1), spawner=None)
    while True:
        reply = await dispatcher.ref.ask(lambda reply_to: ThreadDispatcher.SpawnActor(reply_to=reply_to, id="App", behaviour=App.start()))
        print(reply)
        if isinstance(reply, ThreadDispatcher.ActorSpawned):
            break
    await dispatcher.task


def main():
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main_async())
