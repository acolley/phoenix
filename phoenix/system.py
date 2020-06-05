import asyncio
from asyncio import Queue, Task
import attr
from attr.validators import instance_of, optional
from datetime import timedelta
import logging
from multipledispatch import dispatch
from pyrsistent import PRecord, field
import traceback
from typing import Any, Callable, Generic, Optional, TypeVar
import uuid

from phoenix.actor import Ref
from phoenix.behaviour import (
    Behaviour,
    Ignore,
    Receive,
    Restart,
    Same,
    Schedule,
    Setup,
    Stop,
)

logger = logging.getLogger(__name__)


async def system(user: Behaviour):
    class ActorFailed(PRecord):
        ref: Ref = field(type=Ref)
        exc: Exception = field(type=Exception)

    class ActorStopped(PRecord):
        ref: Ref = field(type=Ref)

    class ActorKilled(PRecord):
        ref: Ref = field(type=Ref)

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

    @attr.s
    class TimerTimedOut:
        ref: Ref = attr.ib(validator=instance_of(Ref))
        key: str = attr.ib(validator=instance_of(str))

    @attr.s
    class StartSingleShotTimer:
        ref: Ref = attr.ib(validator=instance_of(Ref))
        message: Any = attr.ib()
        interval: timedelta = attr.ib(validator=instance_of(timedelta))
        key: Optional[str] = attr.ib(validator=optional(instance_of(str)))

    @attr.s
    class StartFixedDelayTimer:
        ref: Ref = attr.ib(validator=instance_of(Ref))
        message: Any = attr.ib()
        interval: timedelta = attr.ib(validator=instance_of(timedelta))
        key: Optional[str] = attr.ib(validator=optional(instance_of(str)))

    @attr.s
    class CancelTimer:
        ref: Ref = attr.ib(validator=instance_of(Ref))
        key: str = attr.ib(validator=instance_of(str))

    @attr.s
    class Timer:
        ref: Ref = attr.ib(validator=instance_of(Ref))
        key: str = attr.ib(validator=instance_of(str))
        interval: timedelta = attr.ib(validator=instance_of(timedelta))
        resolution: timedelta = attr.ib(
            validator=instance_of(timedelta), default=timedelta(milliseconds=100)
        )

        async def run(self):
            remaining = self.interval
            while remaining.total_seconds() > 0:
                # FIXME: will result in clock drift
                await asyncio.sleep(self.resolution.total_seconds())
                remaining = remaining - self.resolution
            return TimerTimedOut(ref=self.ref, key=self.key)

    @attr.s
    class SingleShotTimer:
        message: Any = attr.ib()
        timer: Timer = attr.ib()
        task: Task = attr.ib(validator=instance_of(Task))

    @attr.s
    class FixedDelayTimer:
        message: Any = attr.ib()
        timer: Timer = attr.ib()
        task: Task = attr.ib(validator=instance_of(Task))

    scheduler = Queue()

    async def schedule():
        timers = {}

        @dispatch(StartSingleShotTimer)
        async def scheduler_handle(message: StartSingleShotTimer):
            key = message.key or str(uuid.uuid1())
            timer = Timer(ref=message.ref, key=key, interval=message.interval)
            task = asyncio.create_task(timer.run())
            # Since this is not async it will block the thread, guaranteeing
            # that we do not get a race condition.
            timer = SingleShotTimer(message=message.message, task=task, timer=timer)
            timers[message.ref][key] = timer
            scheduler.task_done()
            logger.debug("Timer started: %s", timer)

        @dispatch(StartFixedDelayTimer)
        async def scheduler_handle(message: StartFixedDelayTimer):
            key = message.key or str(uuid.uuid1())
            timer = Timer(ref=message.ref, key=key, interval=message.interval)
            task = asyncio.create_task(timer.run())
            # Since this is not async it will block the thread, guaranteeing
            # that we do not get a race condition.
            timer = FixedDelayTimer(message=message.message, task=task, timer=timer)
            timers[message.ref][key] = timer
            scheduler.task_done()
            logger.debug("Timer started: %s", timer)

        @dispatch(CancelTimer)
        async def scheduler_handle(message: CancelTimer):
            timer = timers[message.ref].pop(message.key)
            timer.task.cancel()
            scheduler.task_done()

        @dispatch(ActorSpawned)
        async def scheduler_handle(message: ActorSpawned):
            timers[message.ref] = {}
            scheduler.task_done()

        @dispatch((ActorFailed, ActorKilled, ActorRestarted, ActorStopped))
        async def scheduler_handle(message):
            actor_timers = timers.pop(message.ref)
            for timer in actor_timers:
                timer.task.cancel()
            scheduler.task_done()

        @dispatch(TimerTimedOut)
        async def scheduler_handle(message: TimerTimedOut):
            timer = timers[message.ref][message.key]
            if isinstance(timer, SingleShotTimer):
                del timers[message.ref][message.key]
            elif isinstance(timer, FixedDelayTimer):
                task = asyncio.create_task(timer.timer.run())
                timers[message.ref][message.key] = attr.evolve(timer, task=task)
            await timer.timer.ref.tell(timer.message)

        while True:
            aws = [scheduler.get()] + [timer.task for actor_timers in timers.values() for timer in actor_timers.values()]
            for coro in asyncio.as_completed(aws):
                message = await coro
                logger.debug("Scheduler received: %s", message)
                await scheduler_handle(message)
                break

    @attr.s
    class Timers:
        ref: Ref = attr.ib(validator=instance_of(Ref))

        async def start_singleshot_timer(
            self, message: Any, interval: timedelta, key: Optional[str] = None
        ):
            await scheduler.put(
                StartSingleShotTimer(
                    ref=self.ref, message=message, interval=interval, key=key
                )
            )

        async def start_fixed_delay_timer(
            self, message: Any, interval: timedelta, key: Optional[str] = None
        ):
            await scheduler.put(
                StartFixedDelayTimer(
                    ref=self.ref, message=message, interval=interval, key=key
                )
            )

        async def cancel(self, key: str):
            await scheduler.put(CancelTimer(ref=self.ref, key=key))

    class Executor:
        """
        Responsible for executing the behaviours returned
        by the actor's starting behaviour.
        """

        def __init__(self, start: Behaviour, ref: Ref):
            self.ref = ref
            self.start = start

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
            # FIXME: behaviours can result in blowing out the stack.

            try:
                await self.execute(self.start)
            except RestartActor as e:
                return ActorRestarted(ref=self.ref, behaviour=e.behaviour)
            except StopActor:
                return ActorStopped(ref=self.ref)
            except asyncio.CancelledError:
                # We were cancelled by the supervisor
                # TODO: Allow the actor to perform cleanup.
                return ActorKilled(ref=self.ref)
            except Exception as e:
                logger.debug("Behaviour raised unhandled exception", exc_info=True)
                return ActorFailed(ref=self.ref, exc=e)

        @dispatch(Setup)
        async def execute(self, behaviour: Setup):
            logger.debug("Executing %s", behaviour)

            async def _spawn(factory: Callable[[], Callable[[], Behaviour]]) -> Ref:
                return await spawn(self.ref, Queue(), factory)

            await self.execute(await behaviour(_spawn))

        @dispatch(Receive)
        async def execute(self, behaviour: Receive):
            logger.debug("Executing %s", behaviour)
            message = await self.ref.inbox.get()
            logger.debug("Message received: %s: %s", self.ref, message)
            next_ = await behaviour(message)
            self.ref.inbox.task_done()
            await self.execute(next_)

        @dispatch(Ignore)
        async def execute(self, behaviour: Ignore):
            logger.debug("Executing %s", behaviour)
            try:
                self.ref.inbox.get_nowait()
            except asyncio.QueueEmpty:
                pass
            else:
                self.ref.inbox.task_done()
            return

        @dispatch(Restart)
        async def execute(self, behaviour: Restart):
            logger.debug("Executing %s", behaviour)
            try:
                await self.execute(behaviour.behaviour)
            except asyncio.CancelledError:
                raise
            except Exception:
                if behaviour.restarts >= behaviour.max_restarts:
                    raise

                logger.debug("Restart behaviour caught exception", exc_info=True)

                if behaviour.backoff:
                    await asyncio.sleep(behaviour.backoff(behaviour.restarts))

                raise RestartActor(attr.evolve(behaviour, restarts=behaviour.restarts + 1))

        @dispatch(Schedule)
        async def execute(self, behaviour: Schedule):
            logger.debug("Executing %s", behaviour)
            await self.execute(await behaviour(Timers(self.ref)))

        @dispatch(Same)
        async def execute(self, behaviour: Same):
            logger.debug("Executing %s", behaviour)
            # FIXME: executing Same does not work in this implementation
            return

        @dispatch(Stop)
        async def execute(self, behaviour: Stop):
            logger.debug("Executing %s", behaviour)
            raise StopActor

    @attr.s
    class Actor:
        parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
        behaviour: Behaviour = attr.ib()
        ref: Ref = attr.ib(validator=instance_of(Ref))
        task: Task = attr.ib(validator=instance_of(Task))

    supervisor = Queue()

    async def spawn(parent: Optional[Ref], inbox: Queue, behaviour: Behaviour) -> Ref:
        ref = Ref(inbox=inbox)
        await supervisor.put(ActorSpawned(parent=parent, behaviour=behaviour, ref=ref))
        return ref

    async def supervise():
        """
        Manage the Actor's execution lifecycle.
        """
        actors = {}

        async def execute(
            parent: Optional[Ref], ref: Ref, behaviour: Behaviour
        ) -> Actor:
            executor = Executor(behaviour, ref)
            task = asyncio.create_task(executor.run())
            return Actor(parent=parent, behaviour=behaviour, ref=ref, task=task)

        @dispatch(ActorSpawned)
        async def supervisor_handle(msg: ActorSpawned):
            actor = await execute(
                parent=msg.parent, ref=msg.ref, behaviour=msg.behaviour
            )
            actors[msg.ref] = actor
            supervisor.task_done()

        @dispatch(ActorFailed)
        async def supervisor_handle(msg: ActorFailed):
            actor = actors.pop(msg.ref)
            # TODO: Notify parent

            # Kill children
            children = [x for x in actors.values() if x.parent is actor.ref]
            for child in children:
                child.task.cancel()

        @dispatch(ActorKilled)
        async def supervisor_handle(msg: ActorKilled):
            actor = actors.pop(msg.ref)

            # Kill children
            children = [x for x in actors.values() if x.parent is actor.ref]
            for child in children:
                child.task.cancel()

        @dispatch(ActorRestarted)
        async def supervisor_handle(msg: ActorRestarted):
            actor = actors[msg.ref]
            # Do this before re-execution to ensure
            # cleanup is done before the new Actor
            # interacts with any timers.
            actor = await execute(
                parent=actor.parent, ref=actor.ref, behaviour=msg.behaviour
            )
            actors[actor.ref] = actor

        @dispatch(ActorStopped)
        async def supervisor_handle(msg: ActorStopped):
            del actors[msg.ref]

        while True:
            aws = [supervisor.get()] + [x.task for x in actors.values()]
            for coro in asyncio.as_completed(aws):
                result = await coro
                logger.debug("Supervisor received: %s", result)
                # Notify scheduler of lifecycle events
                await scheduler.put(result)
                await supervisor_handle(result)
                break

    await spawn(None, Queue(), user)

    await asyncio.gather(asyncio.create_task(supervise()), asyncio.create_task(schedule()))
