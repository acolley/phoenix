import asyncio
from asyncio import Queue, Task
import attr
from attr.validators import instance_of, optional
from datetime import timedelta
import janus
import logging
from multipledispatch import dispatch
from pyrsistent import PRecord, field, m
import threading
import traceback
from typing import Any, Callable, Generic, Optional, TypeVar
import uuid

from phoenix import behaviour
from phoenix.actor import ActorCell, ActorContext
from phoenix.behaviour import Behaviour
from phoenix.dispatchers import ThreadDispatcher
from phoenix.ref import Ref
from phoenix.system.messages import ActorSpawned, SpawnActor

logger = logging.getLogger(__name__)


async def system(user: Behaviour):

    # @attr.s
    # class TimerTimedOut:
    #     ref: Ref = attr.ib(validator=instance_of(Ref))
    #     key: str = attr.ib(validator=instance_of(str))

    # @attr.s
    # class StartSingleShotTimer:
    #     ref: Ref = attr.ib(validator=instance_of(Ref))
    #     message: Any = attr.ib()
    #     interval: timedelta = attr.ib(validator=instance_of(timedelta))
    #     key: Optional[str] = attr.ib(validator=optional(instance_of(str)))

    # @attr.s
    # class StartFixedDelayTimer:
    #     ref: Ref = attr.ib(validator=instance_of(Ref))
    #     message: Any = attr.ib()
    #     interval: timedelta = attr.ib(validator=instance_of(timedelta))
    #     key: Optional[str] = attr.ib(validator=optional(instance_of(str)))

    # @attr.s
    # class CancelTimer:
    #     ref: Ref = attr.ib(validator=instance_of(Ref))
    #     key: str = attr.ib(validator=instance_of(str))

    # @attr.s
    # class Timer:
    #     ref: Ref = attr.ib(validator=instance_of(Ref))
    #     key: str = attr.ib(validator=instance_of(str))
    #     interval: timedelta = attr.ib(validator=instance_of(timedelta))
    #     resolution: timedelta = attr.ib(
    #         validator=instance_of(timedelta), default=timedelta(milliseconds=100)
    #     )

    #     async def run(self):
    #         remaining = self.interval
    #         while remaining.total_seconds() > 0:
    #             # FIXME: will result in clock drift
    #             await asyncio.sleep(self.resolution.total_seconds())
    #             remaining = remaining - self.resolution
    #         return TimerTimedOut(ref=self.ref, key=self.key)

    # @attr.s
    # class SingleShotTimer:
    #     message: Any = attr.ib()
    #     timer: Timer = attr.ib()
    #     task: Task = attr.ib(validator=instance_of(Task))

    # @attr.s
    # class FixedDelayTimer:
    #     message: Any = attr.ib()
    #     timer: Timer = attr.ib()
    #     task: Task = attr.ib(validator=instance_of(Task))

    # scheduler = Queue()

    # async def schedule():
    #     timers = {}

    #     @dispatch(StartSingleShotTimer)
    #     async def scheduler_handle(message: StartSingleShotTimer):
    #         # TODO: implement timers using asyncio.get_event_loop().call_later(...)
    #         key = message.key or str(uuid.uuid1())
    #         timer = Timer(ref=message.ref, key=key, interval=message.interval)
    #         task = asyncio.create_task(timer.run())
    #         timer = SingleShotTimer(message=message.message, task=task, timer=timer)
    #         timers[message.ref][key] = timer
    #         scheduler.task_done()
    #         logger.debug("Timer started: %s", timer)

    #     @dispatch(StartFixedDelayTimer)
    #     async def scheduler_handle(message: StartFixedDelayTimer):
    #         # TODO: implement timers using asyncio.get_event_loop().call_later(...)
    #         key = message.key or str(uuid.uuid1())
    #         timer = Timer(ref=message.ref, key=key, interval=message.interval)
    #         task = asyncio.create_task(timer.run())
    #         timer = FixedDelayTimer(message=message.message, task=task, timer=timer)
    #         timers[message.ref][key] = timer
    #         scheduler.task_done()
    #         logger.debug("Timer started: %s", timer)

    #     @dispatch(CancelTimer)
    #     async def scheduler_handle(message: CancelTimer):
    #         timer = timers[message.ref].pop(message.key)
    #         timer.task.cancel()
    #         scheduler.task_done()
    #         logger.debug("Timer cancelled: %s", timer)

    #     @dispatch(ActorSpawned)
    #     async def scheduler_handle(message: ActorSpawned):
    #         timers[message.ref] = {}
    #         scheduler.task_done()
    #         logger.debug("Actor timers added: %s", message.ref)

    #     @dispatch((ActorFailed, ActorKilled, ActorRestarted, ActorStopped))
    #     async def scheduler_handle(message):
    #         actor_timers = timers.pop(message.ref)
    #         for timer in actor_timers:
    #             timer.task.cancel()
    #         scheduler.task_done()
    #         logger.debug("Actor timers removed: %s", message.ref)

    #     @dispatch(TimerTimedOut)
    #     async def scheduler_handle(message: TimerTimedOut):
    #         timer = timers[message.ref][message.key]
    #         if isinstance(timer, SingleShotTimer):
    #             del timers[message.ref][message.key]
    #         elif isinstance(timer, FixedDelayTimer):
    #             task = asyncio.create_task(timer.timer.run())
    #             timers[message.ref][message.key] = attr.evolve(timer, task=task)
    #         await timer.timer.ref.tell(timer.message)
    #         logger.debug("Timer message sent: %s %s", message.ref, timer.message)

    #     while True:
    #         aws = [scheduler.get()] + [timer.task for actor_timers in timers.values() for timer in actor_timers.values()]
    #         for coro in asyncio.as_completed(aws):
    #             message = await coro
    #             logger.debug("Scheduler received: %s", message)
    #             await scheduler_handle(message)
    #             break

    # supervisor = Queue()

    # async def spawn(parent: Optional[Ref], inbox: Queue, start: Behaviour, id: Optional[str] = None) -> Ref:
    #     id = id or str(uuid.uuid1())
    #     ref = Ref(id=id, inbox=inbox)
    #     await supervisor.put(ActorSpawned(parent=parent, behaviour=start, ref=ref))
    #     return ref

    # async def supervise():
    #     """
    #     Manage the Actor's execution lifecycle.
    #     """
    #     actors = {}

    #     async def execute(
    #         parent: Optional[Ref], ref: Ref, behaviour: Behaviour
    #     ) -> Actor:
    #         executor = Executor(behaviour, ref)

    #         task = asyncio.create_task(executor.run())
    #         return Actor(parent=parent, behaviour=behaviour, ref=ref, task=task)

    #     @dispatch(ActorSpawned)
    #     async def supervisor_handle(msg: ActorSpawned):
    #         actor = await execute(
    #             parent=msg.parent, ref=msg.ref, behaviour=msg.behaviour
    #         )
    #         actors[msg.ref] = actor
    #         supervisor.task_done()

    #     @dispatch(ActorFailed)
    #     async def supervisor_handle(msg: ActorFailed):
    #         actor = actors.pop(msg.ref)
    #         # TODO: Notify parent

    #         # Kill children
    #         children = [x for x in actors.values() if x.parent is actor.ref]
    #         for child in children:
    #             child.task.cancel()

    #     @dispatch(ActorKilled)
    #     async def supervisor_handle(msg: ActorKilled):
    #         actor = actors.pop(msg.ref)

    #         # Kill children
    #         children = [x for x in actors.values() if x.parent is actor.ref]
    #         for child in children:
    #             child.task.cancel()

    #     @dispatch(ActorRestarted)
    #     async def supervisor_handle(msg: ActorRestarted):
    #         actor = actors[msg.ref]
    #         # Do this before re-execution to ensure
    #         # cleanup is done before the new Actor
    #         # interacts with any timers.
    #         actor = await execute(
    #             parent=actor.parent, ref=actor.ref, behaviour=msg.behaviour
    #         )
    #         actors[actor.ref] = actor

    #     @dispatch(ActorStopped)
    #     async def supervisor_handle(msg: ActorStopped):
    #         del actors[msg.ref]

    #     while True:
    #         aws = [supervisor.get()] + [x.task for x in actors.values()]
    #         for coro in asyncio.as_completed(aws):
    #             result = await coro
    #             logger.debug("Supervisor received: %s", result)
    #             # Notify scheduler of lifecycle events
    #             await scheduler.put(result)
    #             await supervisor_handle(result)
    #             break

    def active(dispatchers) -> Behaviour:
        @dispatch(SpawnActor)
        async def handle(msg: SpawnActor):
            dispatcher = dispatchers[msg.dispatcher]
            ref = await dispatcher.ask(
                lambda reply_to: SpawnActor(
                    reply_to=reply_to,
                    id=msg.id,
                    behaviour=msg.behaviour,
                    parent=msg.parent,
                )
            )
            await msg.reply_to.tell(ActorSpawned(ref=ref))

        async def f(msg):
            await handle(msg)
            return behaviour.same()

        return behaviour.receive(f)

    def start(default_dispatcher: Ref) -> Behaviour:
        async def f(context: ActorContext):
            dispatchers = m(default=default_dispatcher)
            return active(dispatchers)

        return behaviour.receive(f)
    
    root_ref = Ref(id="root", inbox=janus.Queue())
    system_ref = Ref(id="system", inbox=janus.Queue())
    default_dispatcher_ref = Ref(id="dispatcher-default", inbox=janus.Queue())
    
    root_cell = ActorCell(
        behaviour=behaviour.ignore(),
        context=ActorContext(
            ref=root_ref,
            parent=None,
            thread=threading.current_thread(),
            loop=asyncio.get_event_loop(),
            system=system_ref,
        ),
    )

    default_dispatcher_cell = ActorCell(
        behaviour=ThreadDispatcher.start(2),
        context=ActorContext(
            ref=default_dispatcher_ref,
            parent=system_ref,
            thread=threading.current_thread(),
            loop=asyncio.get_event_loop(),
            system=system_ref,
        ),
    )
    system_cell = ActorCell(
        behaviour=start(default_dispatcher_ref),
        context=ActorContext(
            ref=system_ref,
            parent=root_ref,
            thread=threading.current_thread(),
            loop=asyncio.get_event_loop(),
            system=system_ref,  # self-reference
        ),
    )

    system_task = asyncio.create_task(system_cell.run())
    default_dispatcher_task = asyncio.create_task(default_dispatcher_cell.run())
    root_task = asyncio.create_task(root_cell.run())

    await default_dispatcher_ref.ask(
        lambda reply_to: ThreadDispatcher.SpawnActor(
            reply_to=reply_to, id="user", behaviour=user, parent=root_ref
        )
    )

    await asyncio.gather(system_task, default_dispatcher_task, root_task)
