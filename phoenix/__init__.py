import asyncio
from asyncio import Queue
import attr
from attr.validators import instance_of
from collections import defaultdict
from enum import Enum
import logging
from multipledispatch import dispatch
import threading
import uuid
from typing import Optional, Tuple, Union

logger = logging.getLogger(__name__)


@attr.s(frozen=True)
class ActorId:
    value: str = attr.ib(validator=instance_of(str))

    def __str__(self) -> str:
        return self.value


class Behaviour(Enum):
    done = 0
    stop = 1


@attr.s
class Stop:
    pass


@attr.s
class Error:
    exc: Exception = attr.ib(validator=instance_of(Exception))


ExitReason = Union[Stop, Error]


@attr.s
class Exit:
    id: ActorId = attr.ib(validator=instance_of(ActorId))
    reason: ExitReason = attr.ib(validator=instance_of((Stop, Error)))


async def run_actor(context, queue, start, handler) -> Tuple[ActorId, ExitReason]:
    try:
        state = await start(context)
    except asyncio.CancelledError:
        # Catch all exceptions except for CancelledError
        raise
    except Exception as e:
        logger.debug("[%s]", str(context.id), exc_info=True)
        return Exit(id=context.id, reason=Error(e))
    while True:
        msg = await queue.get()
        logger.debug("[%s] Received: [%s]", str(context.id), str(msg))
        try:
            behaviour, state = await handler(state, msg)
        except asyncio.CancelledError:
            # Catch all exceptions except for CancelledError
            raise
        except Exception as e:
            logger.debug("[%s]", str(context.id), exc_info=True)
            return Exit(id=context.id, reason=Error(e))
        logger.debug("[%s] %s", str(context.id), str(behaviour))
        if behaviour == Behaviour.done:
            queue.task_done()
            continue
        elif behaviour == Behaviour.stop:
            return Exit(id=context.id, reason=Stop())
        else:
            raise ValueError(f"Unsupported behaviour: {behaviour}")


class NoSuchActor(Exception):
    pass


class ActorExists(Exception):
    pass


@attr.s
class Down:
    """
    Message sent to a watcher when the watched exits.
    """
    id: ActorId = attr.ib(validator=instance_of(ActorId))
    reason: ExitReason = attr.ib()


class ActorSystem:
    def __init__(self):
        self.event_loop = asyncio.get_running_loop()
        self.actor_tasks = {}
        self.actor_queues = {}
        self.watchers = defaultdict(list)
        self.spawned = asyncio.Event()

    async def run(self):
        while True:
            aws = list(self.actor_tasks.values()) + [self.spawned.wait()]
            for coro in asyncio.as_completed(aws):
                result = await coro
                if isinstance(result, Exit):
                    del self.actor_tasks[result.id]
                    del self.actor_queues[result.id]
                    logger.debug("[%s] Actor Removed", str(result.id))
                    watchers = self.watchers.pop(result.id, [])
                    for watcher in watchers:
                        await self.cast(watcher, Down(id=result.id, reason=result.reason))
                else:
                    self.spawned.clear()
                break

    async def spawn(self, start, handler, name=None) -> str:
        """
        Note: not thread-safe.
        """
        id = ActorId(name or str(uuid.uuid1()))
        if id in self.actor_tasks:
            raise ActorExists(id)
        queue = Queue()
        context = Context(id=id, system=self)
        task = self.event_loop.create_task(run_actor(context, queue, start, handler))
        self.actor_tasks[id] = task
        self.actor_queues[id] = queue
        self.spawned.set()
        logger.debug("[%s] Actor Spawned", str(id))
        return id
    
    def watch(self, watcher: ActorId, watched: ActorId):
        """
        Note: not thread-safe.
        """
        if watched not in self.actor_tasks:
            raise NoSuchActor(watched)
        self.watchers[watched].append(watcher)
    
    async def cast(self, id, msg):
        """
        Note: not thread-safe.
        """
        try:
            queue = self.actor_queues[id]
        except KeyError:
            raise NoSuchActor(id)
        await queue.put(msg)
    
    async def call(self, id, f):
        """
        Note: not thread-safe.
        """
        try:
            queue = self.actor_queues[id]
        except KeyError:
            raise NoSuchActor(id)
        
        async def _start(ctx):
            return None
        
        reply = None
        received = asyncio.Event()
        async def _handle(state: None, msg):
            nonlocal reply
            reply = msg
            received.set()
            return Behaviour.stop, state

        reply_to = await self.spawn(_start, _handle, name=f"{id}->{uuid.uuid1()}")
        msg = f(reply_to)
        await queue.put(msg)
        await received.wait()
        return reply
    
    async def shutdown(self):
        for task in list(self.actor_tasks.values()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


@attr.s
class Context:
    """
    A handle to the runtime context of an actor.
    """
    id: ActorId = attr.ib(validator=instance_of(ActorId))
    system = attr.ib()

    async def cast(self, id, msg):
        await self.system.cast(id, msg)
    
    async def call(self, id, msg):
        return await self.system.call(id, msg)
    
    async def spawn(self, start, handle, name=None) -> ActorId:
        return await self.system.spawn(start, handle, name=name)
    
    def watch(self, id: ActorId):
        self.system.watch(self.id, id)
