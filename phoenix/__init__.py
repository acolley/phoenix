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


async def run_actor(context, queue, start, handler) -> Tuple[ActorId, ExitReason]:
    try:
        state = await start(context)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.debug("[%s]", str(context.id), exc_info=True)
        return context.id, Error(e)
    while True:
        msg = await queue.get()
        logger.debug("[%s] Received: [%s]", str(context.id), str(msg))
        try:
            behaviour, state = await handler(state, msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("[%s]", str(context.id), exc_info=True)
            return context.id, Error(e)
        logger.debug("[%s] %s", str(context.id), str(behaviour))
        if behaviour == Behaviour.done:
            queue.task_done()
            continue
        elif behaviour == Behaviour.stop:
            return context.id, Stop()
        else:
            raise ValueError(f"Unsupported behaviour: {behaviour}")


@attr.s
class Actor:
    queue: asyncio.Queue = attr.ib(validator=instance_of(asyncio.Queue))
    task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))


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
        self.actors = {}
        self.watchers = defaultdict(list)

    async def run(self):
        while True:
            for coro in asyncio.as_completed([actor.task for actor in self.actors.values()]):
                id, exit_reason = await coro
                break
            
            del self.actors[id]
            logger.debug("[%s] Actor Removed", str(id))
            watchers = self.watchers.pop(id, [])
            for watcher in watchers:
                await self.cast(watcher, Down(id, reason=exit_reason))

    def spawn(self, start, handler, name=None) -> str:
        """
        Note: not thread-safe.
        """
        id = ActorId(name or str(uuid.uuid1()))
        if id in self.actors:
            raise ActorExists(id)
        queue = Queue()
        context = Context(id=id, system=self)
        task = self.event_loop.create_task(run_actor(context, queue, start, handler))
        self.actors[id] = Actor(queue=queue, task=task)
        return id
    
    def watch(self, watcher: ActorId, watched: ActorId):
        """
        Note: not thread-safe.
        """
        if watched not in self.actors:
            raise NoSuchActor(watched)
        self.watchers[watched].append(watcher)
    
    async def cast(self, id, msg):
        """
        Note: not thread-safe.
        """
        try:
            actor = self.actors[id]
        except KeyError:
            raise NoSuchActor(id)
        await actor.queue.put(msg)
    
    async def call(self, id, f):
        """
        Note: not thread-safe.
        """
        try:
            actor = self.actors[id]
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

        reply_to = self.spawn(_start, _handle, name=f"{id}->{uuid.uuid1()}")
        msg = f(reply_to)
        await actor.queue.put(msg)
        await received.wait()
        return reply
    
    async def shutdown(self):
        for actor in list(self.actors.values()):
            actor.task.cancel()
            try:
                await actor.task
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
    
    def spawn(self, start, handle, name=None) -> ActorId:
        return self.system.spawn(start, handle, name=name)
    
    def watch(self, id: ActorId):
        self.system.watch(self.id, id)
