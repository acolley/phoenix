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
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    reason: ExitReason = attr.ib(validator=instance_of((Stop, Error)))


async def run_actor(context, queue, start, handler) -> Tuple[ActorId, ExitReason]:
    logger.debug("[%s] Starting", str(context.actor_id))
    try:
        state = await start(context)
    except asyncio.CancelledError:
        # Catch all exceptions except for CancelledError
        raise
    except Exception as e:
        logger.debug("[%s] Start", str(context.actor_id), exc_info=True)
        return Exit(actor_id=context.actor_id, reason=Error(e))
    while True:
        msg = await queue.get()
        logger.debug("[%s] Received: [%s]", str(context.actor_id), str(msg))
        try:
            behaviour, state = await handler(state, msg)
        except asyncio.CancelledError:
            # Catch all exceptions except for CancelledError
            raise
        except Exception as e:
            logger.debug("[%s] %s", str(context.actor_id), str(msg), exc_info=True)
            return Exit(actor_id=context.actor_id, reason=Error(e))
        finally:
            queue.task_done()
        logger.debug("[%s] %s", str(context.actor_id), str(behaviour))
        if behaviour == Behaviour.done:
            continue
        elif behaviour == Behaviour.stop:
            return Exit(actor_id=context.actor_id, reason=Stop())
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
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    reason: ExitReason = attr.ib()


@attr.s
class Actor:
    task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))
    queue: asyncio.Queue = attr.ib(validator=instance_of(asyncio.Queue))


class ActorSystem:
    def __init__(self):
        self.event_loop = asyncio.get_running_loop()
        self.actors = {}
        self.watchers = defaultdict(list)
        self.spawned = asyncio.Event()

    async def run(self):
        while True:
            aws = [actor.task for actor in self.actors.values()] + [self.spawned.wait()]
            for coro in asyncio.as_completed(aws):
                result = await coro
                if isinstance(result, Exit):
                    del self.actors[result.actor_id]
                    logger.debug("[%s] Actor Removed; Reason: [%s]", str(result.actor_id), str(result.reason))
                    watchers = self.watchers.pop(result.actor_id, [])
                    for watcher in watchers:
                        await self.cast(watcher, Down(actor_id=result.actor_id, reason=result.reason))
                else:
                    self.spawned.clear()
                break

    async def spawn(self, start, handler, name=None) -> ActorId:
        """
        Note: not thread-safe.
        """
        actor_id = ActorId(name or str(uuid.uuid1()))
        logger.debug("[%s] Spawn Actor", str(actor_id))
        if actor_id in self.actors:
            raise ActorExists(actor_id)
        # TODO: encourage bounded queues for back pressure to prevent overload
        queue = Queue()
        context = Context(actor_id=actor_id, system=self)
        task = self.event_loop.create_task(run_actor(context, queue, start, handler))
        actor = Actor(task=task, queue=queue)
        self.actors[actor_id] = actor
        self.spawned.set()
        logger.debug("[%s] Actor Spawned", str(actor_id))
        return actor_id
    
    def watch(self, watcher: ActorId, watched: ActorId):
        """
        Note: not thread-safe.
        """
        if watched not in self.actors:
            raise NoSuchActor(watched)
        self.watchers[watched].append(watcher)
    
    async def cast(self, actor_id: ActorId, msg):
        """
        Note: not thread-safe.
        """
        try:
            actor = self.actors[actor_id]
        except KeyError:
            raise NoSuchActor(actor_id)
        await actor.queue.put(msg)
    
    async def call(self, actor_id: ActorId, f):
        """
        Note: not thread-safe.
        """
        try:
            actor = self.actors[actor_id]
        except KeyError:
            raise NoSuchActor(actor_id)
        
        async def _start(ctx):
            return None
        
        reply = None
        received = asyncio.Event()
        async def _handle(state: None, msg):
            nonlocal reply
            reply = msg
            received.set()
            return Behaviour.stop, state

        reply_to = await self.spawn(_start, _handle, name=f"{actor_id}->{uuid.uuid1()}")
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
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    system = attr.ib()

    async def cast(self, id, msg):
        await self.system.cast(id, msg)
    
    async def call(self, id, msg):
        return await self.system.call(id, msg)
    
    async def spawn(self, start, handle, name=None) -> ActorId:
        return await self.system.spawn(start, handle, name=name)
    
    def watch(self, actor_id: ActorId):
        self.system.watch(self.actor_id, actor_id)
