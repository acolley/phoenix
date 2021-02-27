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
from typing import Optional

logger = logging.getLogger(__name__)


class Behaviour(Enum):
    done = 0
    stop = 1


@attr.s
class Stop:
    pass


@attr.s
class Error:
    exc: Exception = attr.ib(validator=instance_of(Exception))


@attr.s
class Context:
    """
    A handle to the runtime context of an actor.
    """
    system = attr.ib()

    async def cast(self, id, msg):
        await self.system.cast(id, msg)
    
    async def call(self, id, msg):
        return await self.system.call(id, msg)


async def run_actor(context, queue, start, handler):
    try:
        state = await start(context)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.debug("", exc_info=True)
        return Error(e)
    while True:
        msg = await queue.get()
        logger.debug(str(msg))
        try:
            behaviour, state = await handler(state, msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("", exc_info=True)
            return Error(e)
        queue.task_done()
        logger.debug(str(behaviour))
        if behaviour == Behaviour.done:
            continue
        elif behaviour == Behaviour.stop:
            return Stop()
        else:
            raise ValueError(f"Unsupported behaviour: {behaviour}")


@attr.s
class Actor:
    task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))


class NoSuchActor(Exception):
    pass


@attr.s
class Cast:
    msg = attr.ib()


@attr.s
class Call:
    reply_to: asyncio.Queue = attr.ib(validator=instance_of(asyncio.Queue))
    msg = attr.ib()


class ActorSystem:
    def __init__(self, user):
        self.event_loop = asyncio.get_running_loop()
        self.actors = {}
        self.watchers = defaultdict(list)

    async def run(self):
        while True:
            for id, actor in self.actors.items():
                exit_ = await actor.task
                watchers = self.watchers.pop(id, [])
                for watcher in watchers:
                    await watcher.cast(Down(actor.ref, reason="stopped"))

    def spawn(self, start, handler, name=None) -> str:
        # NOTE: not thread safe!
        queue = Queue()
        context = Context(system=self)
        task = self.event_loop.create_task(run_actor(context, queue, start, handler))
        id = name or str(uuid.uuid1())
        self.actors[id] = Actor(task=task)
        return id
    
    def watch(self, watcher, watched):
        # NOTE: not thread safe!
        self.watchers[watched].append(watcher)
    
    async def cast(self, id, msg):
        # NOTE: not thread safe!
        try:
            actor = self.actors[id]
        except KeyError:
            raise NoSuchActor(id)
        # Does not block if queue is full
        await actor.queue.put(Cast(msg))
    
    async def call(self, id, msg):
        try:
            actor = self.actors[id]
        except KeyError:
            raise NoSuchActor(id)
        reply_to = asyncio.Queue()
        await actor.queue.put(Call(reply_to, msg))
        return await reply_to.get()
    
    async def shutdown(self):
        for task in self.actor_tasks:
            task.cancel()
            await task
        self.event_loop.stop()
        self.event_loop.close()
