import asyncio
from asyncio import Queue
import attr
from attr.validators import instance_of
from enum import Enum
import logging
from multipledispatch import dispatch
import threading

logger = logging.getLogger(__name__)


class Behaviour(Enum):
    done = 0
    stop = 1


@attr.s
class ActorRef:
    queue: Queue = attr.ib(validator=instance_of(Queue))

    async def cast(self, msg):
        await self.queue.put(msg)
    
    async def call(self, f):
        queue = Queue()
        ref = ActorRef(queue)
        msg = f(ref)
        await self.cast(msg)
        return await ref.queue.get()


@attr.s
class Context:
    """
    A handle to the runtime context of an actor.
    """
    system = attr.ib()
    ref: ActorRef = attr.ib()

    def spawn(self, start, handler) -> ActorRef:
        return self.system.spawn(start, handler)
    
    def monitor(self, ref):
        raise NotImplementedError


async def run_actor(context, start, handler):
    state = await start(context)
    while True:
        msg = await context.ref.queue.get()
        logger.debug(str(msg))
        behaviour, state = await handler(state, msg)
        context.ref.queue.task_done()
        logger.debug(str(behaviour))
        if behaviour == Behaviour.done:
            continue
        elif behaviour == Behaviour.stop:
            return
        else:
            raise ValueError(f"Unsupported behaviour: {behaviour}")


class ActorSystem:
    def __init__(self):
        self.event_loop = asyncio.get_running_loop()
        self.actor_tasks = []
    
    async def run(self):
        for task in self.actor_tasks:
            await task

    def spawn(self, start, handler) -> ActorRef:
        # NOTE: not thread safe!
        queue = Queue()
        ref = ActorRef(queue)
        context = Context(system=self, ref=ref)
        task = self.event_loop.create_task(run_actor(context, start, handler))
        self.actor_tasks.append(task)
        return ref
    
    async def shutdown(self):
        for task in self.actor_tasks:
            task.cancel()
            await task
        self.event_loop.stop()
        self.event_loop.close()
