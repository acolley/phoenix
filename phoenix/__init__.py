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
import time
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
class Shutdown:
    pass


@attr.s
class Stop:
    pass


@attr.s
class Error:
    exc: Exception = attr.ib(validator=instance_of(Exception))


ExitReason = Union[Shutdown, Stop, Error]


@attr.s
class Exit:
    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    reason: ExitReason = attr.ib(validator=instance_of((Shutdown, Stop, Error)))


async def run_actor(context, queue, start, handler) -> Tuple[ActorId, ExitReason]:
    logger.debug("[%s] Starting", str(context.actor_id))
    try:
        state = await start(context)
    except asyncio.CancelledError:
        return Exit(actor_id=context.actor_id, reason=Shutdown())
    except Exception as e:
        logger.debug("[%s] Start", str(context.actor_id), exc_info=True)
        return Exit(actor_id=context.actor_id, reason=Error(e))
    try:
        while True:
            msg = await queue.get()
            logger.debug("[%s] Received: [%s]", str(context.actor_id), str(msg))
            try:
                behaviour, state = await handler(state, msg)
            except asyncio.CancelledError:
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
    except asyncio.CancelledError:
        return Exit(actor_id=context.actor_id, reason=Shutdown())
    except Exception as e:
        logger.debug("[%s]", str(context.actor_id), exc_info=True)
        return Exit(actor_id=context.actor_id, reason=Error(e))


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
    reason: ExitReason = attr.ib(validator=instance_of((Shutdown, Stop, Error)))


@attr.s
class Actor:
    task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))
    queue: asyncio.Queue = attr.ib(validator=instance_of(asyncio.Queue))


@attr.s(frozen=True)
class TimerId:
    value: str = attr.ib(validator=instance_of(str))


@attr.s
class Timer:
    task: asyncio.Task = attr.ib(validator=instance_of(asyncio.Task))


class ActorSystem:
    def __init__(self):
        self.event_loop = asyncio.get_running_loop()
        self.actors = {}
        self.links = defaultdict(set)
        self.watchers = defaultdict(set)
        self.watched = defaultdict(set)
        self.spawned = asyncio.Event()
        self.timers = {}

    async def run(self):
        while True:
            aws = [actor.task for actor in self.actors.values()] + [self.spawned.wait()]
            for coro in asyncio.as_completed(aws):
                result = await coro
                if isinstance(result, Exit):
                    del self.actors[result.actor_id]
                    logger.debug(
                        "[%s] Actor Removed; Reason: [%s]",
                        str(result.actor_id),
                        str(result.reason),
                    )

                    # Remove as a watcher of other Actors
                    for watcher in self.watched.pop(result.actor_id, set()):
                        self.watchers[watcher].remove(result.actor_id)

                    # Shutdown linked actors
                    # Remove bidirectional links
                    for linked_id in self.links.pop(result.actor_id, set()):
                        actor = self.actors[linked_id]
                        logger.debug(
                            "Shutdown [%s] due to Linked Actor Exit [%s]",
                            str(linked_id),
                            str(result.actor_id),
                        )
                        actor.task.cancel()
                        self.links[linked_id].remove(result.actor_id)

                    # Notify watchers
                    for watcher in self.watchers.pop(result.actor_id, set()):
                        await self.cast(
                            watcher,
                            Down(actor_id=result.actor_id, reason=result.reason),
                        )
                else:
                    self.spawned.clear()
                break

    async def spawn(self, start, handler, name=None) -> ActorId:
        """
        Note: not thread-safe.

        Raises:
            ActorExists: if the actor given by ``name`` already exists.
        """
        actor_id = ActorId(name or str(uuid.uuid1()))
        logger.debug("[%s] Spawn Actor", str(actor_id))
        if actor_id in self.actors:
            raise ActorExists(actor_id)
        # TODO: bounded queues for back pressure to prevent overload
        queue = Queue()
        context = Context(actor_id=actor_id, system=self)
        task = self.event_loop.create_task(run_actor(context, queue, start, handler))
        actor = Actor(task=task, queue=queue)
        self.actors[actor_id] = actor
        self.spawned.set()
        logger.debug("[%s] Actor Spawned", str(actor_id))
        return actor_id

    def link(self, a: ActorId, b: ActorId):
        """
        Create a bi-directional link between two actors.

        Linked actors are shutdown when either exits.

        An actor cannot be linked to itself.

        Note: not thread-safe.

        Raises:
            NoSuchActor: if neither a nor b is an actor.
            ValueError: if a == b.
        """
        logger.debug("Link Actors [%s] and [%s]", str(a), str(b))
        if a not in self.actors:
            raise NoSuchActor(a)
        if b not in self.actors:
            raise NoSuchActor(b)
        if a == b:
            raise ValueError(f"{a!r} == {b!r}")
        self.links[a].add(b)
        self.links[b].add(a)

    def watch(self, watcher: ActorId, watched: ActorId):
        """
        ``watcher`` watches ``watched``.

        A watcher is notified when the watched actor exits.

        Note: not thread-safe.
        """
        logger.debug("[%s] watch actor [%s]", str(watcher), str(watched))
        if watcher not in self.actors:
            raise NoSuchActor(watcher)
        if watched not in self.actors:
            raise NoSuchActor(watched)
        self.watchers[watched].add(watcher)
        self.watched[watcher].add(watched)

    async def cast(self, actor_id: ActorId, msg):
        """
        Send a message to the actor with id ``actor_id``.

        Note: not thread-safe.
        """
        try:
            actor = self.actors[actor_id]
        except KeyError:
            raise NoSuchActor(actor_id)
        await actor.queue.put(msg)

    async def cast_after(self, actor_id: ActorId, msg, delay: float) -> TimerId:
        timer_id = TimerId(str(uuid.uuid1()))

        async def _timer():
            await asyncio.sleep(delay)
            await self.cast(actor_id, msg)
            del self.timers[timer_id]

        task = asyncio.create_task(_timer())
        self.timers[timer_id] = Timer(task=task)
        return timer_id

    async def call(self, actor_id: ActorId, f):
        """
        Send a message to the actor with id ``actor_id``
        and await a response.

        Note: not thread-safe.
        """
        start = time.monotonic()
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
        dt = time.monotonic() - start
        logger.debug("Call [time=%s] [%s] [%s]", str(dt), str(actor_id), repr(msg))
        return reply

    async def shutdown(self):
        for actor in list(self.actors.values()):
            actor.task.cancel()
            await actor.task


@attr.s
class Context:
    """
    A handle to the runtime context of an actor.
    """

    actor_id: ActorId = attr.ib(validator=instance_of(ActorId))
    system = attr.ib()

    async def cast(self, actor_id, msg):
        await self.system.cast(actor_id, msg)

    async def cast_after(self, actor_id: ActorId, msg, delay: float) -> TimerId:
        return await self.system.cast_after(actor_id, msg, delay)

    async def call(self, actor_id, msg):
        return await self.system.call(actor_id, msg)

    async def spawn(self, start, handle, name=None) -> ActorId:
        return await self.system.spawn(start, handle, name=name)

    def watch(self, actor_id: ActorId):
        self.system.watch(self.actor_id, actor_id)

    def link(self, a: ActorId, b: ActorId):
        self.system.link(a, b)
