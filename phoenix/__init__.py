from abc import ABC, abstractmethod
import asyncio
from asyncio import Queue
from collections import defaultdict
from enum import Enum
from functools import partial
import logging
from multipledispatch import dispatch
import threading
import uuid
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, Union

from .dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActorId:
    value: str

    def __str__(self) -> str:
        return self.value


class Behaviour(Enum):
    done = 0
    stop = 1


@dataclass
class Shutdown:
    pass


@dataclass
class Stop:
    pass


@dataclass
class Error:
    exc: Exception


ExitReason = Union[Shutdown, Stop, Error]


@dataclass
class Exit:
    actor_id: ActorId
    reason: ExitReason


async def run_actor(
    context,
    events,
    mailbox,
    start: Callable[["Context"], Coroutine],
    handler: Callable[[Any, Any], Coroutine],
) -> Tuple[ActorId, ExitReason]:
    logger.debug("[%s] Starting", str(context.actor_id))
    try:
        state = await start(context)
    except asyncio.CancelledError:
        await events.put(Exited(actor_id=context.actor_id, reason=Shutdown()))
        return
        # return Exit(actor_id=context.actor_id, reason=Shutdown())
    except Exception as e:
        logger.debug("[%s] Start", str(context.actor_id), exc_info=True)
        return Exit(actor_id=context.actor_id, reason=Error(e))
    try:
        while True:
            msg = await mailbox.get()
            logger.debug("[%s] Received: [%s]", str(context.actor_id), str(msg))
            try:
                behaviour, state = await handler(state, msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("[%s] %s", str(context.actor_id), str(msg), exc_info=True)
                return Exit(actor_id=context.actor_id, reason=Error(e))
            finally:
                mailbox.task_done()
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


@dataclass
class Down:
    """
    Message sent to a watcher when the watched exits.
    """

    actor_id: ActorId
    reason: ExitReason


@dataclass
class Spawned:
    pass


@dataclass
class Linked:
    pass


@dataclass
class Exited:
    pass


ActorEvent = Union[Spawned, Linked, Exited]


@dataclass
class Actor:
    id: ActorId
    events: List[ActorEvent]


@dataclass
class ActorHandle:
    task: asyncio.Task
    queue: asyncio.Queue


@dataclass(frozen=True)
class TimerId:
    value: str


@dataclass
class Timer:
    task: asyncio.Task


class Context(ABC):
    @abstractmethod
    async def cast(self, actor_id: ActorId, msg: Any):
        raise NotImplementedError

    @abstractmethod
    async def cast_after(self, actor_id: ActorId, msg, delay: float) -> TimerId:
        raise NotImplementedError

    @abstractmethod
    async def call(self, actor_id: ActorId, msg: Any):
        raise NotImplementedError

    @abstractmethod
    async def spawn(self, start, handle, name=None) -> ActorId:
        raise NotImplementedError

    @abstractmethod
    def watch(self, actor_id: ActorId):
        raise NotImplementedError

    @abstractmethod
    def link(self, a: ActorId, b: ActorId):
        raise NotImplementedError


class ActorSystem(Context):
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

    async def spawn(
        self,
        start: Callable[["Context"], Coroutine],
        handler: Callable[[Any, Any], Coroutine],
        name=None,
    ) -> ActorId:
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
        context = ActorContext(actor_id=actor_id, system=self)
        task = self.event_loop.create_task(run_actor(context, queue, start, handler))
        actor = ActorHandle(task=task, queue=queue)
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
        # TODO: timeout in case a reply is never received to prevent
        # unbounded reply actors from accumulating?
        await actor.queue.put(msg)
        await received.wait()
        dt = time.monotonic() - start
        logger.debug("Call [time=%s] [%s] [%s]", str(dt), str(actor_id), repr(msg))
        return reply

    async def shutdown(self):
        for actor in list(self.actors.values()):
            actor.task.cancel()
            await actor.task


@dataclass
class ActorContext(Context):
    """
    A handle to the runtime context of an actor.
    """

    actor_id: ActorId
    system: ActorSystem

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


class RestartStrategy(Enum):
    one_for_one = 0


@dataclass
class Supervisor:
    actor_id: ActorId
    context: Context

    @dataclass
    class Uninitialised:
        context: Context

    @dataclass
    class Supervising:
        context: Context
        factories: List[
            Tuple[
                Callable[[Context], Coroutine],
                Callable[[Any, Any], Coroutine],
                Dict[str, str],
            ]
        ]
        children: List[ActorId]
        restarts: List[int]
        strategy: RestartStrategy

    @dataclass
    class Init:
        reply_to: ActorId
        children: List[
            Tuple[
                Callable[[Context], Coroutine],
                Callable[[Any, Any], Coroutine],
                Dict[str, str],
            ]
        ]
        strategy: RestartStrategy

    @dataclass
    class Restart:
        """
        Internal message indicating that an actor
        should be restarted.
        """

        actor_id: ActorId
        reason: ExitReason

    @classmethod
    async def new(cls, context, name=None) -> "Supervisor":
        actor_id = await context.spawn(cls.start, cls.handle, name=name)
        return Supervisor(actor_id=actor_id, context=context)

    @staticmethod
    async def start(context):
        return Supervisor.Uninitialised(context=context)

    @staticmethod
    @dispatch(Uninitialised, Init)
    async def handle(state: Uninitialised, msg: Init) -> Supervising:
        children = []
        restarts = []
        for start, handle, kwargs in msg.children:
            child = await state.context.spawn(start, handle, **kwargs)
            state.context.watch(child)
            children.append(child)
            restarts.append(0)
        await state.context.cast(msg.reply_to, None)
        return Behaviour.done, Supervisor.Supervising(
            context=state.context,
            factories=msg.children,
            children=children,
            restarts=restarts,
            strategy=msg.strategy,
        )

    @staticmethod
    @dispatch(Supervising, Down)
    async def handle(state: Supervising, msg: Down) -> Supervising:
        index = state.children.index(msg.actor_id)
        if state.strategy == RestartStrategy.one_for_one:
            backoff = 2 ** state.restarts[index]
            await state.context.cast_after(
                state.context.actor_id,
                Supervisor.Restart(actor_id=msg.actor_id, reason=msg.reason),
                backoff,
            )
        else:
            raise ValueError(f"Unsupported RestartStrategy: {state.strategy}")
        return Behaviour.done, state

    @staticmethod
    @dispatch(Supervising, Restart)
    async def handle(state: Supervising, msg: Restart) -> Tuple[Behaviour, Supervising]:
        index = state.children.index(msg.actor_id)
        start, handle, kwargs = state.factories[index]
        logger.debug(
            "[%s] Supervisor restarting child: [%s]; Reason: [%s]",
            str(state.context.actor_id),
            msg.actor_id,
            str(msg.reason),
        )
        child = await state.context.spawn(start, handle, **kwargs)
        state.context.watch(child)
        state.children[index] = child
        state.restarts[index] += 1
        return Behaviour.done, state

    async def init(self, children, strategy):
        await self.context.call(
            self.actor_id, partial(self.Init, children=children, strategy=strategy)
        )


@dataclass
class Router:
    actor_id: ActorId
    context: Context

    @dataclass
    class State:
        context: Context
        actors: List[ActorId]
        index: int

    @classmethod
    async def new(cls, context, workers: int, start, handle, name=None) -> "Router":
        actor_id = await context.spawn(
            partial(cls.start, workers=workers, start=start, handle=handle),
            cls.handle,
            name=name,
        )
        return cls(actor_id=actor_id, context=context)

    @staticmethod
    async def start(context, workers: int, start, handle) -> State:
        children = [
            (start, handle, dict(name=f"{context.actor_id}.{i}"))
            for i in range(workers)
        ]
        supervisor = await Supervisor.new(
            context, name=f"{context.actor_id}.Supervisor"
        )
        await supervisor.init(children=children, strategy=RestartStrategy.one_for_one)
        return Router.State(
            context=context,
            actors=[ActorId(f"{context.actor_id}.{i}") for i in range(workers)],
            index=0,
        )

    @staticmethod
    async def handle(state: State, msg: Any) -> Tuple[Behaviour, State]:
        actor_id = state.actors[state.index]
        await state.context.cast(actor_id, msg)
        state.index = (state.index + 1) % len(state.actors)
        return Behaviour.done, state

    async def route(self, msg: Any):
        await self.context.cast(self.actor_id, msg)


def retry(
    max_retries: int = 5,
    backoff: Callable[[int], float] = lambda n: 2 ** n,
):
    async def _retry(func: Callable[[], Coroutine]):
        n = 0
        while n <= max_retries:
            try:
                return await func()
            except Exception as e:
                exc = e
                wait_for = backoff(n)
                n += 1
                logger.debug(
                    "Retry caught error. Retrying in %f seconds. Retry: %d.",
                    wait_for,
                    n,
                    exc_info=True,
                )
                await asyncio.sleep(wait_for)

        raise exc
    return _retry
