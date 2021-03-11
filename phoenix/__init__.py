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
    messages,
    queue,
    start: Callable[["Context"], Coroutine],
    handler: Callable[[Any, Any], Coroutine],
) -> Tuple[ActorId, ExitReason]:
    logger.debug("[%s] Starting", str(context.actor_id))
    try:
        state = await start(context)
    except asyncio.CancelledError:
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Shutdown()))
        return
    except Exception as e:
        logger.debug("[%s] Start", str(context.actor_id), exc_info=True)
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Error(e)))
        return
    try:
        while True:
            msg = await queue.get()
            logger.debug("[%s] Received: [%s]", str(context.actor_id), str(msg))
            try:
                behaviour, state = await handler(state, msg)
            except asyncio.CancelledError:
                await messages.put(
                    ActorExited(actor_id=context.actor_id, reason=Shutdown())
                )
                return
            except Exception as e:
                logger.debug("[%s] %s", str(context.actor_id), str(msg), exc_info=True)
                await messages.put(
                    ActorExited(actor_id=context.actor_id, reason=Error(e))
                )
                return
            finally:
                queue.task_done()
            logger.debug("[%s] %s", str(context.actor_id), str(behaviour))
            if behaviour == Behaviour.done:
                continue
            elif behaviour == Behaviour.stop:
                await messages.put(
                    ActorExited(actor_id=context.actor_id, reason=Stop())
                )
                return
            else:
                raise ValueError(f"Unsupported behaviour: {behaviour}")
    except asyncio.CancelledError:
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Shutdown()))
        return
    except Exception as e:
        logger.debug("[%s]", str(context.actor_id), exc_info=True)
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Error(e)))
        return


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
class Actor:
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
    async def watch(self, actor_id: ActorId):
        raise NotImplementedError

    @abstractmethod
    async def link(self, a: ActorId, b: ActorId):
        raise NotImplementedError


ActorStart = Callable[[Context], Coroutine]
ActorHandle = Callable[[Any, Any], Coroutine]
ActorSpawnOptions = Dict[str, str]
ActorFactory = Tuple[ActorStart, ActorHandle, ActorSpawnOptions]


@dataclass
class SpawnActor:
    reply_to: Queue
    actor_id: ActorId
    start: ActorStart
    handler: ActorHandle


@dataclass
class WatchActor:
    reply_to: Queue
    watcher: ActorId
    watched: ActorId


@dataclass
class LinkActors:
    reply_to: Queue
    a: ActorId
    b: ActorId


@dataclass
class ActorExited:
    actor_id: ActorId
    reason: ExitReason


@dataclass
class ShutdownSystem:
    reply_to: Queue


class ActorSystem(Context):
    def __init__(self, name: str):
        self.name = name
        self.event_loop = asyncio.get_running_loop()
        self.actors = {}
        self.links = defaultdict(set)
        self.watchers = defaultdict(set)
        self.watched = defaultdict(set)
        # Serialize changes to system state through the use
        # of a Queue.
        self.messages = Queue()
        self.timers = {}

    async def run(self):
        while True:
            message = await self.messages.get()
            logger.debug("[ActorSystem(%s)] %s", self.name, str(message))
            await self.handle_message(message)
            self.messages.task_done()
            if isinstance(message, ShutdownSystem):
                return

    @dispatch(ShutdownSystem)
    async def handle_message(self, msg: ShutdownSystem):
        for actor in self.actors.values():
            actor.task.cancel()
            await actor.task
        await msg.reply_to.put(None)

    @dispatch(SpawnActor)
    async def handle_message(self, msg: SpawnActor):
        if msg.actor_id in self.actors:
            await msg.reply_to.put(ActorExists(msg.actor_id))
            return
        # TODO: bounded queues for back pressure to prevent overload
        queue = Queue(50)
        context = ActorContext(actor_id=msg.actor_id, system=self)
        task = self.event_loop.create_task(
            run_actor(context, self.messages, queue, msg.start, msg.handler),
            name=str(msg.actor_id),
        )
        actor = Actor(task=task, queue=queue)
        self.actors[msg.actor_id] = actor
        logger.debug("[%s] Actor Spawned", str(msg.actor_id))
        await msg.reply_to.put(msg.actor_id)

    @dispatch(WatchActor)
    async def handle_message(self, msg: WatchActor):
        if msg.watcher not in self.actors:
            await msg.reply_to.put(NoSuchActor(msg.watcher))
            return
        if msg.watched not in self.actors:
            await msg.reply_to.put(NoSuchActor(msg.watched))
            return
        self.watchers[msg.watched].add(msg.watcher)
        self.watched[msg.watcher].add(msg.watched)
        await msg.reply_to.put(None)

    @dispatch(LinkActors)
    async def handle_message(self, msg: LinkActors):
        if msg.a not in self.actors:
            await msg.reply_to.put(NoSuchActor(msg.a))
            return
        if msg.b not in self.actors:
            await msg.reply_to.put(NoSuchActor(msg.b))
            return
        if msg.a == msg.b:
            await msg.reply_to.put(ValueError(f"{msg.a!r} == {msg.b!r}"))
            return
        self.links[msg.a].add(msg.b)
        self.links[msg.b].add(msg.a)
        await msg.reply_to.put(None)

    @dispatch(ActorExited)
    async def handle_message(self, msg: ActorExited):
        logger.debug(
            "[%s] Actor Exited; Reason: [%s]",
            str(msg.actor_id),
            str(msg.reason),
        )
        del self.actors[msg.actor_id]

        # Remove as a watcher of other Actors
        for watcher in self.watched.pop(msg.actor_id, set()):
            self.watchers[watcher].remove(msg.actor_id)

        # Shutdown linked actors
        # Remove bidirectional links
        for linked_id in self.links.pop(msg.actor_id, set()):
            actor = self.actors[linked_id]
            logger.debug(
                "Shutdown [%s] due to Linked Actor Exit [%s]",
                str(linked_id),
                str(msg.actor_id),
            )
            actor.task.cancel()
            self.links[linked_id].remove(msg.actor_id)

        # Notify watchers
        for watcher in self.watchers.pop(msg.actor_id, set()):
            await self.cast(
                watcher,
                Down(actor_id=msg.actor_id, reason=msg.reason),
            )

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
        reply_to = Queue()
        await self.messages.put(
            SpawnActor(
                reply_to=reply_to,
                actor_id=actor_id,
                start=start,
                handler=handler,
            )
        )
        reply = await reply_to.get()
        if isinstance(reply, Exception):
            raise reply
        return reply

    async def link(self, a: ActorId, b: ActorId):
        """
        Create a bi-directional link between two actors.

        Linked actors are shutdown when either exits.

        An actor cannot be linked to itself.

        Note: not thread-safe.

        Raises:
            NoSuchActor: if neither a nor b is an actor.
            ValueError: if a == b.
        """
        reply_to = Queue()
        await self.messages.put(
            LinkActors(
                reply_to=reply_to,
                a=a,
                b=b,
            )
        )
        reply = await reply_to.get()
        if isinstance(reply, Exception):
            raise reply

    async def watch(self, watcher: ActorId, watched: ActorId):
        """
        ``watcher`` watches ``watched``.

        A watcher is notified when the watched actor exits.

        Note: not thread-safe.
        """
        reply_to = Queue()
        await self.messages.put(
            WatchActor(
                reply_to=reply_to,
                watcher=watcher,
                watched=watched,
            )
        )
        reply = await reply_to.get()
        if isinstance(reply, Exception):
            raise reply

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
        reply_to = Queue()
        await self.messages.put(ShutdownSystem(reply_to=reply_to))
        await reply_to.get()


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

    async def watch(self, actor_id: ActorId):
        await self.system.watch(self.actor_id, actor_id)

    async def link(self, a: ActorId, b: ActorId):
        await self.system.link(a, b)


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
        factories: List[ActorFactory]
        children: List[ActorId]
        restarts: List[int]
        strategy: RestartStrategy

    @dataclass
    class Init:
        """
        Synchronously initialise the Supervisor
        enabling clients to wait until children
        have all been spawned.
        """

        reply_to: ActorId
        children: List[ActorFactory]
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
        actor_id = await context.spawn(
            cls.start,
            cls.handle,
            name=name,
        )
        return Supervisor(actor_id=actor_id, context=context)

    @staticmethod
    async def start(context: Context) -> Uninitialised:
        return Supervisor.Uninitialised(context=context)

    @staticmethod
    @dispatch(Uninitialised, Init)
    async def handle(state: Uninitialised, msg: Init) -> Tuple[Behaviour, Supervising]:
        children = []
        restarts = []
        for start, handle, kwargs in msg.children:
            child = await state.context.spawn(start, handle, **kwargs)
            await state.context.watch(child)
            children.append(child)
            restarts.append(0)
        await state.context.cast(msg.reply_to, None)
        return (
            Behaviour.done,
            Supervisor.Supervising(
                context=state.context,
                factories=msg.children,
                children=children,
                restarts=restarts,
                strategy=msg.strategy,
            ),
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
        await state.context.watch(child)
        state.children[index] = child
        state.restarts[index] += 1
        return Behaviour.done, state

    async def init(self, children: List[ActorFactory], strategy: RestartStrategy):
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
            context,
            children=children,
            strategy=RestartStrategy.one_for_one,
            name=f"{context.actor_id}.Supervisor",
        )
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


@dataclass
class DynamicSupervisor:
    """
    Supervisor for children that are started dynamically.
    """

    actor_id: ActorId
    context: Context

    @dataclass
    class State:
        context: Context
        factories: List[ActorFactory]
        children: List[ActorId]
        restarts: List[int]

    @dataclass
    class StartChild:
        reply_to: ActorId
        start: ActorStart
        handle: ActorHandle
        kwargs: Dict[str, str]

    @dataclass
    class Restart:
        """
        Internal message indicating that an actor
        should be restarted.
        """

        actor_id: ActorId
        reason: ExitReason

    @classmethod
    async def new(cls, context: Context, name=None) -> "DynamicSupervisor":
        actor_id = await context.spawn(cls.start, cls.handle, name=name)
        return DynamicSupervisor(actor_id=actor_id, context=context)

    @staticmethod
    async def start(context: Context) -> State:
        return DynamicSupervisor.State(
            context=context, factories=[], children=[], restarts=[]
        )

    @staticmethod
    @dispatch(State, StartChild)
    async def handle(state: State, msg: StartChild) -> Tuple[Behaviour, State]:
        child = await state.context.spawn(msg.start, msg.handle, **msg.kwargs)
        await state.context.watch(child)
        state.factories.append((msg.start, msg.handle, msg.kwargs))
        state.children.append(child)
        state.restarts.append(0)
        await state.context.cast(msg.reply_to, child)
        return Behaviour.done, state

    @staticmethod
    @dispatch(State, Down)
    async def handle(state: State, msg: Down) -> State:
        index = state.children.index(msg.actor_id)
        backoff = 2 ** state.restarts[index]
        await state.context.cast_after(
            state.context.actor_id,
            DynamicSupervisor.Restart(actor_id=msg.actor_id, reason=msg.reason),
            backoff,
        )
        return Behaviour.done, state

    @staticmethod
    @dispatch(State, Restart)
    async def handle(state: State, msg: Restart) -> Tuple[Behaviour, State]:
        index = state.children.index(msg.actor_id)
        start, handle, kwargs = state.factories[index]
        logger.debug(
            "[%s] Restarting child: [%s]; Reason: [%s]",
            str(state.context.actor_id),
            msg.actor_id,
            str(msg.reason),
        )
        child = await state.context.spawn(start, handle, **kwargs)
        await state.context.watch(child)
        state.children[index] = child
        state.restarts[index] += 1
        return Behaviour.done, state

    async def start_child(
        self, start: ActorStart, handle: ActorHandle, kwargs: Dict[str, str]
    ) -> ActorId:
        return await self.context.call(
            self.actor_id,
            partial(self.StartChild, start=start, handle=handle, kwargs=kwargs),
        )


def retry(max_retries: int = 5, backoff: Callable[[int], float] = lambda n: 2 ** n):
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
