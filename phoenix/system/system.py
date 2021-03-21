import asyncio
from asyncio import Queue
from collections import defaultdict
from enum import Enum
from functools import partial
import logging
from multimethod import multimethod
import threading
import uuid
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple, Union

from phoenix.actor import (
    Actor,
    ActorId,
    ActorHandler,
    ActorStart,
    Behaviour,
    Context,
    Down,
    Error,
    ExitReason,
    Shutdown,
    Stop,
    TimerId,
)
from phoenix.cluster.etcd import node
from phoenix.cluster.protocol import (
    Accepted,
    ClusterNode,
    ClusterNodeShutdown,
    Join,
    Leave,
    Rejected,
    RemoteActorMessage,
)
from phoenix.connection import Connection
from phoenix.dataclasses import dataclass
from phoenix.retry import retry
from phoenix.supervisor import ChildSpec, RestartStrategy, RestartWhen, Supervisor
from phoenix.system import cluster_connection
from phoenix.system.protocol import ActorExists, ClusterDisconnected, NoSuchActor

logger = logging.getLogger(__name__)


@dataclass
class Exit:
    actor_id: ActorId
    reason: ExitReason


async def run_actor(
    context,
    messages,
    queue,
    start: Callable[["Context"], Coroutine],
) -> Tuple[ActorId, ExitReason]:
    # TODO: allow callback on exit for cleanup
    logger.debug("[%s] Starting", str(context.actor_id))
    try:
        actor = await start(context)
    except asyncio.CancelledError:
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Shutdown()))
        return
    except Exception as e:
        logger.debug("[%s] Start", str(context.actor_id), exc_info=True)
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Error(e)))
        return
    state = actor.state
    try:
        while True:
            msg = await queue.get()
            logger.debug("[%s] Received: [%s]", str(context.actor_id), str(msg))
            try:
                behaviour, state = await actor.handler(state, msg)
            except asyncio.CancelledError:
                if actor.on_exit:
                    await actor.on_exit(state, Shutdown())
                await messages.put(
                    ActorExited(actor_id=context.actor_id, reason=Shutdown())
                )
                return
            except Exception as e:
                logger.debug("[%s] %s", str(context.actor_id), str(msg), exc_info=True)
                if actor.on_exit:
                    await actor.on_exit(state, Error(e))
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
                if actor.on_exit:
                    await actor.on_exit(state, Stop())
                await messages.put(
                    ActorExited(actor_id=context.actor_id, reason=Stop())
                )
                return
            else:
                raise ValueError(f"Unsupported behaviour: {behaviour}")
    except asyncio.CancelledError:
        if actor.on_exit:
            await actor.on_exit(state, Shutdown())
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Shutdown()))
        return
    except Exception as e:
        logger.debug("[%s]", str(context.actor_id), exc_info=True)
        if actor.on_exit:
            await actor.on_exit(state, Error(e))
        await messages.put(ActorExited(actor_id=context.actor_id, reason=Error(e)))
        return


@dataclass
class ActorSpawning:
    actor_id: ActorId
    queue: asyncio.Queue
    temporary: bool


@dataclass
class ActorUp:
    actor_id: ActorId
    task: asyncio.Task
    queue: asyncio.Queue
    temporary: bool


@dataclass
class ActorDown:
    actor_id: ActorId
    reason: ExitReason


ActorState = Union[ActorUp, ActorDown]


@dataclass
class Timer:
    dest: ActorId
    task: asyncio.Task


@dataclass
class SpawnActor:
    reply_to: Queue
    actor_id: ActorId
    start: ActorStart
    temporary: bool


@dataclass
class StopActor:
    reply_to: Queue
    actor_id: ActorId


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
class TimerFinished:
    timer_id: TimerId


@dataclass
class ShutdownSystem:
    reply_to: Queue


@dataclass
class ClusterConfig:
    server: Tuple[str, int]
    remote: Tuple[str, int]


@dataclass
class SystemStats:
    actor_count: int


@dataclass
class ActorStats:
    mailbox_size: int


class NoClusterConnection(Exception):
    pass


class TemporaryActor(Exception):
    pass


class ActorSystem(Context):
    def __init__(self, name: str, cluster: Optional[ClusterConfig] = None):
        self.name = name
        self.cluster = cluster
        self.event_loop = asyncio.get_running_loop()
        self.actors = {}
        self.links = defaultdict(set)
        self.watchers = defaultdict(set)
        self.watched = defaultdict(set)
        # Use a queue to serialize state changes
        self.messages = Queue()
        self.timers = {}
        # Timers by destination ActorId
        self.actor_timers = defaultdict(set)
        self.cluster = cluster
        self.conn = None
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self.run())

    async def run(self):
        if self.cluster:
            asyncio.create_task(self.connect())

        while True:
            message = await self.messages.get()
            logger.debug("[ActorSystem(%s)] %s", self.name, str(message))
            await self.handle_message(message)
            self.messages.task_done()
            if isinstance(message, ShutdownSystem):
                return

    async def run_forever(self):
        await self.task

    @multimethod
    async def handle_message(self, msg: ShutdownSystem):
        up = (
            (key, actor)
            for key, actor in self.actors.items()
            if isinstance(actor, ActorUp)
        )
        for key, actor in up:
            logger.debug("Shutdown Actor: [%s]", key)
            actor.task.cancel()
            try:
                await asyncio.wait_for(actor.task, timeout=5)
            except asyncio.TimeoutError:
                # Second cancel after timeout to
                # kill badly behaving actors.
                logger.debug("[%s] Force kill actor after shutdown timeout", key)
                actor.task.cancel()

        await msg.reply_to.put(None)

    @multimethod
    async def handle_message(self, msg: SpawnActor):
        if isinstance(self.actors.get(msg.actor_id), ActorUp):
            await msg.reply_to.put(ActorExists(msg.actor_id))
            return  # Queue is bounded for back pressure to prevent overload
        # https://ferd.ca/queues-don-t-fix-overload.html
        queue = Queue(50)
        self.actors[msg.actor_id] = ActorSpawning(
            actor_id=msg.actor_id,
            queue=queue,
            temporary=msg.temporary,
        )
        context = ActorContext(actor_id=msg.actor_id, system=self)
        task = self.event_loop.create_task(
            run_actor(context, self.messages, queue, msg.start),
            name=str(msg.actor_id),
        )
        actor = ActorUp(
            actor_id=msg.actor_id, task=task, queue=queue, temporary=msg.temporary
        )
        self.actors[msg.actor_id] = actor
        logger.debug("[%s] Actor Spawned", str(msg.actor_id))
        await msg.reply_to.put(msg.actor_id)

    @multimethod
    async def handle_message(self, msg: StopActor):
        if isinstance(self.actors.get(msg.actor_id), ActorDown):
            await msg.reply_to.put(NoSuchActor(msg.actor_id))
            return

        actor = self.actors[msg.actor_id]
        actor.task.cancel()
        await actor.task

        await msg.reply_to.put(None)

    @multimethod
    async def handle_message(self, msg: WatchActor):
        try:
            watcher = self.actors[msg.watcher]
        except KeyError:
            await msg.reply_to.put(NoSuchActor(msg.watcher))
            return
        if isinstance(watcher, ActorDown):
            await msg.reply_to.put(NoSuchActor(msg.watcher))
            return

        if msg.watched not in self.actors:
            await msg.reply_to.put(NoSuchActor(msg.watched))
            return

        watched = self.actors[msg.watched]

        if watched.temporary:
            await msg.reply_to.put(TemporaryActor(msg.watched))
            return

        # Notify immediately if watched actor is Down
        if isinstance(watched, ActorDown):
            await self.cast(
                watcher.actor_id,
                Down(actor_id=watched.actor_id, reason=watched.reason),
            )
            await msg.reply_to.put(None)
            return

        self.watchers[msg.watched].add(msg.watcher)
        self.watched[msg.watcher].add(msg.watched)
        await msg.reply_to.put(None)

    @multimethod
    async def handle_message(self, msg: LinkActors):
        # TODO: if one actor is already down and the
        # other is up then stop the linked actor
        # immediately.
        if msg.a not in self.actors:
            await msg.reply_to.put(NoSuchActor(msg.a))
            return
        if msg.b not in self.actors:
            await msg.reply_to.put(NoSuchActor(msg.b))
            return
        if msg.a == msg.b:
            await msg.reply_to.put(ValueError(f"{msg.a!r} == {msg.b!r}"))
            return
        if self.actors[msg.a].temporary or self.actors[msg.b].temporary:
            await msg.reply_to(TemporaryActor())
            return
        self.links[msg.a].add(msg.b)
        self.links[msg.b].add(msg.a)
        await msg.reply_to.put(None)

    @multimethod
    async def handle_message(self, msg: ActorExited):
        logger.debug(
            "[%s] Actor Exited; Reason: [%s]",
            str(msg.actor_id),
            str(msg.reason),
        )
        actor = self.actors[msg.actor_id]
        # Temporary actors are permanently removed
        if actor.temporary:
            del self.actors[msg.actor_id]
        else:
            self.actors[msg.actor_id] = ActorDown(
                actor_id=msg.actor_id, reason=msg.reason
            )

        # Remove as a watcher of other Actors
        for watcher in self.watched.pop(msg.actor_id, set()):
            try:
                self.watchers[watcher].remove(msg.actor_id)
            except KeyError:
                pass

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

        # Cancel timers with msg.actor_id as destination
        try:
            timer_ids = self.actor_timers.pop(msg.actor_id)
        except KeyError:
            pass
        else:
            for timer_id in timer_ids:
                timer = self.timers.pop(timer_id)
                timer.task.cancel()
                try:
                    await timer.task
                except asyncio.CancelledError:
                    pass

    @multimethod
    async def handle_message(self, msg: TimerFinished):
        timer = self.timers.pop(msg.timer_id)
        self.actor_timers[timer.dest].remove(msg.timer_id)

    async def connect(self):
        logger.debug("[%r] Connect to cluster [%s]", self, self.cluster)
        supervisor = await Supervisor.new(
            context=self,
            name="Supervisor.ActorSystem.ClusterNode",
        )
        await supervisor.init(
            children=[
                ChildSpec(
                    start=partial(
                        node.start,
                        name=self.name,
                        address=self.cluster.server,
                        etcd=self.cluster.remote,
                    ),
                    options=dict(name="ActorSystem.ClusterNode"),
                    # Do not restart when shutdown by cluster node
                    restart_when=RestartWhen.transient,
                ),
            ],
            strategy=RestartStrategy.one_for_one,
        )
        self.conn = ClusterNode(
            actor_id=ActorId(system_id=self.name, value="ActorSystem.ClusterNode"),
            context=self,
        )

    async def spawn(
        self,
        start: Callable[["Context"], Coroutine],
        name=None,
        temporary: bool = False,
    ) -> ActorId:
        """
        Args:
            temporary (bool): A temporary actor cannot be watched
                 or linked against. It is permanently removed from
                 the system once it has exited. Use temporary actors
                 to perform one-off tasks.

        Note: not thread-safe.

        Raises:
            ActorExists: if the actor given by ``name`` already exists.
        """
        actor_id = ActorId(self.name, name or str(uuid.uuid1()))
        reply_to = Queue()
        await self.messages.put(
            SpawnActor(
                reply_to=reply_to,
                actor_id=actor_id,
                start=start,
                temporary=temporary,
            )
        )
        reply = await reply_to.get()
        if isinstance(reply, Exception):
            raise reply
        return reply

    async def stop(self, actor_id: ActorId):
        """
        Note: not thread-safe.
        """
        reply_to = Queue()
        await self.messages.put(
            StopActor(
                reply_to=reply_to,
                actor_id=actor_id,
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

    async def list_actors(self) -> Set[ActorId]:
        return set(self.actors.keys())

    async def get_system_stats(self) -> SystemStats:
        actor_count = len(
            [actor for actor in self.actors.values() if isinstance(actor, ActorUp)]
        )
        return SystemStats(actor_count=actor_count)

    async def get_actor_stats(self, actor_id: ActorId) -> ActorStats:
        actor = self.actors[actor_id]
        return ActorStats(
            mailbox_size=actor.queue.qsize(),
        )

    async def cast(self, actor_id: ActorId, msg: Any):
        """
        Send a message to the actor with id ``actor_id``.

        Note: not thread-safe.
        """
        if actor_id.system_id == self.name:
            try:
                actor = self.actors[actor_id]
            except KeyError:
                logger.debug("No such actor: [%s]", actor_id)
                return
            if isinstance(actor, ActorDown):
                logger.debug("Actor down: [%s]", actor_id)
                return
            await actor.queue.put(msg)
        else:
            if self.conn is None:
                raise NoClusterConnection

            # TODO: serialize remote sends via self.messages?
            await self.conn.send(actor_id=actor_id, msg=msg)

    async def send_after(self, dest: ActorId, msg: Any, delay: float) -> TimerId:
        timer_id = TimerId(str(uuid.uuid1()))

        async def _timer():
            await asyncio.sleep(delay)
            await self.cast(dest, msg)
            await self.messages.put(TimerFinished(timer_id))

        task = asyncio.create_task(_timer())
        self.timers[timer_id] = Timer(dest=dest, task=task)
        self.actor_timers[dest].add(timer_id)
        return timer_id

    async def call(self, actor_id: ActorId, f, timeout=None):
        """
        Send a message to the actor with id ``actor_id``
        and await a response.

        Args:
            timeout (Optional[float]): Time in seconds before
                the call is aborted if it has not yet received
                a reply.

        Note: not thread-safe.
        """
        # Prevent unbounded growth of exited response actors
        # by designating them as temporary actors.
        start = time.monotonic()

        reply = None
        received = asyncio.Event()

        async def _handle(state: None, msg):
            nonlocal reply
            reply = msg
            received.set()
            return Behaviour.stop, state

        async def _start(ctx):
            return Actor(state=None, handler=_handle)

        reply_to = await self.spawn(
            _start, name=f"Response.{actor_id.value}.{uuid.uuid1()}", temporary=True
        )
        msg = f(reply_to)
        # TODO: timeout in case a reply is never received to prevent
        # unbounded reply actors from accumulating?
        await self.cast(actor_id, msg)
        try:
            await asyncio.wait_for(received.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            await self.stop(reply_to)
            raise

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

    async def send_after(self, dest: ActorId, msg, delay: float) -> TimerId:
        return await self.system.send_after(dest, msg, delay)

    async def call(self, actor_id, msg):
        return await self.system.call(actor_id, msg)

    async def spawn(self, start, name=None) -> ActorId:
        return await self.system.spawn(start, name=name)

    async def watch(self, actor_id: ActorId):
        await self.system.watch(self.actor_id, actor_id)

    async def link(self, a: ActorId, b: ActorId):
        await self.system.link(a, b)
