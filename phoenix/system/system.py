import asyncio
from asyncio import Queue, Task
import attr
from attr.validators import deep_iterable, deep_mapping, instance_of, optional
import contextlib
from datetime import timedelta
from functools import partial
import janus
import logging
from multipledispatch import dispatch
from pyrsistent import PVector, m, v
import threading
import traceback
from typing import Any, Callable, Generic, Iterable, List, Mapping, Optional, TypeVar
import uuid

from phoenix import behaviour, registry, routers
from phoenix.actor import cell
from phoenix.actor.actor import ActorContext
from phoenix.actor.cell import ActorCell, BootstrapActorCell
from phoenix.actor.timers.asyncio import AsyncioScheduler
from phoenix.behaviour import Behaviour
from phoenix.dispatchers import dispatcher as dispatchermsg
from phoenix.dispatchers.coro import CoroDispatcher
from phoenix.mailbox import BoundedMailbox
from phoenix.persistence.store import SqliteStore
from phoenix.ref import Ref
from phoenix.result import Success
from phoenix.system.messages import (
    ActorSpawned,
    ActorStopped,
    Confirmation,
    SpawnActor,
    SpawnSystemActor,
    StopActor,
    WatchActor,
)

logger = logging.getLogger(__name__)


@attr.s
class Watcher:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    msg: Any = attr.ib()


@attr.s
class SystemState:
    context: ActorContext = attr.ib(validator=instance_of(ActorContext))
    dispatchers: Mapping[str, Ref] = attr.ib(
        validator=deep_mapping(
            key_validator=instance_of(str), value_validator=instance_of(Ref)
        )
    )
    """
    Mapping of dispatcher name to actor reference.
    """

    stopped: Iterable[Ref] = attr.ib(
        validator=deep_iterable(
            member_validator=instance_of(Ref), iterable_validator=instance_of(PVector)
        )
    )
    """
    Collection of stopped actors for the purposes of
    notifying watchers if their watch request is made
    after the actor has already stopped.
    """

    watchers: Mapping[Ref, Watcher] = attr.ib(
        validator=deep_mapping(
            key_validator=instance_of(Ref), value_validator=instance_of(Watcher)
        )
    )
    """
    Mapping from watched to watcher (i.e. child to parent).
    """
    actor_dispatcher: Mapping[Ref, Ref] = attr.ib(
        validator=deep_mapping(
            key_validator=instance_of(Ref), value_validator=instance_of(Ref)
        )
    )
    actor_hierarchy: Mapping[Ref, Iterable[Ref]] = attr.ib(
        validator=deep_mapping(
            key_validator=instance_of(Ref),
            value_validator=deep_iterable(member_validator=instance_of(Ref)),
        )
    )


@attr.s
class Shutdown:
    reply_to: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorSystem:
    user: Callable[[], Behaviour] = attr.ib()
    default_dispatcher: Callable[[], Behaviour] = attr.ib(default=CoroDispatcher.start)
    db_url: str = attr.ib(default="sqlite:///db")
    user_ref: Optional[Ref] = attr.ib(init=False, default=None)
    system_ref: Optional[Ref] = attr.ib(init=False, default=None)

    system_task: Optional[asyncio.Task] = attr.ib(
        init=False, validator=optional(instance_of(asyncio.Task)), default=None
    )
    default_dispatcher_task: Optional[asyncio.Task] = attr.ib(
        init=False, validator=optional(instance_of(asyncio.Task)), default=None
    )
    root_task: Optional[asyncio.Task] = attr.ib(
        init=False, validator=optional(instance_of(asyncio.Task)), default=None
    )
    registry_task: Optional[asyncio.Task] = attr.ib(
        init=False, validator=optional(instance_of(asyncio.Task)), default=None
    )

    async def start(self):
        """
        System actor manages spawning and stopping actors.

        When an actor stops it sends a message to the system
        notifying it of this fact. The system then organises
        for the children of that actor and their children and
        so on to be stopped (unless told otherwise).

        This also happens when stopping an actor deliberately 
        from another actor.
        """

        def active(state: SystemState) -> Behaviour:
            async def f(msg):
                dispatch_namespace = {}

                @dispatch(SpawnActor, namespace=dispatch_namespace)
                async def handle(msg: SpawnActor):
                    dispatcher = state.dispatchers[msg.dispatcher or "default"]
                    if msg.parent not in state.actor_hierarchy:
                        raise ValueError(f"Parent actor `{msg.parent}` does not exist.")
                    response = await dispatcher.ask(
                        lambda reply_to: dispatchermsg.SpawnActor(
                            reply_to=reply_to,
                            id=msg.id,
                            behaviour=msg.behaviour,
                            parent=msg.parent,
                            mailbox=msg.mailbox,
                        )
                    )
                    await msg.reply_to.tell(ActorSpawned(ref=response.ref))

                    actor_hierarchy = state.actor_hierarchy.set(response.ref, v())
                    children = state.actor_hierarchy[msg.parent]
                    actor_hierarchy = actor_hierarchy.set(
                        msg.parent, children.append(response.ref)
                    )
                    return active(
                        attr.evolve(
                            state,
                            actor_dispatcher=state.actor_dispatcher.set(
                                response.ref, dispatcher
                            ),
                            actor_hierarchy=actor_hierarchy,
                        )
                    )

                @dispatch(StopActor, namespace=dispatch_namespace)
                async def handle(msg: StopActor):
                    # TODO: drain messages from mailboxes
                    # Stop all descendants recursively
                    refs = [msg.ref]
                    descendants = v()
                    actor_hierarchy = state.actor_hierarchy
                    actor_dispatcher = state.actor_dispatcher
                    while refs:
                        ref = refs.pop()
                        children = state.actor_hierarchy.get(ref, v())
                        descendants = descendants.append(ref)
                        refs.extend(children)
                        actor_hierarchy = actor_hierarchy.discard(ref)
                        actor_dispatcher = actor_dispatcher.remove(ref)

                    dispatchers = [state.actor_dispatcher[ref] for ref in descendants]

                    aws = [
                        disp.ask(
                            lambda reply_to, ref=ref: dispatchermsg.StopActor(
                                reply_to=reply_to, ref=ref
                            )
                        )
                        for disp, ref in zip(dispatchers, descendants)
                    ]
                    # TODO: check for valid return values
                    await asyncio.gather(*aws)

                    try:
                        watcher = state.watchers[msg.ref]
                    except KeyError:
                        watcher = None

                    # Remove all watchers
                    watchers = state.watchers
                    for ref in descendants:
                        watchers = watchers.discard(ref)

                    # Notify watcher
                    # We do not notify all descendants watchers as those can only
                    # be their already-killed parents.
                    if watcher:
                        await watcher.ref.tell(watcher.msg)

                    # Only store top-level actor here as descendants
                    # cannot watch for stopped actors if they themselves
                    # have been stopped.
                    stopped = state.stopped.append(msg.ref)

                    await msg.reply_to.tell(ActorStopped(ref=msg.ref))

                    return active(
                        attr.evolve(
                            state,
                            actor_dispatcher=actor_dispatcher,
                            actor_hierarchy=actor_hierarchy,
                            stopped=stopped,
                            watchers=watchers,
                        )
                    )

                @dispatch(cell.ActorStopped, namespace=dispatch_namespace)
                async def handle(msg: cell.ActorStopped):
                    # TODO: drain messages from mailboxes
                    # Stop all descendants recursively
                    refs = [msg.ref]
                    descendants = v()
                    actor_hierarchy = state.actor_hierarchy
                    actor_dispatcher = state.actor_dispatcher
                    while refs:
                        ref = refs.pop()
                        children = state.actor_hierarchy.get(ref, v())
                        descendants = descendants.append(ref)
                        refs.extend(children)
                        actor_hierarchy = actor_hierarchy.discard(ref)
                        actor_dispatcher = actor_dispatcher.remove(ref)

                    descendants = descendants.remove(msg.ref)
                    dispatchers = [state.actor_dispatcher[ref] for ref in descendants]
                    dispatcher = state.actor_dispatcher[msg.ref]

                    aws = [
                        dispatcher.ask(
                            lambda reply_to: dispatchermsg.RemoveActor(
                                reply_to=reply_to, ref=msg.ref
                            )
                        )
                    ] + [
                        disp.ask(
                            lambda reply_to, ref=ref: dispatchermsg.StopActor(
                                reply_to=reply_to, ref=ref
                            )
                        )
                        for disp, ref in zip(dispatchers, descendants)
                    ]
                    # TODO: check for valid return values
                    await asyncio.gather(*aws)

                    try:
                        watcher = state.watchers[msg.ref]
                    except KeyError:
                        watcher = None

                    # Remove all watchers
                    watchers = state.watchers
                    for ref in descendants.append(msg.ref):
                        watchers = watchers.discard(ref)

                    # Notify watcher
                    # We do not notify all child watchers as those
                    # can only be their already-killed parents.
                    if watcher:
                        await watcher.ref.tell(watcher.msg)

                    # Only store top-level actor here as descendants
                    # cannot watch for stopped actors if they themselves
                    # have been stopped.
                    stopped = state.stopped.append(msg.ref)

                    # ActorStopped messages do not expect replies.

                    return active(
                        attr.evolve(
                            state,
                            actor_dispatcher=actor_dispatcher,
                            actor_hierarchy=actor_hierarchy,
                            stopped=stopped,
                            watchers=watchers,
                        )
                    )

                @dispatch(WatchActor, namespace=dispatch_namespace)
                async def handle(msg: WatchActor):
                    await msg.reply_to.tell(Confirmation())
                    if msg.ref in state.stopped:
                        await msg.parent.tell(msg.message)
                        return behaviour.same()
                    else:
                        return active(
                            attr.evolve(
                                state,
                                watchers=state.watchers.set(
                                    msg.ref, Watcher(ref=msg.parent, msg=msg.message)
                                ),
                            )
                        )

                @dispatch(Shutdown, namespace=dispatch_namespace)
                async def handle(msg: Shutdown):
                    for dispatcher in state.dispatchers.values():
                        await dispatcher.ask(
                            lambda reply_to: dispatchermsg.Shutdown(reply_to=reply_to)
                        )

                    await msg.reply_to.tell(Success(None))

                    return behaviour.stop()

                return await handle(msg)

            return behaviour.receive(f)

        def start(default_dispatcher: Ref, root: Ref) -> Behaviour:
            async def f(context: ActorContext):
                return active(
                    SystemState(
                        context=context,
                        dispatchers=m(default=default_dispatcher),
                        actor_dispatcher=m(),
                        actor_hierarchy=m().set(root, v()).set(context.ref, v()),
                        stopped=v(),
                        watchers=m(),
                    )
                )

            return behaviour.setup(f)

        root_ref = Ref(
            id="root", inbox=janus.Queue(), thread=threading.current_thread()
        )
        system_ref = Ref(
            id="system", inbox=janus.Queue(), thread=threading.current_thread()
        )
        default_dispatcher_ref = Ref(
            id="dispatcher-default",
            inbox=janus.Queue(),
            thread=threading.current_thread(),
        )
        registry_ref = Ref(
            id="registry", inbox=janus.Queue(), thread=threading.current_thread()
        )

        # FIXME: What happens when the application is asked to shut down?
        # FIXME: How do these bootstrapped actors stop gracefully?

        root_cell = BootstrapActorCell(
            behaviour=behaviour.ignore(),
            context=ActorContext(
                ref=root_ref,
                parent=None,
                thread=threading.current_thread(),
                loop=asyncio.get_event_loop(),
                system=system_ref,
                registry=registry_ref,
                timers=AsyncioScheduler(ref=root_ref, lock=asyncio.Lock()),
            ),
        )

        default_dispatcher_cell = BootstrapActorCell(
            behaviour=self.default_dispatcher(),
            context=ActorContext(
                ref=default_dispatcher_ref,
                parent=system_ref,
                thread=threading.current_thread(),
                loop=asyncio.get_event_loop(),
                system=system_ref,
                registry=registry_ref,
                timers=AsyncioScheduler(
                    ref=default_dispatcher_ref, lock=asyncio.Lock()
                ),
            ),
        )
        system_cell = BootstrapActorCell(
            behaviour=start(default_dispatcher_ref, root_ref),
            context=ActorContext(
                ref=system_ref,
                parent=root_ref,
                thread=threading.current_thread(),
                loop=asyncio.get_event_loop(),
                system=system_ref,  # self-reference
                registry=registry_ref,
                timers=AsyncioScheduler(ref=system_ref, lock=asyncio.Lock()),
            ),
        )
        registry_cell = BootstrapActorCell(
            behaviour=registry.Registry.start(),
            context=ActorContext(
                ref=registry_ref,
                parent=system_ref,
                thread=threading.current_thread(),
                loop=asyncio.get_event_loop(),
                system=system_ref,
                registry=None,
                timers=AsyncioScheduler(ref=registry_ref, lock=asyncio.Lock()),
            ),
        )

        self.system_task = asyncio.create_task(system_cell.run(), name="system")
        self.default_dispatcher_task = asyncio.create_task(
            default_dispatcher_cell.run(), name="default-dispatcher"
        )
        self.root_task = asyncio.create_task(root_cell.run(), name="root")
        self.registry_task = asyncio.create_task(registry_cell.run(), name="registry")

        # TODO: persister only started when persistence first used.
        persister_ref = await system_ref.ask(
            lambda reply_to: SpawnActor(
                reply_to=reply_to,
                id="persister",
                behaviour=partial(SqliteStore.start, self.db_url),
                dispatcher=None,
                parent=system_ref,
                # TODO: allow this to be configured
                mailbox=BoundedMailbox(100),
            )
        )
        await registry_ref.tell(
            registry.Register(key="persister", ref=persister_ref.ref)
        )

        user_ref = await system_ref.ask(
            lambda reply_to: SpawnActor(
                reply_to=reply_to,
                id="user",
                behaviour=self.user,
                dispatcher=None,
                parent=root_ref,
                # TODO: allow this to be configured
                mailbox=BoundedMailbox(100),
            )
        )

        self.user_ref = user_ref.ref
        self.system_ref = system_ref

    async def run(self):
        await asyncio.gather(
            self.system_task,
            self.default_dispatcher_task,
            self.root_task,
            self.registry_task,
        )

    async def tell(self, msg: Any):
        await self.user_ref.tell(msg)

    async def ask(
        self, f: Callable[["Ref"], Any], timeout: Optional[timedelta] = None
    ) -> Any:
        return await self.user_ref.ask(f, timeout=timeout)

    async def shutdown(self):
        # TODO: clean shutdown
        # 1. Shutdown user actors
        # 2. Shutdown dispatchers
        # 3. Shutdown system actor
        # 4. Shutdown registry actor
        # 5. Shutdown root actor
        await self.system_ref.ask(lambda reply_to: Shutdown(reply_to=reply_to))
        self.registry_task.cancel()
        self.root_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.registry_task
        with contextlib.suppress(asyncio.CancelledError):
            await self.root_task


async def system(
    user: Callable[[], Behaviour],
    default_dispatcher: Callable[[], Behaviour] = CoroDispatcher.start,
    db_url="sqlite:///db",
) -> ActorSystem:
    """
    Factory for creating and running an ActorSystem.
    """
    actor_system = ActorSystem(
        user=user, default_dispatcher=default_dispatcher, db_url=db_url
    )
    await actor_system.start()
    return actor_system
