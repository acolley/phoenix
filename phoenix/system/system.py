import asyncio
from asyncio import Queue, Task
import attr
from attr.validators import deep_iterable, deep_mapping, instance_of, optional
from datetime import timedelta
import janus
import logging
from multipledispatch import dispatch
from pyrsistent import m, v
import threading
import traceback
from typing import Any, Callable, Generic, Iterable, List, Mapping, Optional, TypeVar
import uuid

from phoenix import behaviour
from phoenix.actor import cell
from phoenix.actor.actor import ActorContext
from phoenix.actor.cell import BootstrapActorCell
from phoenix.behaviour import Behaviour
from phoenix.dispatchers import ThreadDispatcher
from phoenix.ref import Ref
from phoenix.system.messages import (
    ActorSpawned,
    ActorStopped,
    Confirmation,
    SpawnActor,
    StopActor,
)

logger = logging.getLogger(__name__)


@attr.s
class SystemState:
    dispatchers: Mapping[str, Ref] = attr.ib(
        validator=deep_mapping(
            key_validator=instance_of(str), value_validator=instance_of(Ref)
        )
    )
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


# TODO: make the system behave like a Ref for testing and simple experiments.
# For example for testing:
# * User creates a system with the Actor under test.
# * Send messages to system to observe behaviour.
async def system(user: Behaviour):
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
            @dispatch(SpawnActor)
            async def system_handle(msg: SpawnActor):
                dispatcher = state.dispatchers[msg.dispatcher or "default"]
                if msg.parent not in state.actor_hierarchy:
                    raise ValueError(f"Parent actor `{msg.parent}` does not exist.")
                response = await dispatcher.ask(
                    lambda reply_to: ThreadDispatcher.SpawnActor(
                        reply_to=reply_to,
                        id=msg.id,
                        behaviour=msg.behaviour,
                        parent=msg.parent,
                    )
                )
                await msg.reply_to.tell(ActorSpawned(ref=response.ref))

                actor_hierarchy = state.actor_hierarchy.set(response.ref, v())
                children = state.actor_hierarchy[msg.parent]
                actor_hierarchy = actor_hierarchy.set(msg.parent, children.append(response.ref))
                return active(
                    attr.evolve(
                        state,
                        actor_dispatcher=state.actor_dispatcher.set(
                            response.ref, dispatcher
                        ),
                        actor_hierarchy=actor_hierarchy,
                    )
                )

            @dispatch(StopActor)
            async def system_handle(msg: StopActor):
                # TODO: notify watchers
                # Stop all descendants recursively
                refs = [msg.ref]
                descendants = v()
                actor_hierarchy = state.actor_hierarchy
                actor_dispatcher = state.actor_dispatcher
                while refs:
                    ref = refs.pop()
                    children = state.actor_hierarchy.get(ref, v())
                    descendants = descendants.extend(children)
                    refs.extend(children)
                    actor_hierarchy = actor_hierarchy.discard(ref)
                    actor_dispatcher = actor_dispatcher.remove(ref)

                refs = [msg.ref] + children
                dispatchers = [state.actor_dispatcher[ref] for ref in refs]

                aws = [
                    dispatcher.ask(
                        # TODO: shared dispatcher messages
                        lambda reply_to, ref=ref: ThreadDispatcher.StopActor(
                            reply_to=reply_to, ref=ref
                        )
                    )
                    for dispatcher, ref in zip(dispatchers, refs)
                ]
                # TODO: check for valid return values
                await asyncio.gather(*aws)

                await msg.reply_to.tell(ActorStopped(ref=msg.ref))

                return active(
                    attr.evolve(
                        state,
                        actor_dispatcher=actor_dispatcher,
                        actor_hierarchy=actor_hierarchy,
                    )
                )

            @dispatch(cell.ActorStopped)
            async def system_handle(msg: cell.ActorStopped):
                # TODO: notify watchers
                # Stop all descendants recursively
                refs = [msg.ref]
                descendants = v()
                actor_hierarchy = state.actor_hierarchy
                actor_dispatcher = state.actor_dispatcher
                while refs:
                    ref = refs.pop()
                    children = state.actor_hierarchy.get(ref, v())
                    descendants = descendants.extend(children)
                    refs.extend(children)
                    actor_hierarchy = actor_hierarchy.discard(ref)
                    actor_dispatcher = actor_dispatcher.remove(ref)

                dispatchers = [state.actor_dispatcher[ref] for ref in children]
                dispatcher = state.actor_dispatcher[msg.ref]

                # FIXME: it is a bit naughty to use cell.ActorStopped as part of this
                aws = [
                    dispatcher.ask(
                        lambda reply_to: ThreadDispatcher.RemoveActor(
                            reply_to=reply_to, ref=msg.ref
                        )
                    )
                ] + [
                    dispatcher.ask(
                        # TODO: shared dispatcher messages
                        lambda reply_to, ref=ref: ThreadDispatcher.StopActor(
                            reply_to=reply_to, ref=ref
                        )
                    )
                    for dispatcher, ref in zip(dispatchers, refs)
                ]
                # TODO: check for valid return values
                await asyncio.gather(*aws)

                # ActorStopped messages do not expect replies.

                return active(
                    attr.evolve(
                        state,
                        actor_dispatcher=actor_dispatcher,
                        actor_hierarchy=actor_hierarchy,
                    )
                )

            return await system_handle(msg)

        return behaviour.receive(f)

    def start(default_dispatcher: Ref, root: Ref) -> Behaviour:
        async def f(context: ActorContext):
            return active(
                SystemState(
                    dispatchers=m(default=default_dispatcher),
                    actor_dispatcher=m(),
                    actor_hierarchy=m().set(root, v()),
                )
            )

        return behaviour.setup(f)

    root_ref = Ref(id="root", inbox=janus.Queue())
    system_ref = Ref(id="system", inbox=janus.Queue())
    default_dispatcher_ref = Ref(id="dispatcher-default", inbox=janus.Queue())

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
        ),
    )

    default_dispatcher_cell = BootstrapActorCell(
        behaviour=ThreadDispatcher.start(2),
        context=ActorContext(
            ref=default_dispatcher_ref,
            parent=system_ref,
            thread=threading.current_thread(),
            loop=asyncio.get_event_loop(),
            system=system_ref,
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
        ),
    )

    system_task = asyncio.create_task(system_cell.run())
    default_dispatcher_task = asyncio.create_task(default_dispatcher_cell.run())
    root_task = asyncio.create_task(root_cell.run())

    await system_ref.ask(
        lambda reply_to: SpawnActor(
            reply_to=reply_to,
            id="user",
            behaviour=user,
            dispatcher=None,
            parent=root_ref,
        )
    )

    await asyncio.gather(system_task, default_dispatcher_task, root_task)
