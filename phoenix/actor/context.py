import asyncio
import attr
from attr.validators import instance_of, optional
from pyrsistent import v
import threading
from typing import Any, Awaitable, Callable, List, Optional
import uuid

from phoenix.actor.timers import Timers
from phoenix.behaviour import Behaviour
from phoenix.result import Failure, Success
from phoenix.ref import LocalRef
from phoenix.system import messages


@attr.s
class ActorContext:
    ref: LocalRef = attr.ib(validator=instance_of(LocalRef))
    """
    The ref of this Actor.
    """

    parent: Optional[LocalRef] = attr.ib(validator=optional(instance_of(LocalRef)))
    """
    The parent of this Actor.
    """

    thread: threading.Thread = attr.ib(validator=instance_of(threading.Thread))
    """
    The thread that is executing this Actor.
    """

    loop: asyncio.AbstractEventLoop = attr.ib(
        validator=instance_of(asyncio.AbstractEventLoop)
    )
    """
    The event loop that is executing this Actor.
    """

    system: LocalRef = attr.ib(validator=instance_of(LocalRef))
    """
    The system that this Actor belongs to.
    """

    remote: LocalRef = attr.ib(validator=instance_of(LocalRef))

    registry: Optional[LocalRef] = attr.ib(validator=optional(instance_of(LocalRef)))

    timers: Timers = attr.ib(validator=instance_of(Timers))

    children: List[LocalRef] = attr.ib(default=v())

    @thread.validator
    def check(self, attribute: str, value: threading.Thread):
        if value is not self.ref.thread:
            raise ValueError("thread must be the same as ref.thread")

    @ref.validator
    def check(self, attribute: str, value: LocalRef):
        if value.thread is not self.thread:
            raise ValueError("ref.thread must be the same as thread")

    async def spawn(
        self,
        behaviour: Behaviour,
        id: Optional[str] = None,
        dispatcher: Optional[str] = None,
    ) -> LocalRef:
        id = id or str(uuid.uuid1())
        response = await self.system.ask(
            lambda reply_to: messages.SpawnActor(
                reply_to=reply_to,
                id=id,
                behaviour=behaviour,
                dispatcher=dispatcher,
                parent=self.ref,
            )
        )
        self.children = self.children.append(response.ref)
        return response.ref

    async def stop(self, ref: LocalRef):
        # Stop a child actor
        try:
            self.children = self.children.remove(ref)
        except KeyError:
            raise ValueError(f"Must be a child of this actor: `{ref}`")

        await self.system.ask(
            lambda reply_to: messages.StopActor(reply_to=reply_to, ref=ref)
        )

    async def watch(self, ref: LocalRef, msg: Any):
        # Watch a child actor
        if ref not in self.children:
            raise ValueError(f"Must be a child of this actor: `{ref}`")

        await self.system.ask(
            lambda reply_to: messages.WatchActor(
                reply_to=reply_to, ref=ref, parent=self.ref, message=msg,
            )
        )

    async def pipe_to_self(self, awaitable: Awaitable, cb: Callable[[Any], Any]):
        """
        Send a message to self after the awaitable has finished.
        """

        async def pipe():
            try:
                result = await awaitable()
            except Exception as e:
                result = Failure(e)
            else:
                result = Success(result)
            msg = cb(result)
            await self.ref.tell(msg)

        asyncio.create_task(pipe())

    async def select(self, url: str) -> ActorSelection:
        """
        Select actors using a relative or absolute URL.
        """

        return ActorSelection(anchor=self.ref, url=url)
