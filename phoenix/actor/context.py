import asyncio
import attr
from attr.validators import instance_of, optional
import concurrent.futures
from pyrsistent import v
import threading
from typing import Any, Awaitable, Callable, List, Optional
import uuid

from phoenix.actor.scheduler import Scheduler
from phoenix.behaviour import Behaviour
from phoenix.mailbox import BoundedMailbox, UnboundedMailbox
from phoenix.result import Failure, Success
from phoenix.ref import Ref
from phoenix.system import messages


@attr.s
class ActorContext:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    """
    The Ref of this Actor.
    """

    parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
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

    executor: concurrent.futures.Executor = attr.ib(
        validator=instance_of(concurrent.futures.Executor)
    )

    system: Ref = attr.ib(validator=instance_of(Ref))
    """
    The system that this Actor belongs to.
    """

    registry: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))

    timers: Scheduler = attr.ib(validator=instance_of(Scheduler))

    children: List[Ref] = attr.ib(default=v())

    @thread.validator
    def check(self, attribute: str, value: threading.Thread):
        if value is not self.ref.thread:
            raise ValueError("thread must be the same as ref.thread")

    @ref.validator
    def check(self, attribute: str, value: Ref):
        if value.thread is not self.thread:
            raise ValueError("ref.thread must be the same as thread")

    async def spawn(
        self,
        behaviour: Behaviour,
        id: Optional[str] = None,
        dispatcher: Optional[str] = None,
        mailbox=UnboundedMailbox(),
    ) -> Ref:
        id = id or str(uuid.uuid1())
        response = await self.system.ask(
            lambda reply_to: messages.SpawnActor(
                reply_to=reply_to,
                id=id,
                behaviour=behaviour,
                dispatcher=dispatcher,
                mailbox=mailbox,
                parent=self.ref,
            )
        )
        self.children = self.children.append(response.ref)
        return response.ref

    async def stop(self, ref: Ref):
        # Stop a child actor
        try:
            self.children = self.children.remove(ref)
        except KeyError:
            raise ValueError(f"Must be a child of this actor: `{ref}`")

        await self.system.ask(
            lambda reply_to: messages.StopActor(reply_to=reply_to, ref=ref)
        )

    async def watch(self, ref: Ref, msg: Any):
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

        self.loop.create_task(pipe())
