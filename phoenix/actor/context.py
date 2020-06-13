import asyncio
import attr
from attr.validators import instance_of, optional
from pyrsistent import v
import threading
from typing import Any, List, Optional
import uuid

from phoenix.behaviour import Behaviour
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

    system: Ref = attr.ib(validator=instance_of(Ref))
    """
    The system that this Actor belongs to.
    """

    children: List[Ref] = attr.ib(default=v())

    async def spawn(
        self,
        behaviour: Behaviour,
        id: Optional[str] = None,
        dispatcher: Optional[str] = None,
    ) -> Ref:
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
