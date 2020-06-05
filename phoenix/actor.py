import abc
import asyncio
from asyncio import Queue
import attr
from attr.validators import instance_of
from typing import Any, Callable, Optional, Union
import uuid

from phoenix.behaviour import Behaviour


@attr.s(frozen=True, repr=False)
class Ref:
    id: str = attr.ib(validator=instance_of(str))
    inbox: Queue = attr.ib(validator=instance_of(Queue))

    async def tell(self, message: Any):
        await self.inbox.put(message)

    async def ask(
        self, f: Callable[["Ref"], Any], timeout: Optional[Union[float, int]] = None
    ) -> Any:
        """
        """
        # Create a fake actor for the destination to reply to.
        ref = Ref(id=str(uuid.uuid1()), inbox=Queue())
        msg = f(ref)

        async def interact() -> Any:
            await self.inbox.put(msg)
            return await ref.inbox.get()

        return await asyncio.wait_for(interact(), timeout=timeout)
    
    def __repr__(self) -> str:
        return f"Ref(id={repr(self.id)})"
