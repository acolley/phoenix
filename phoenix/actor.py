import abc
import asyncio
from asyncio import Queue
import attr
from typing import Any, Callable, Optional, Union

from phoenix.behaviour import Behaviour


# Intended as public interface for actor framework.
class ActorBase(abc.ABC):
    
    @abc.abstractmethod
    def start(self) -> Behaviour:
        raise NotImplementedError


@attr.s(frozen=True)
class Ref:
    inbox: Queue = attr.ib()

    async def tell(self, message: Any):
        await self.inbox.put(message)
    
    async def ask(self, f: Callable[["Ref"], Any], timeout: Optional[Union[float, int]]=None) -> Any:
        """
        """
        # Create a fake actor for the destination to reply to.
        ref = Ref(Queue())
        msg = f(ref)
        async def interact() -> Any:
            await self.inbox.put(msg)
            return await ref.inbox.get()
        return await asyncio.wait_for(interact(), timeout=timeout)
