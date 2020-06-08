import abc
import asyncio
import attr
from attr.validators import instance_of, optional
import janus
import threading
from typing import Any, Callable, Optional, Union
import uuid


@attr.s(frozen=True, repr=False)
class Ref:
    # For internal purposes an actor is simply a product of
    # a unique identifier, thread/asyncio-safe queue and the
    # thread that it is running on.
    # This allows us to bootstrap an actor outside of the
    # public interface.
    id: str = attr.ib(validator=instance_of(str))
    inbox: janus.Queue = attr.ib(validator=instance_of(janus.Queue))
    _thread: threading.Thread = attr.ib(
        init=False,
        validator=instance_of(threading.Thread),
        default=threading.current_thread(),
    )

    async def tell(self, message: Any):
        if threading.current_thread() is not self._thread:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.inbox.sync_q.put(message)
            )
        else:
            await self.inbox.async_q.put(message)

    async def ask(
        self, f: Callable[["Ref"], Any], timeout: Optional[Union[float, int]] = None
    ) -> Any:
        """
        """
        # Create a fake actor for the destination to reply to.
        ref = Ref(id=str(uuid.uuid1()), inbox=janus.Queue())
        msg = f(ref)

        async def interact() -> Any:
            await self.tell(msg)
            return await ref.inbox.async_q.get()

        return await asyncio.wait_for(interact(), timeout=timeout)

    def __repr__(self) -> str:
        return f"Ref(id={repr(self.id)})"
