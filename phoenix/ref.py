import abc
import asyncio
import attr
from attr.validators import instance_of, optional
from datetime import timedelta
import janus
import logging
import threading
from typing import Any, Callable, Optional, Union
import uuid

logger = logging.getLogger(__name__)


class AskTimeoutError(Exception):
    pass


@attr.s(frozen=True, repr=False)
class Ref:
    # For internal purposes an actor is simply a product of
    # a unique identifier, thread/asyncio-safe queue and the
    # thread that it is running on.
    # This allows us to bootstrap an actor outside of the
    # public interface.
    id: str = attr.ib(validator=instance_of(str))
    inbox: janus.Queue = attr.ib(validator=instance_of(janus.Queue))
    thread: threading.Thread = attr.ib(validator=instance_of(threading.Thread),)

    async def tell(self, message: Any):
        if threading.current_thread() is not self.thread:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.inbox.sync_q.put(message)
            )
        else:
            await self.inbox.async_q.put(message)

    async def ask(
        self, f: Callable[["Ref"], Any], timeout: Optional[timedelta] = None
    ) -> Any:
        """
        """
        # Create a fake actor ref for the destination to reply to.
        reply_to = Ref(
            id=str(uuid.uuid1()), inbox=janus.Queue(), thread=threading.current_thread()
        )
        msg = f(reply_to)

        async def interact() -> Any:
            await self.tell(msg)
            return await reply_to.inbox.async_q.get()

        try:
            return await asyncio.wait_for(
                interact(), timeout=timeout.total_seconds() if timeout else None
            )
        except asyncio.TimeoutError:
            raise AskTimeoutError

    def __repr__(self) -> str:
        return f"Ref(id={repr(self.id)})"
