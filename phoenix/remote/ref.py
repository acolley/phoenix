import attr
from attr.validators import instance_of
from datetime import timedelta
from typing import Any, Callable, Optional

from phoenix.ref import ActorRef, LocalRef
from phoenix.path import ActorPath
from phoenix.remote.remote import Tell


@attr.s(frozen=True)
class RemoteRef(ActorRef):
    id: str = attr.ib(validator=instance_of(str))
    path: ActorPath = attr.ib(validator=instance_of(ActorPath))
    """
    ActorPath of the remote reference.
    """

    remote: LocalRef = attr.ib(validator=instance_of(LocalRef))
    """
    The local actor mediating communications with the remote
    actor.
    """

    async def tell(self, msg: Any):
        await self.remote.tell(Tell(path=self.path, msg=msg))

    async def ask(
        self, f: Callable[[ActorRef], Any], timeout: Optional[timedelta] = None
    ):
        return await self.remote.ask(f=f, timeout=timeout)
