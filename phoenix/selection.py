import attr
from attr.validators import deep_iterable, instance_of, optional
from datetime import timedelta
from pyrsistent import PVector, freeze, v
from typing import Any, Callable, Iterable, Optional, Union
from yarl import URL

from phoenix.address import Address
from phoenix.path import ActorPath
from phoenix.ref import ActorRef, LocalRef
from phoenix.remote.ref import RemoteRef


@attr.s(frozen=True)
class Root:
    def __str__(self) -> str:
        return ""


@attr.s(frozen=True)
class Broadcast:
    def __str__(self) -> str:
        return "*"


@attr.s(frozen=True)
class ActorId:
    value: str = attr.ib(validator=instance_of(str))

    def __str__(self) -> str:
        return self.value


Element = Union[Broadcast, ActorId]


def element_for(s: str) -> Element:
    if s == "*":
        return Broadcast()
    else:
        return ActorId(s)


@attr.s(frozen=True)
class ActorSelection:
    anchor: ActorRef = attr.ib(validator=instance_of(ActorRef))
    address: Optional[Address] = attr.ib(validator=optional(instance_of(Address)))
    elements: Iterable[Element] = attr.ib(
        validator=deep_iterable(
            member_validator=instance_of((Broadcast, ActorId)),
            iterable_validator=instance_of(PVector),
        )
    )

    @classmethod
    def from_url(cls, anchor: ActorRef, url: str) -> "ActorSelection":
        url = URL(url)

        if url.scheme and url.scheme != "phoenix":
            raise ValueError(f"Invalid scheme: `{url.scheme}`")

        if url.host:
            if not url.port:
                raise ValueError(
                    "If host is specified then port must also be specified."
                )

            if not url.user:
                raise ValueError(
                    "If host is specified then system name must also be specified."
                )

            address = Address(host=url.host, port=url.port)
        else:
            address = None

        return cls(
            anchor=anchor,
            address=address,
            elements=v(*[element_for(x) for x in url.parts]),
        )

    @property
    def url(self) -> str:
        path = "/".join(str(elem) for elem in self.elements)
        return f"phoenix://{self.address.host}:{self.address.port}/{path}"

    async def resolve_one(self) -> Optional[ActorRef]:
        """
        Resolve this selection to an ActorRef.

        Raises:
            ValueError: if the selection resolves to more
                than one ActorRef.
        """
        if any(isinstance(x, Broadcast) for x in self.elements):
            raise ValueError("Selection does not specify a unique ActorRef.")

        if self.address is None or self.address == self.anchor.path.address:
            # Local

            if isinstance(self.elements[0], Root):
                # Absolute
                actor = self.anchor.root
                for elem in self.elements[1:]:
                    try:
                        actor = next(
                            child for child in actor.children if child.id == elem.value
                        )
                    except StopIteration:
                        return None
                return actor
            else:
                # Relative
                actor = self.anchor
                for elem in self.elements:
                    try:
                        actor = next(
                            child for child in actor.children if child.id == elem.value
                        )
                    except StopIteration:
                        return None
                return actor
        else:
            # Remote
            return RemoteRef(
                id=self.elements[-1].value,
                path=ActorPath.from_url(self.url),
                remote=self.anchor.remote,
            )
