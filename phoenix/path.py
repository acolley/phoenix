import attr
from attr.validators import deep_iterable, instance_of
from pyrsistent import PVector, v
from typing import Iterable
from yarl import URL

from phoenix.address import Address


@attr.s(frozen=True)
class Part:
    value: str = attr.ib(validator=instance_of(str))

    @value.validator
    def check(self, attribute: str, value: str):
        if "/" in self.value:
            raise ValueError("Invalid character `/` in ActorPath Part.")
        if "*" in self.value:
            raise ValueError("Invalid character `*` in ActorPath Part.")


@attr.s(frozen=True)
class ActorPath:
    address: Address = attr.ib(validator=instance_of(Address))
    parts: Iterable[Part] = attr.ib(
        validator=deep_iterable(
            member_validator=instance_of(Part), iterable_validator=instance_of(PVector)
        ),
        default=v(),
    )

    @classmethod
    def from_url(cls, url: str) -> "ActorPath":
        url = URL(url)

        if url.scheme != "phoenix":
            raise ValueError(f"Invalid scheme: `{url.scheme}`")

        address = Address(host=url.host, port=url.port)

        return cls(address=address, parts=v(*map(Part, url.parts)))

    def __truediv__(self, part: str) -> "ActorPath":
        return ActorPath(address=self.address, parts=self.parts.append(Part(part)))

    def __rtruediv__(self, part: str) -> "ActorPath":
        return ActorPath(address=self.address, parts=self.parts.append(Part(part)))
