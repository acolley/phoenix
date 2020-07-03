import attr
from attr.validators import instance_of
import janus


class UnboundedMailbox:
    def __call__(self) -> janus.Queue:
        return janus.Queue()


@attr.s
class BoundedMailbox:
    maxsize: int = attr.ib(validator=instance_of(int))

    def __call__(self) -> janus.Queue:
        return janus.Queue(maxsize=self.maxsize)
