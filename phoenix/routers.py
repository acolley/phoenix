import attr
from attr.validators import instance_of
from pyrsistent import v
from typing import Any, Callable, Hashable, List, Optional

from phoenix import behaviour
from phoenix.behaviour import Behaviour
from phoenix.mailbox import UnboundedMailbox
from phoenix.ref import Ref


@attr.s(frozen=True)
class RoundRobin:
    def __call__(self, size: int) -> Callable[[Any], int]:
        index = -1

        def index_for(msg: Any) -> int:
            nonlocal index
            index = (index + 1) % size
            return index

        return index_for


@attr.s(frozen=True)
class ConsistentHashing:
    """
    A routing strategy that routes a message to a worker
    by hashing the message using a provided key function.

    This relies on the number of workers not changing
    otherwise messages may not be routed consistently.
    """

    key_for: Callable[[Any], Hashable] = attr.ib()

    def __call__(self, size: int):
        def index_for(msg: Any):
            key = self.key_for(msg)
            index = hash(key) % size
            return index

        return index_for


class PoolRouter:
    """
    Route messages to each worker actor
    using round robin.
    """

    @staticmethod
    def start(
        worker_behaviour: Behaviour,
        size: int,
        strategy: Callable[[], Callable[[Any], int]],
        mailbox,
    ) -> Behaviour:
        async def f(context):
            workers = v()
            for i in range(size):
                worker = await context.spawn(worker_behaviour, f"{context.ref.id}-{i}", mailbox=mailbox)
                workers = workers.append(worker)
            return PoolRouter.work(workers, strategy(size))

        return behaviour.setup(f)

    @staticmethod
    def work(workers, index_for) -> Behaviour:
        async def f(message: Any):
            index = index_for(message)
            worker = workers[index]
            await worker.tell(message)
            return PoolRouter.work(workers, index_for)

        return behaviour.receive(f)


def pool(
    size: int, strategy: Optional[Callable[[], Callable[[Any], int]]] = None, mailbox = UnboundedMailbox()
) -> Callable[[], Callable[[], Behaviour]]:
    def _factory(worker_behaviour: Behaviour) -> Behaviour:
        return PoolRouter.start(
            worker_behaviour, size, RoundRobin() if strategy is None else strategy, mailbox=mailbox
        )

    return _factory
