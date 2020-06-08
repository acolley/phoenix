from pyrsistent import v
from typing import Any, Callable, List

from phoenix import behaviour
from phoenix.actor import Ref
from phoenix.behaviour import Behaviour


class PoolRouter:
    """
    Route messages to each worker actor
    using round robin.
    """

    @staticmethod
    def start(worker_behaviour: Behaviour, size: int) -> Behaviour:
        async def f(spawn):
            workers = v()
            for _ in range(size):
                worker = await spawn(worker_behaviour)
                workers = workers.append(worker)
            return PoolRouter.work(workers, 0)

        return behaviour.setup(f)

    @staticmethod
    def work(workers, index: int) -> Behaviour:
        async def f(message: Any):
            print(f"Worker: {index}")
            worker = workers[index]
            await worker.tell(message)
            return PoolRouter.work(workers, (index + 1) % len(workers))

        return behaviour.receive(f)


def pool(size: int) -> Callable[[], Callable[[], Behaviour]]:
    def _factory(worker_behaviour: Behaviour) -> Behaviour:
        return PoolRouter.start(worker_behaviour, size)

    return _factory
