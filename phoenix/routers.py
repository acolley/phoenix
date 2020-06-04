from typing import Any, Callable

from phoenix import behaviour
from phoenix.actor import ActorBase
from phoenix.behaviour import Behaviour


class PoolRouter(ActorBase):
    """
    Route messages to each worker actor
    using round robin.
    """

    def __init__(self, worker_behaviour: Behaviour, pool_size: int):
        if pool_size < 1:
            raise ValueError("pool_size must be greater than zero")

        self.worker_behaviour = worker_behaviour
        self.pool_size = pool_size
        self.workers = []
        self.index = 0
    
    def start(self) -> Behaviour:
        worker_behaviour = self.worker_behaviour

        class RouterWorker(ActorBase):

            def start(self):
                return worker_behaviour

        async def f(spawn):
            for _ in range(self.pool_size):
                worker = await spawn(RouterWorker)
                self.workers.append(worker)
            return self.work()

        return behaviour.setup(f)
    
    def work(self) -> Behaviour:
        async def f(message: Any):
            print(f"Worker: {self.index}")
            worker = self.workers[self.index]
            await worker.tell(message)
            self.index = (self.index + 1) % len(self.workers)
            return behaviour.same()

        return behaviour.receive(f)


def pool(size: int) -> Callable[[], Callable[[], PoolRouter]]:
    def _factory(behaviour: Behaviour) -> Callable[[], PoolRouter]:
        return lambda: PoolRouter(worker_behaviour=behaviour, pool_size=size)
    return _factory
