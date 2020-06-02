import inspect
from pyrsistent import PRecord, field
from typing import Union

class Receive(PRecord):

    behaviour = field(invariant=lambda x: (inspect.iscoroutinefunction(x), f"Not a coroutine: {x}"))

    async def __call__(self, message):
        return await self.behaviour(message)


def receive(behaviour) -> Receive:
    return Receive(behaviour=behaviour)


def receive_decorator(behaviour):
    def _receive() -> Receive:
        return Receive(behaviour=behaviour)
    return _receive


class Setup(PRecord):

    behaviour = field(invariant=lambda x: (inspect.iscoroutinefunction(x), f"Not a coroutine: {x}"))

    async def __call__(self, spawn):
        return await self.behaviour(spawn)


def setup(behaviour) -> Setup:
    return Setup(behaviour=behaviour)


def setup_decorator(behaviour):
    def _setup() -> Setup:
        return Setup(behaviour=behaviour)
    return _setup


class Same:
    pass


def same() -> Same:
    return Same()


class Ignore:
    pass


def ignore() -> Ignore:
    return Ignore()


class Stop:
    pass


def stop() -> Stop:
    return Stop()


Behaviour = Union[Stop, Ignore, Setup, Receive]
