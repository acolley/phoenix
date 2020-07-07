from phoenix.actor.behaviour import (
    Behaviour,
    Ignore,
    Receive,
    Restart,
    Same,
    Setup,
    Stash,
    Stop,
)
from phoenix.persistence.behaviour import Persist


def receive(f) -> Receive:
    return Receive(f=f)


def setup(f) -> Setup:
    return Setup(f=f)


def same() -> Same:
    return Same()


def ignore() -> Ignore:
    return Ignore()


def stop() -> Stop:
    return Stop()


def restart(behaviour) -> Restart:
    return Restart(behaviour=behaviour)


def persist(
    id: str, empty_state, command_handler, event_handler, encode, decode
) -> Persist:
    return Persist(
        id=id,
        empty_state=empty_state,
        command_handler=command_handler,
        event_handler=event_handler,
        encode=encode,
        decode=decode,
    )


def stash(f, capacity: int) -> Stash:
    return Stash(f=f, capacity=capacity)
