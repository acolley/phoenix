from phoenix.actor.behaviour import (
    Behaviour,
    Ignore,
    Receive,
    RestartStrategy,
    Same,
    Setup,
    Stop,
    Supervise,
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


def supervise(behaviour) -> Supervise:
    return Supervise(behaviour=behaviour)


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
