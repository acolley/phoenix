from pyrsistent import v
import pytest

from phoenix import behaviour


class Batcher:
    @staticmethod
    def start() -> behaviour.Behaviour:
        async def stash(buffer):
            return Batcher.batch(buffer, v())

        return behaviour.stash(stash, capacity=3)

    @staticmethod
    def batch(buffer, messages):
        if len(messages) == 3:
            return buffer.unstash(Batcher.flush)

        async def recv(msg):
            return Batcher.batch(buffer, messages.append(msg))

        return behaviour.receive(recv)

    @staticmethod
    def flush() -> behaviour.Behaviour:
        async def recv(msg):
            return
