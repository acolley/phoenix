import pytest

from phoenix import behaviour
from phoenix.ref import Ref
from phoenix.testing.kit import ActorTestKit


class Ping:
    @staticmethod
    def start() -> behaviour.Behaviour:
        async def recv(msg):
            m, reply_to = msg
            await reply_to.tell(m)
            return behaviour.stop()

        return behaviour.receive(recv)


@pytest.mark.asyncio
async def test_ping(actor_test_kit: ActorTestKit):
    ping = await actor_test_kit.spawn(Ping.start)
    probe = await actor_test_kit.create_probe()
    await ping.tell(("Hello", probe.ref))
    assert await probe.expect_message("Hello")
