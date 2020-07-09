from pyrsistent import pset
import pytest

from phoenix import behaviour
from phoenix.registry import Find, Listing, NotFound, Register, Registry


class TestActor:
    @staticmethod
    def start() -> behaviour.Behaviour:
        return behaviour.ignore()


@pytest.mark.asyncio
async def test_register(actor_test_kit):
    registry = await actor_test_kit.spawn(Registry.start)
    test = await actor_test_kit.spawn(TestActor.start)

    await registry.tell(Register(key="actor", ref=test))

    reply = await registry.ask(lambda reply_to: Find(reply_to=reply_to, key="actor"))

    assert reply == Listing(pset({test}))


@pytest.mark.asyncio
async def test_missing(actor_test_kit):
    registry = await actor_test_kit.spawn(Registry.start)

    reply = await registry.ask(lambda reply_to: Find(reply_to=reply_to, key="missing"))

    assert reply == NotFound(key="missing")
