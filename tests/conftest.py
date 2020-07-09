import pytest

from phoenix.testing.kit import ActorTestKit


@pytest.fixture
async def actor_test_kit():
    kit = ActorTestKit()
    await kit.start()
    yield kit
    await kit.shutdown()
