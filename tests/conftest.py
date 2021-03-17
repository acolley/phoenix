import pytest

from phoenix.system.system import ActorSystem


@pytest.fixture
async def actor_system():
    system = ActorSystem("system")
    await system.start()
    yield system
    await system.shutdown()
