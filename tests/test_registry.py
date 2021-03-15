import pytest

from phoenix.dataclasses import dataclass
from phoenix.registry import KeyExists, Registry, start
from phoenix.system.system import ActorSystem


@pytest.fixture
async def actor_system() -> ActorSystem:
    system = ActorSystem("system")
    await system.start()
    yield system
    await system.shutdown()


@pytest.fixture
async def registry(actor_system: ActorSystem) -> Registry:
    registry = await actor_system.spawn(start, name="Registry")
    return Registry(actor_id=registry, context=actor_system)


@pytest.mark.asyncio
async def test_success(registry: Registry):
    await registry.put("key", 101)
    assert await registry.get("key") == 101

    await registry.remove("key")

    assert await registry.get("key") is None


@pytest.mark.asyncio
async def test_put_existing(registry: Registry):
    await registry.put("key", 101)
    with pytest.raises(KeyExists):
        await registry.put("key", 200)


@pytest.mark.asyncio
async def test_remove_missing(registry: Registry):
    with pytest.raises(KeyError):
        await registry.remove("key")
