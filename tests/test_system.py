import asyncio
import pytest
from typing import Any

from phoenix.actor import Actor, ActorId, Behaviour, Context, Shutdown
from phoenix.system.system import ActorDown, ActorStats, ActorSystem, SystemStats


async def start(context: Context) -> Actor:
    return Actor(state=context, handler=handle)


async def handle(state: Context, msg: Any):
    await asyncio.sleep(60)
    return Behaviour.done, state


@pytest.fixture
async def actor_system():
    system = ActorSystem("system")
    await system.start()
    yield system
    await system.shutdown()


@pytest.mark.asyncio
async def test_stop(actor_system: ActorSystem):
    actor_id = await actor_system.spawn(start, name="Actor")
    await actor_system.stop(actor_id)

    assert actor_system.actors[actor_id] == ActorDown(actor_id=actor_id, reason=Shutdown())


@pytest.mark.asyncio
async def test_list_actors(actor_system: ActorSystem):
    await actor_system.spawn(start, name="Actor1")
    await actor_system.spawn(start, name="Actor2")

    actors = await actor_system.list_actors()

    assert actors == {ActorId("system", "Actor1"), ActorId("system", "Actor2")}


@pytest.mark.asyncio
async def test_get_actor_stats(actor_system: ActorSystem):
    actor_id = await actor_system.spawn(start, name="Actor1")

    stats = await actor_system.get_actor_stats(actor_id)

    assert stats == ActorStats(mailbox_size=0)

    await actor_system.cast(actor_id, "hello")
    await actor_system.cast(actor_id, "hello")

    stats = await actor_system.get_actor_stats(actor_id)

    assert stats == ActorStats(mailbox_size=2)


@pytest.mark.asyncio
async def test_get_system_stats(actor_system: ActorSystem):
    await actor_system.spawn(start, name="Actor1")
    await actor_system.spawn(start, name="Actor2")

    stats = await actor_system.get_system_stats()

    assert stats == SystemStats(actor_count=2)
