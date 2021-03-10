import asyncio
import pytest
from typing import Tuple

from phoenix import ActorId, ActorSystem, Behaviour, Context, DynamicSupervisor, retry
from phoenix.dataclasses import dataclass


@pytest.fixture
async def actor_system() -> ActorSystem:
    system = ActorSystem("system")
    task = asyncio.create_task(system.run())
    yield system
    await system.shutdown()


@pytest.mark.asyncio
async def test_restart(actor_system: ActorSystem):
    @dataclass
    class Message:
        reply_to: ActorId
        msg: str

    async def start(context: Context) -> Context:
        return context

    error = True

    async def handle(state: Context, msg: Message) -> Tuple[Behaviour, Context]:
        nonlocal error
        if error:
            error = False
            raise ValueError
        await state.cast(msg.reply_to, msg.msg)
        return Behaviour.done, state

    supervisor = await DynamicSupervisor.new(
        context=actor_system, name="DynamicSupervisor"
    )
    child_id = await supervisor.start_child(start, handle, dict(name="Hello"))

    resp = await retry()(
        lambda: asyncio.wait_for(
            actor_system.call(
                child_id, lambda reply_to: Message(reply_to=reply_to, msg="hello there")
            ),
            timeout=2,
        )
    )
    assert resp == "hello there"
