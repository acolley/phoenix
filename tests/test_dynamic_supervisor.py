import asyncio
import pytest
from typing import Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.dataclasses import dataclass
from phoenix.retry import retry
from phoenix.supervision.dynamic_supervisor import (
    ChildSpec,
    DynamicSupervisor,
    RestartWhen,
)
from phoenix.system.system import ActorDown, ActorSystem


@pytest.fixture
async def actor_system() -> ActorSystem:
    system = ActorSystem("system")
    await system.start()
    yield system
    await system.shutdown()


@pytest.mark.asyncio
async def test_restart(actor_system: ActorSystem):
    @dataclass
    class Message:
        reply_to: ActorId
        msg: str

    error = True

    async def handle(state: Context, msg: Message) -> Tuple[Behaviour, Context]:
        nonlocal error
        if error:
            error = False
            raise ValueError
        await state.cast(msg.reply_to, msg.msg)
        return Behaviour.done, state

    async def start(context: Context) -> Actor:
        return Actor(state=context, handler=handle)

    supervisor = await DynamicSupervisor.new(
        context=actor_system, name="DynamicSupervisor"
    )
    child_id = await supervisor.start_child(
        ChildSpec(
            start=start, options=dict(name="Hello"), restart_when=RestartWhen.permanent
        )
    )

    resp = await retry()(
        lambda: asyncio.wait_for(
            actor_system.call(
                child_id, lambda reply_to: Message(reply_to=reply_to, msg="hello there")
            ),
            timeout=2,
        )
    )
    assert resp == "hello there"


@pytest.mark.asyncio
async def test_stop_child(actor_system: ActorSystem):
    async def handle(state: Context, msg: None) -> Tuple[Behaviour, Context]:
        raise ValueError

    async def start(context: Context) -> Actor:
        return Actor(state=context, handler=handle)

    supervisor = await DynamicSupervisor.new(
        context=actor_system, name="DynamicSupervisor"
    )
    child_id = await supervisor.start_child(
        ChildSpec(
            start=start, options=dict(name="Hello"), restart_when=RestartWhen.permanent
        )
    )

    await supervisor.stop_child(child_id)

    assert isinstance(actor_system.actors[child_id], ActorDown)
