import pytest
from typing import Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.supervisor import ChildInfo, ChildSpec, RestartStrategy, RestartWhen, Supervisor
from phoenix.system.system import ActorSystem


async def start(context: Context) -> Actor:
    return Actor(state=context, handler=handle)


async def handle(state: Context, msg: None) -> Tuple[Behaviour, Context]:
    return Behaviour.done, state


@pytest.mark.asyncio
async def test_which_children(actor_system: ActorSystem):
    supervisor = await Supervisor.new(
        context=actor_system,
        name="Supervisor",
    )

    children = await supervisor.which_children()

    assert children == []

    await supervisor.init(
        children=[
            ChildSpec(
                start=start,
                options=dict(name="child1"),
                restart_when=RestartWhen.permanent,
            ),
            ChildSpec(
                start=start,
                options=dict(name="child2"),
                restart_when=RestartWhen.permanent,
            ),
        ],
        strategy=RestartStrategy.one_for_one,
    )

    children = await supervisor.which_children()

    assert children == [
        ChildInfo(actor_id=ActorId(system_id="system", value="child1")),
        ChildInfo(actor_id=ActorId(system_id="system", value="child2")),
    ]
