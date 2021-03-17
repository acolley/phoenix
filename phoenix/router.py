from functools import partial
from multimethod import multimethod
from typing import Any, List, Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.dataclasses import dataclass
from phoenix.supervisor import ChildSpec, RestartStrategy, RestartWhen, Supervisor


@dataclass
class State:
    context: Context
    actors: List[ActorId]
    index: int


async def start(context: Context, workers: int, start) -> Actor:
    children = [
        ChildSpec(
            start=start,
            options=dict(name=f"{context.actor_id.value}.{i}"),
            restart_when=RestartWhen.permanent,
        )
        for i in range(workers)
    ]
    supervisor = await Supervisor.new(
        context=context,
        name=f"{context.actor_id.value}.Supervisor",
    )
    await supervisor.init(
        children=children,
        strategy=RestartStrategy.one_for_one,
    )
    return Actor(
        state=State(
            context=context,
            actors=[
                ActorId(
                    system_id=context.actor_id.system_id,
                    value=f"{context.actor_id.value}.{i}",
                )
                for i in range(workers)
            ],
            index=0,
        ),
        handler=handle,
    )


async def handle(state: State, msg: Any) -> Tuple[Behaviour, State]:
    actor_id = state.actors[state.index]
    await state.context.cast(actor_id, msg)
    state.index = (state.index + 1) % len(state.actors)
    return Behaviour.done, state


@dataclass
class Router:
    actor_id: ActorId
    context: Context

    @classmethod
    async def new(cls, context, workers: int, start, name=None) -> "Router":
        actor_id = await context.spawn(
            partial(start, workers=workers, start=start),
            name=name,
        )
        return cls(actor_id=actor_id, context=context)

    async def route(self, msg: Any):
        await self.context.cast(self.actor_id, msg)
