from functools import partial
from multimethod import multimethod
from typing import Any, List, Tuple

from phoenix.actor import Actor, ActorId, Behaviour, Context
from phoenix.dataclasses import dataclass


@dataclass
class State:
    context: Context
    actors: List[ActorId]
    index: int


async def start(context: Context, workers: int, start) -> Actor:
    actors = [
        await context.spawn(
            start=start,
            name=f"{context.actor_id.value}.{i}",
        )
        for i in range(workers)
    ]
    for actor_id in actors:
        await context.link(context.actor_id, actor_id)

    return Actor(
        state=State(
            context=context,
            actors=actors,
            index=0,
        ),
        handler=handle,
    )


async def handle(state: State, msg: Any) -> Tuple[Behaviour, State]:
    actor_id = state.actors[state.index]
    await state.context.cast(actor_id, msg)
    state.index = (state.index + 1) % len(state.actors)
    return Behaviour.done, state
