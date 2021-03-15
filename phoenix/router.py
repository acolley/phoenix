from functools import partial
from multimethod import multimethod
from typing import Any, List, Tuple

from phoenix.actor import ActorId
from phoenix.dataclasses import dataclass


@dataclass
class Router:
    actor_id: ActorId
    context: Context

    @dataclass
    class State:
        context: Context
        actors: List[ActorId]
        index: int

    @classmethod
    async def new(cls, context, workers: int, start, handle, name=None) -> "Router":
        actor_id = await context.spawn(
            partial(cls.start, workers=workers, start=start, handle=handle),
            cls.handle,
            name=name,
        )
        return cls(actor_id=actor_id, context=context)

    @staticmethod
    async def start(context, workers: int, start, handle) -> State:
        children = [
            (start, handle, dict(name=f"{context.actor_id.value}.{i}"))
            for i in range(workers)
        ]
        supervisor = await Supervisor.new(
            context,
            children=children,
            strategy=RestartStrategy.one_for_one,
            name=f"{context.actor_id.value}.Supervisor",
        )
        return Router.State(
            context=context,
            actors=[ActorId(f"{context.actor_id.value}.{i}") for i in range(workers)],
            index=0,
        )

    @staticmethod
    async def handle(state: State, msg: Any) -> Tuple[Behaviour, State]:
        actor_id = state.actors[state.index]
        await state.context.cast(actor_id, msg)
        state.index = (state.index + 1) % len(state.actors)
        return Behaviour.done, state

    async def route(self, msg: Any):
        await self.context.cast(self.actor_id, msg)
