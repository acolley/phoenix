import attr
from attr.validators import instance_of
from functools import partial
from typing import Any, Tuple

from phoenix import ActorId, Behaviour
from phoenix.supervisor import RestartStrategy, Supervisor


@attr.s
class Router:
    actor_id = attr.ib()
    context = attr.ib()

    @attr.s
    class State:
        context = attr.ib()
        actors = attr.ib()
        index: int = attr.ib()

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
            (start, handle, dict(name=f"{context.actor_id}.{i}"))
            for i in range(workers)
        ]
        supervisor = await Supervisor.new(
            context, name=f"{context.actor_id}.Supervisor"
        )
        await supervisor.init(children=children, strategy=RestartStrategy.one_for_one)
        return Router.State(
            context=context,
            actors=[ActorId(f"{context.actor_id}.{i}") for i in range(workers)],
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
