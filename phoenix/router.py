import attr
from typing import Any


@attr.s
class Router:
    actors = attr.ib()
    index: int = attr.ib()


def start(actor_start, count: int):
    async def _inner(ctx):
        actors = [await ctx.spawn(actor_start) for _ in range(count)]
        return Router(actors=actors, index=0)

    return _inner


async def handle(state: Router, msg: Any) -> Router:
    await state.actors[state.index].cast(msg)
    state.index = (state.index + 1) % len(state.actors)
    return state
