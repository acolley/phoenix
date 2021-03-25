# Description

Phoenix is an [actor](https://en.wikipedia.org/wiki/Actor_model) framework for Python.

It is heavily-inspired by [Akka](https://akka.io/) and [Elixir](https://elixir-lang.org/).

# Example

```python
import asyncio
from phoenix.actor import Actor, Behaviour, Context
from phoenix.system.system import ActorSystem
from typing import Tuple


async def actor_start(context: Context) -> Actor:
    return Actor(state=context, handler=actor_handle)


async def actor_handle(state: Context, msg: str) -> Tuple[Behaviour, Context]:
    print(msg)
    return Behaviour.done, state


async def main():
    system = ActorSystem("system")
    await system.start()
    actor_id = await system.spawn(actor_start)
    await system.cast("Hello there")
    await asyncio.sleep(2)
    await system.shutdown()


asyncio.run(main())
```
