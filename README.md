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
    await system.cast(actor_id, "Hello there")
    await asyncio.sleep(2)
    await system.shutdown()


asyncio.run(main())
```

# Design

Actors in `phoenix` are functions that return the actor's state.

Actors are spawned and scheduled using an `ActorSystem`, which is the
runtime upon which actors are executed.

Spawning an actor requires a `start` function to be supplied that initialises
the actor and returns its initial state and the `message handler` function that
will return its next state after receiving a message.

Spawning an actor returns an `ActorId` that uniquely identifies the actor allowing
other actors to send messages to it.

`ActorId`s can be included in messages so that receiving actors can
send messages to other actors dynamically.

Actors can `watch` other actors in order to be notified when they exit.

An actor can be `linked` with another actor so that both are `stopped` when either one exits.
