import asyncio
from asyncio import Queue, Task
import attr
from attr.validators import instance_of, optional
import logging
from multipledispatch import dispatch
from pyrsistent import PRecord, field
import traceback
from typing import Callable, Optional

from phoenix.actor import Ref
from phoenix.behaviour import Behaviour, Ignore, Receive, Restart, Same, Setup, Stop


async def system(user: Callable[[], Callable[[], Behaviour]]):

    class ActorFailed(PRecord):
        ref: Ref = field(type=Ref)
        exc: Exception = field(type=Exception)
    
    class ActorStopped(PRecord):
        ref: Ref = field(type=Ref)
    
    class ActorKilled(PRecord):
        ref: Ref = field(type=Ref)

    @attr.s
    class ActorSpawned:
        parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
        factory = attr.ib()
        ref: Ref = attr.ib(validator=instance_of(Ref))
    
    class ActorRestarted(PRecord):
        ref: Ref = field(type=Ref)
        restarts: int = field(type=int)

    class RestartActor(Exception):
        pass
    
    class StopActor(Exception):
        pass

    class Executor:
        """
        Responsible for executing the behaviours returned
        by the actor's starting behaviour.
        """
        def __init__(self, start: Behaviour, ref: Ref, restarts: int):
            self.ref = ref
            self.behaviours = [start]
            self.restarts = restarts

        async def run(self):
            """
            Execute the actor in a coroutine.

            Returns terminating behaviours for the actor
            so that the supervisor for this actor knows
            how to react to a termination.
            """
            # A stack of actor behaviours.
            # The top of the stack determines
            # what the behaviour will be on
            # the next loop cycle.
            try:
                while self.behaviours:
                    current = self.behaviours[-1]
                    try:
                        await self.execute(current)
                    except RestartActor:
                        return ActorRestarted(ref=self.ref, restarts=self.restarts + 1)
                    except StopActor:
                        return ActorStopped(ref=self.ref)
                    except Exception as e:
                        traceback.print_exc()
                        return ActorFailed(ref=self.ref, exc=e)
            except asyncio.CancelledError:
                # We were cancelled by the supervisor
                # TODO: Allow the actor to perform cleanup.
                return ActorKilled(ref=self.ref)
        
        @dispatch(Setup)
        async def execute(self, behaviour: Setup):
            async def _spawn(factory: Callable[[], Callable[[], Behaviour]]) -> Ref:
                return await spawn(self.ref, Queue(), factory)
            next_ = await behaviour(_spawn)
            self.behaviours.append(next_)

        @dispatch(Receive)
        async def execute(self, behaviour: Receive):
            message = await self.ref.inbox.get()
            logging.debug("Message received: %s: %s", self.ref, message)
            next_ = await behaviour(message)
            self.ref.inbox.task_done()
            self.behaviours.append(next_)
        
        @dispatch(Ignore)
        async def execute(self, behaviour: Ignore):
            await self.ref.inbox.get()
            self.ref.inbox.task_done()
        
        @dispatch(Restart)
        async def execute(self, behaviour: Restart):
            try:
                await self.execute(behaviour.behaviour)
            except Exception:
                if self.restarts >= behaviour.max_restarts:
                    raise
                raise RestartActor
        
        @dispatch(Same)
        async def execute(self, behaviour: Same):
            if len(self.behaviours) <= 1:
                raise ValueError("Same behaviour requires an enclosing behaviour.")
            self.behaviours.pop()
        
        @dispatch(Stop)
        async def execute(self, behaviour: Stop):
            raise StopActor

    @attr.s
    class Actor:
        parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
        factory = attr.ib()
        ref: Ref = attr.ib(validator=instance_of(Ref))
        task: Task = attr.ib(validator=instance_of(Task))
    
    supervisor = Queue()

    async def spawn(parent: Optional[Ref], inbox: Queue, factory: Callable[[], Callable[[], Behaviour]]) -> Ref:
        ref = Ref(inbox=inbox)
        await supervisor.put(ActorSpawned(parent=parent, factory=factory, ref=ref))
        return ref

    async def supervise():
        actors = {}

        async def execute(parent: Optional[Ref], ref: Ref, factory: Callable[[], Callable[[], Behaviour]], restarts: int) -> Actor:
            actor = factory()
            executor = Executor(actor(), ref, restarts)
            task = asyncio.create_task(executor.run())
            return Actor(parent=parent, factory=factory, ref=ref, task=task)

        @dispatch(ActorSpawned)
        async def handle(msg: ActorSpawned):
            actor = await execute(parent=msg.parent, ref=msg.ref, factory=msg.factory, restarts=0)
            actors[msg.ref] = actor
            logging.debug("Actor spawned: %s", actor)
        
        @dispatch(ActorFailed)
        async def handle(msg: ActorFailed):
            actor = actors.pop(msg.ref)
            logging.debug("Actor failed: %s %s", actor, msg.exc)
            # TODO: Notify parent

            # Kill children
            children = [x for x in actors.values() if x.parent is actor.ref]
            for child in children:
                child.task.cancel()

        @dispatch(ActorKilled)
        async def handle(msg: ActorKilled):
            actor = actors.pop(msg.ref)
            logging.debug("Actor killed: %s", actor)

            # Kill children
            children = [x for x in actors.values() if x.parent is actor.ref]
            for child in children:
                child.task.cancel()
        
        @dispatch(ActorRestarted)
        async def handle(msg: ActorRestarted):
            actor = actors[msg.ref]
            actor = await execute(parent=actor.parent, ref=actor.ref, factory=actor.factory, restarts=msg.restarts)
            actors[actor.ref] = actor
            logging.debug("Actor restarted: %s", actor)
        
        @dispatch(ActorStopped)
        async def handle(msg: ActorStopped):
            actor = actors.pop(msg.ref)
            logging.debug("Actor stopped: %s", actor)

        while True:
            aws = [supervisor.get()] + [x.task for x in actors.values()]
            for coro in asyncio.as_completed(aws):
                result = await coro
                await handle(result)
                break
    
    await spawn(None, Queue(), user)
    
    task = asyncio.create_task(supervise())
    await task
