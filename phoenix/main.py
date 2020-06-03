import abc
import asyncio
from asyncio import Queue, Task
import attr
import logging
from multipledispatch import dispatch
from pyrsistent import PRecord, field
from typing import Any, Callable

from phoenix import behaviour
from phoenix.behaviour import Behaviour, Ignore, Receive, Restart, Same, Setup, Stop


# Intended as public interface for actor framework.
class ActorBase(abc.ABC):
    
    @abc.abstractmethod
    def start(self) -> Behaviour:
        raise NotImplementedError


@attr.s(frozen=True)
class Ref:
    inbox: Queue = attr.ib()

    async def tell(self, message: Any):
        await self.inbox.put(message)


async def system(user: Callable[[], ActorBase]):

    class ActorFailed(PRecord):
        ref: Ref = field(type=Ref)
        exc: Exception = field(type=Exception)
    
    class ActorStopped(PRecord):
        ref: Ref = field(type=Ref)

    class ActorSpawned(PRecord):
        factory = field()
        ref: Ref = field(type=Ref)
    
    class ActorRestarted(PRecord):
        ref: Ref = field(type=Ref)

    class RestartActor(Exception):
        pass
    
    class StopActor(Exception):
        pass

    class Executor:
        def __init__(self, start: Behaviour, ref: Ref):
            self.ref = ref
            self.behaviours = [start]

        async def run(self):
            while self.behaviours:
                current = self.behaviours[-1]
                try:
                    await self.execute(current)
                except RestartActor:
                    return ActorRestarted(ref=self.ref)
                except StopActor:
                    return ActorStopped(ref=self.ref)
                except Exception as e:
                    return ActorFailed(ref=self.ref, exc=e)
        
        @dispatch(Setup)
        async def execute(self, behaviour: Setup):
            async def _spawn(factory: Callable[[], ActorBase]) -> Ref:
                return await spawn(Queue(), factory)
            next_ = await behaviour(_spawn)
            self.behaviours.append(next_)

        @dispatch(Receive)
        async def execute(self, behaviour: Receive):
            message = await self.ref.inbox.get()
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
                raise RestartActor
        
        @dispatch(Same)
        async def execute(self, behaviour: Same):
            if len(self.behaviours) <= 1:
                raise ValueError("Same behaviour requires an enclosing behaviour.")
            self.behaviours.pop()
        
        @dispatch(Stop)
        async def execute(self, behaviour: Stop):
            raise StopActor


    class Actor(PRecord):
        factory = field()
        ref: Ref = field(type=Ref)
        task: Task = field(type=Task)
    
    supervisor = Queue()

    async def spawn(inbox: Queue, factory: Callable[[], ActorBase]) -> Ref:
        ref = Ref(inbox=inbox)
        await supervisor.put(ActorSpawned(factory=factory, ref=ref))
        return ref

    async def supervise():
        actors = {}

        # async def execute()

        @dispatch(ActorSpawned)
        async def handle(msg: ActorSpawned):
            logging.debug("Actor spawned: %s", msg)
            actor = msg.factory()
            executor = Executor(actor.start(), msg.ref)
            task = asyncio.create_task(executor.run())
            actors[msg.ref] = Actor(factory=msg.factory, ref=msg.ref, task=task)
        
        @dispatch(ActorFailed)
        async def handle(msg: ActorFailed):
            actor = actors.pop(msg.ref)
            logging.debug("Actor failed: %s %s", actor, msg.exc)
        
        @dispatch(ActorRestarted)
        async def handle(msg: ActorRestarted):
            actor = actors[msg.ref]
            logging.debug("Actor restarted: %s", actor)
            new = actor.factory()
            executor = Executor(new.start(), actor.ref)
            task = asyncio.create_task(executor.run())
            actors[actor.ref] = Actor(factory=actor.factory, ref=actor.ref, task=task)
        
        @dispatch(ActorStopped)
        async def handle(msg: ActorStopped):
            logging.debug("Actor stopped: %s", msg)
            del actors[msg.ref]

        while True:
            aws = [supervisor.get()] + [x.task for x in actors.values()]
            for coro in asyncio.as_completed(aws):
                result = await coro
                await handle(result)
                break
    
    await spawn(Queue(), user)
    
    task = asyncio.create_task(supervise())
    await task


class Greeter(ActorBase):
    def __init__(self, greeting: str):
        self.greeting = greeting
        self.count = 0

    def start(self) -> Behaviour:
        async def f(message):
            self.count += 1
            print(f"{self.greeting} {message}")
            if self.count >= 5:
                raise Exception("Oh noooooo!!")
            await asyncio.sleep(1)
            return behaviour.same()
        return behaviour.restart(behaviour.receive(f))


class Ping(ActorBase):
    def __init__(self):
        self.pong = None
    
    def start(self) -> Behaviour:
        async def f(pong: Ref) -> Behaviour:
            self.pong = pong
            await self.pong.tell("ping")
            return self.ping()

        return behaviour.receive(f)
    
    def ping(self) -> Behaviour:
        async def f(message: str) -> Behaviour:
            print(message)
            await self.pong.tell("ping")
            await asyncio.sleep(1)
            return behaviour.same()

        return behaviour.receive(f)


class Pong(ActorBase):
    def __init__(self):
        self.ping = None
    
    def start(self) -> Behaviour:
        async def f(ping: Ref) -> Behaviour:
            self.ping = ping
            return self.pong()

        return behaviour.receive(f)
    
    def pong(self) -> Behaviour:
        async def f(message: str) -> Behaviour:
            print(message)
            await self.ping.tell("pong")
            await asyncio.sleep(1)
            return behaviour.same()

        return behaviour.receive(f)


class PingPong(ActorBase):

    def __init__(self):
        self.ping = None
        self.pong = None
    
    def start(self) -> Behaviour:
        async def f(spawn):
            self.greeter = await spawn(lambda: Greeter("Hello"))
            for i in range(100):
                await self.greeter.tell(str(i))
            self.ping = await spawn(Ping)
            self.pong = await spawn(Pong)
            await self.ping.tell(self.pong)
            await self.pong.tell(self.ping)
            return behaviour.ignore()

        return behaviour.setup(f)


# async def main_async():
#     ref = await spawn(PingPong())
#     while True:
#         await asyncio.sleep(1)

    # task1, ref1 = await spawn(Ping())
    # task2, ref2 = await spawn(Pong())
    # await ref1.inbox.put(ref2)
    # await ref2.inbox.put(ref1)
    # await asyncio.gather(task1, task2)

    # task1, ref1 = await spawn(Greeter("Hello"))
    # await ref1.inbox.put("Alasdair")
    # await ref1.inbox.put("Katy")
    # task2, ref2 = await spawn(Greeter("Goodbye"))
    # await ref2.inbox.put("Alasdair")
    # await ref2.inbox.put("Katy")
    # await asyncio.gather(task1, task2)


def main():
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(system(PingPong), debug=True)


# system = ActorSystem(Greeter("Hello"), "hello")
# asyncio.run(system.run())