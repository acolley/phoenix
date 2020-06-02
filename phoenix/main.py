import abc
import asyncio
from asyncio import Queue, Task
from multipledispatch import dispatch
from pyrsistent import PRecord, field
from typing import Any

from phoenix import behaviour
from phoenix.behaviour import Behaviour, Ignore, Receive, Setup


class Ref(PRecord):
    inbox: Queue = field(type=Queue)

    async def tell(self, message: Any):
        await self.inbox.put(message)


# Intended as public interface for actor framework.
class ActorBase(abc.ABC):
    
    @abc.abstractmethod
    def start(self) -> Behaviour:
        raise NotImplementedError


async def system(user: ActorBase):

    class Executor:
        def __init__(self, behaviour: Behaviour, ref: Ref):
            self.behaviour = behaviour
            self.ref = ref

        async def run(self):
            while True:
                self.behaviour = await self.execute_behaviour(self.behaviour)
        
        @dispatch(Setup)
        async def execute_behaviour(self, behaviour: Setup):
            return await self.behaviour(spawn)

        @dispatch(Receive)
        async def execute_behaviour(self, behaviour: Receive):
            message = await self.ref.inbox.get()
            return await self.behaviour(message)
        
        @dispatch(Ignore)
        async def execute_behaviour(self, behaviour: Ignore):
            await self.ref.inbox.get()
            return behaviour
        
        # @dispatch(Stop)
        # async def execute_behaviour(self, behaviour: Stop):
        #     await self.system.put(ScheduleStop(ref=self.ref))


    class Actor(PRecord):
        ref: Ref = field(type=Ref)
        task: Task = field(type=Task)

    spawned = Queue()
    async def spawn(actor: ActorBase) -> Ref:
        ref = Ref(inbox=Queue())
        executor = Executor(actor.start(), ref)
        task = asyncio.create_task(executor.run())
        await spawned.put(Actor(ref=ref, task=task))
        return ref


    async def supervise():
        actors = []
        while True:
            get = spawned.get()
            aws = [get] + [x.task for x in actors]
            for coro in asyncio.as_completed(aws):
                result = await coro
                if isinstance(result, Actor):
                    actors.append(result)
                else:
                    to_remove = [x for x in actors if x.task.done()]
                    assert len(to_remove) > 0
                    # Otherwise we can't guarantee a match between the result
                    # and the finished task.
                    # FIXME: there must be a better way to do this as
                    # this could go wrong if there are lots of actors.
                    assert len(to_remove) == 1
                    actors = [x for x in actors if x is not to_remove[0]]
                break
    
    await spawn(user)
    
    task = asyncio.create_task(supervise())
    await task


class Greeter(ActorBase):
    def __init__(self, greeting: str):
        self.greeting = greeting

    @behaviour.receive_decorator
    async def start(self, message):
        if message is None:
            return behaviour.same()

        print(f"{self.greeting} {message}")
        return behaviour.same()


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
            return self.ping()

        return behaviour.receive(f)


class Pong(ActorBase):
    def __init__(self):
        self.ping = None
        self.count = 0
    
    def start(self) -> Behaviour:
        async def f(ping: Ref) -> Behaviour:
            self.ping = ping
            return self.pong()

        return behaviour.receive(f)
    
    def pong(self) -> Behaviour:
        async def f(message: str) -> Behaviour:
            print(message)
            if self.count > 5:
                raise Exception
            await self.ping.tell("pong")
            self.count += 1
            await asyncio.sleep(1)
            return self.pong()

        return behaviour.receive(f)


class PingPong(ActorBase):

    def __init__(self):
        self.ping = None
        self.pong = None
    
    def start(self) -> Behaviour:
        async def f(spawn):
            self.ping = await spawn(Ping())
            self.pong = await spawn(Pong())
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
    asyncio.run(system(PingPong()))


# system = ActorSystem(Greeter("Hello"), "hello")
# asyncio.run(system.run())