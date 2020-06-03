import abc
import asyncio
from asyncio import Queue, Task
import attr
import logging
from multipledispatch import dispatch
from pyrsistent import PRecord, field
import traceback
from typing import Any, Callable, Optional, Union

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
    
    async def ask(self, f: Callable[["Ref"], Any], timeout: Optional[Union[float, int]]=None) -> Any:
        """
        """
        # Create a fake actor for the destination to reply to.
        ref = Ref(Queue())
        msg = f(ref)
        async def interact() -> Any:
            await self.inbox.put(msg)
            return await ref.inbox.get()
        return await asyncio.wait_for(interact(), timeout=timeout)


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

        async def execute(ref: Ref, factory: Callable[[], ActorBase], restarts: int) -> Actor:
            actor = factory()
            executor = Executor(actor.start(), ref, restarts)
            task = asyncio.create_task(executor.run())
            return Actor(factory=factory, ref=ref, task=task)

        @dispatch(ActorSpawned)
        async def handle(msg: ActorSpawned):
            actor = await execute(ref=msg.ref, factory=msg.factory, restarts=0)
            actors[msg.ref] = actor
            logging.debug("Actor spawned: %s", actor)
        
        @dispatch(ActorFailed)
        async def handle(msg: ActorFailed):
            actor = actors.pop(msg.ref)
            logging.debug("Actor failed: %s %s", actor, msg.exc)
        
        @dispatch(ActorRestarted)
        async def handle(msg: ActorRestarted):
            actor = actors[msg.ref]
            actor = await execute(ref=actor.ref, factory=actor.factory, restarts=msg.restarts)
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




class EchoMsg:

    def __init__(self, reply_to, message):
        self.reply_to = reply_to
        self.message = message
    # reply_to: Ref = attr.ib()
    # message: str = attr.ib()


class Echo(ActorBase):

    def start(self) -> Behaviour:
        async def f(message):
            await message.reply_to.tell(message.message)
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
            
            echo = await spawn(Echo)
            reply = await echo.ask(lambda reply_to: EchoMsg(reply_to, "Echooooo"))
            print(reply)

            return behaviour.ignore()

        return behaviour.setup(f)


def main():
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(system(PingPong), debug=True)
