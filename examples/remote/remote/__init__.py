import asyncio
import asyncssh
import attr
from multipledispatch import dispatch
import phoenix
from phoenix import ActorRef, ActorSystem
from phoenix.supervisor import Supervisor


@attr.s
class State:
    ssh = attr.ib()


@attr.s
class RunCommand:
    reply_to = attr.ib()
    cmd = attr.ib()


@attr.s
class Read:
    reply_to: ActorRef = attr.ib()


def start(ctx, hostname) -> Supervisor:
    async def _start(ctx):
        supervisor.init(children=[(_start, handle)], strategy=Strategy.one_for_one)
        return supervisor
    supervisor = Supervisor.start(ctx)
    conn = await asyncssh.connect(hostname)
    return State(conn)


@dispatch(State, RunCommand)
async def handle(state: State, msg: RunCommand) -> State:
    result = await state.ssh.run(msg.cmd)
    await msg.reply_to.cast(result)
    return state


@attr.s
class Remote:
    ref = attr.ib()

    async def run_command(self, cmd):
        return await self.ref.call(lambda reply_to: RunCommand(reply_to=reply_to, cmd=cmd))


async def main_async():
    system = ActorSystem()
    remote = Supervisor.start()
    ref = system.spawn(setup("localhost"), handle)
    remote = RemoteActorRef(ref)

    print(await remote.run_command("echo hello"))


def main():
    asyncio.run(main_async())
