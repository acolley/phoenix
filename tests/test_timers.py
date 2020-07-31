import asyncio
from datetime import timedelta
from functools import partial
import pytest
import sys

from phoenix import behaviour
from phoenix.manual_time_events import ManualTimeEventLoop
from phoenix.ref import Ref


class Test:
    @staticmethod
    def start(ref: Ref) -> behaviour.Behaviour:
        async def setup(context):
            await context.timers.start_single_shot_timer("hello", timedelta(seconds=10))
            return Test.active(ref)
        return behaviour.setup(setup)
    
    @staticmethod
    def active(ref: Ref) -> behaviour.Behaviour:
        async def recv(msg: str):
            await ref.tell(msg)
            return behaviour.stop()
        return behaviour.receive(recv)


@pytest.fixture
def event_loop():
    loop = ManualTimeEventLoop()
    loop.set_debug(True)
    async def _wakeup():
        while True:
            await asyncio.sleep(1)

    if sys.platform.startswith("win"):
        # https://github.com/python/asyncio/issues/407
        # https://stackoverflow.com/questions/27480967/why-does-the-asyncios-event-loop-suppress-the-keyboardinterrupt-on-windows
        # https://stackoverflow.com/questions/24774980/why-cant-i-catch-sigint-when-asyncio-event-loop-is-running/24775107#24775107
        # https://gist.github.com/lambdalisue/05d5654bd1ec04992ad316d50924137c
        loop.create_task(_wakeup())
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_scheduled(actor_test_kit, event_loop: ManualTimeEventLoop):
    probe = await actor_test_kit.create_probe()
    await actor_test_kit.spawn(partial(Test.start, ref=probe.ref))

    assert event_loop.expect_no_message_for(5, probe.ref)

    event_loop.advance_time(6)

    assert await probe.expect_message("hello")
