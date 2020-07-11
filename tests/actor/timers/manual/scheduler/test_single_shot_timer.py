import asyncio
from datetime import timedelta
import janus
from pyrsistent import m, s
import pytest
import threading

from phoenix.ref import Ref
from phoenix.actor.timers.manual import ManualSingleShotTimer, ManualTimeScheduler


@pytest.fixture
async def ref() -> Ref:
    return Ref(id="test", inbox=janus.Queue(), thread=threading.current_thread())


@pytest.fixture
async def scheduler(ref: Ref) -> ManualTimeScheduler:
    return ManualTimeScheduler(ref=ref, resolution=0.1)


@pytest.mark.asyncio
async def test_cancel(scheduler: ManualTimeScheduler, ref: Ref):
    await scheduler.start_single_shot_timer(
        message="hello", delay=timedelta(seconds=1), name="test"
    )
    await scheduler.cancel("test")

    assert scheduler == ManualTimeScheduler(
        ref=ref,
        resolution=0.1,
        timers=m(
            test=ManualSingleShotTimer(
                ref=ref, msg="hello", delay=timedelta(seconds=1), timer=1.0
            )
        ),
        cancelled=s("test"),
    )
    
    await scheduler.advance_time(timedelta(seconds=0.1))

    assert scheduler == ManualTimeScheduler(
        ref=ref,
        resolution=0.1,
        timers=m(),
        cancelled=s(),
    )


@pytest.mark.asyncio
async def test_advance_time(scheduler: ManualTimeScheduler, ref: Ref):
    await scheduler.start_single_shot_timer(
        message="hello", delay=timedelta(seconds=1), name="test"
    )

    await scheduler.advance_time(timedelta(seconds=1.1))

    msg = ref.inbox.async_q.get_nowait()
    assert msg == "hello"

    with pytest.raises(asyncio.QueueEmpty):
        ref.inbox.async_q.get_nowait()
