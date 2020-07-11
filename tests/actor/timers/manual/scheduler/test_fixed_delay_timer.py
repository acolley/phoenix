import asyncio
from datetime import timedelta
import janus
from pyrsistent import m, s
import pytest
import threading

from phoenix.actor.timers.manual import ManualFixedDelayTimer, ManualTimeScheduler
from phoenix.actor.timers.protocol import FixedDelayEnvelope
from phoenix.ref import Ref


@pytest.fixture
async def ref() -> Ref:
    return Ref(id="test", inbox=janus.Queue(), thread=threading.current_thread())


@pytest.fixture
async def scheduler(ref: Ref) -> ManualTimeScheduler:
    return ManualTimeScheduler(ref=ref, resolution=0.1)


@pytest.mark.asyncio
async def test_cancel(ref: Ref):
    event = asyncio.Event()
    event.set()
    scheduler = ManualTimeScheduler(
        ref=ref,
        resolution=0.1,
        timers=m(
            test=ManualFixedDelayTimer(
                ref=ref,
                msg="hello",
                interval=timedelta(seconds=1),
                initial_delay=None,
                initial_timer=None,
                timer=1.0,
                event=event,
            )
        ),
    )

    await scheduler.cancel("test")

    assert scheduler == ManualTimeScheduler(
        ref=ref,
        resolution=0.1,
        timers=m(
            test=ManualFixedDelayTimer(
                ref=ref,
                msg="hello",
                interval=timedelta(seconds=1),
                initial_delay=None,
                initial_timer=None,
                timer=1.0,
                event=event,
            )
        ),
        cancelled=s("test"),
    )

    await scheduler.advance_time(timedelta(seconds=0.1))

    assert scheduler == ManualTimeScheduler(
        ref=ref, resolution=0.1, timers=m(), cancelled=s(),
    )


@pytest.mark.asyncio
async def test_interval(scheduler: ManualTimeScheduler, ref: Ref):
    await scheduler.start_fixed_delay_timer(
        message="hello", interval=timedelta(seconds=1), name="test"
    )

    await scheduler.advance_time(timedelta(seconds=1.1))

    msg = ref.inbox.async_q.get_nowait()
    assert isinstance(msg, FixedDelayEnvelope)
    assert msg.msg == "hello"


@pytest.mark.asyncio
async def test_fixed_delay(scheduler: ManualTimeScheduler, ref: Ref):
    """
    Test that messages are sent at a fixed delay, meaning that they
    are only sent after the previous message has been consumed.
    """
    await scheduler.start_fixed_delay_timer(
        message="hello", interval=timedelta(seconds=1), name="test"
    )

    await scheduler.advance_time(timedelta(seconds=2.1))

    msg = ref.inbox.async_q.get_nowait()
    assert isinstance(msg, FixedDelayEnvelope)
    assert msg.msg == "hello"

    with pytest.raises(asyncio.QueueEmpty):
        ref.inbox.async_q.get_nowait()
    
    msg.event.set()

    await scheduler.advance_time(timedelta(seconds=2.1))

    msg = ref.inbox.async_q.get_nowait()
    assert isinstance(msg, FixedDelayEnvelope)
    assert msg.msg == "hello"

    with pytest.raises(asyncio.QueueEmpty):
        ref.inbox.async_q.get_nowait()


@pytest.mark.asyncio
async def test_initial_delay(scheduler: ManualTimeScheduler, ref: Ref):
    await scheduler.start_fixed_delay_timer(
        message="hello",
        interval=timedelta(seconds=1),
        initial_delay=timedelta(seconds=0.5),
        name="test",
    )

    await scheduler.advance_time(timedelta(seconds=0.6))

    msg = ref.inbox.async_q.get_nowait()
    assert isinstance(msg, FixedDelayEnvelope)
    assert msg.msg == "hello"

    await scheduler.advance_time(timedelta(seconds=1.1))

    with pytest.raises(asyncio.QueueEmpty):
        ref.inbox.async_q.get_nowait()

    msg.event.set()

    await scheduler.advance_time(timedelta(seconds=1.1))

    msg = ref.inbox.async_q.get_nowait()
    assert isinstance(msg, FixedDelayEnvelope)
    assert msg.msg == "hello"

    with pytest.raises(asyncio.QueueEmpty):
        ref.inbox.async_q.get_nowait()
