import pytest

from phoenix.manual_time_events import ManualTimeEventLoop


@pytest.fixture
def loop():
    return ManualTimeEventLoop()


def test_call_at(loop: ManualTimeEventLoop):
    called = False
    def callback():
        nonlocal called
        called = True

    loop.call_at(when=1.0, callback=callback)

    loop.advance_time(0.5)

    assert not called

    loop.advance_time(0.1)

    assert not called

    loop.advance_time(0.5)

    assert called


def test_call_later(loop: ManualTimeEventLoop):
    called = False

    def callback():
        nonlocal called
        called = True

    loop.call_later(delay=1.0, callback=callback)

    loop.advance_time(0.5)

    assert not called

    loop.advance_time(0.1)

    assert not called

    loop.advance_time(0.5)

    assert called
