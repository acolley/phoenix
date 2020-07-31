import asyncio
from asyncio.selector_events import BaseSelectorEventLoop

from phoenix.ref import Ref


class ManualTimeEventLoop(BaseSelectorEventLoop):
    def __init__(self, selector=None):
        super(ManualTimeEventLoop, self).__init__(selector=selector)

        self._time = 0

    def time(self) -> float:
        return self._time

    def advance_time(self, dt: float):
        self._time += dt
        # FIXME: race condition as this is not meant to be called by others
        if not self.is_running():
            self._run_once()

    def expect_no_message_for(self, dt: float, ref: Ref):
        self.advance_time(dt)
        try:
            ref.inbox.async_q.get_nowait()
        except asyncio.QueueEmpty:
            return True
        return False
