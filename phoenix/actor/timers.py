import asyncio
import attr
from attr.validators import instance_of
import contextlib
from datetime import timedelta
from pyrsistent import m
from typing import Any, Optional
import uuid

from phoenix.ref import Ref


@attr.s
class Timers:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    lock: asyncio.Lock = attr.ib(validator=instance_of(asyncio.Lock))
    _timers = attr.ib(init=False, default=m())

    async def start_fixed_delay_timer(
        self, message: Any, delay: timedelta, name: Optional[str] = None
    ):
        name = name or str(uuid.uuid1())

        async def _fixed_delay_timer():
            while True:
                await asyncio.sleep(delay.total_seconds())
                await self.ref.tell(message)

        async with self.lock:
            if name in self._timers:
                raise ValueError(f"Timer `{name}` already exists.")

            self._timers = self._timers.set(name, asyncio.create_task(_fixed_delay_timer()))

    async def start_single_shot_timer(
        self, message: Any, delay: timedelta, name: Optional[str] = None
    ):
        name = name or str(uuid.uuid1())

        async def _single_shot_timer():
            asyncio.sleep(delay.total_seconds())
            await self.ref.tell(message)
            await self.cancel(name)

        async with self.lock:
            if name in self._timers:
                raise ValueError(f"Timer `{name}` already exists.")

            self._timers = self._timers.set(name, asyncio.create_task(_single_shot_timer()))

    async def cancel(self, name: str):
        async with self.lock:
            timer = self._timers[name]
            timer = self._timers.pop(name)
            timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await timer
            self._timers = self._timers.remove(name)

    async def cancel_all(self):
        async with self.lock:
            timers = self._timers
            names = list(timers.keys())
            while names:
                name = names.pop()
                timer = timers[name]
                timer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await timer
                timers = timers.remove(name)
            self._timers = timers