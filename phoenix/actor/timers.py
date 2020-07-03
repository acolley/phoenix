import asyncio
import attr
from attr.validators import instance_of
import contextlib
from datetime import timedelta
from pyrsistent import m
from typing import Any, Optional
import uuid

from phoenix.ref import Ref


@attr.s(frozen=True)
class FixedDelayEnvelope:
    msg: Any = attr.ib()
    event: asyncio.Event = attr.ib(validator=instance_of(asyncio.Event))


@attr.s
class Timers:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    lock: asyncio.Lock = attr.ib(validator=instance_of(asyncio.Lock))
    _timers = attr.ib(init=False, default=m())

    async def start_fixed_rate_timer(
        self,
        message: Any,
        interval: timedelta,
        name: Optional[str] = None,
        initial_delay: Optional[timedelta] = None,
    ):
        """
        Args:
            message (Any): The message to be sent on timer timeout.
            interval (timedelta): The time interval between timer timeouts.
            name (Optional[str]): Identifier for this timer for later modification.
            initial_delay (Optional[timedelta]): An optional initial delay that differs
                from the interval.
        """
        name = name or str(uuid.uuid1())

        async def _fixed_rate_timer():
            if initial_delay is not None:
                await asyncio.sleep(initial_delay.total_seconds())
                await self.ref.tell(message)
            while True:
                await asyncio.sleep(interval.total_seconds())
                await self.ref.tell(message)

        async with self.lock:
            if name in self._timers:
                raise ValueError(f"Timer `{name}` already exists.")

            self._timers = self._timers.set(
                name, asyncio.get_event_loop().create_task(_fixed_rate_timer())
            )

    async def start_fixed_delay_timer(
        self,
        message: Any,
        interval: timedelta,
        name: Optional[str] = None,
        initial_delay: Optional[timedelta] = None,
    ):
        name = name or str(uuid.uuid1())

        async def _fixed_delay_timer():
            if initial_delay is not None:
                await asyncio.sleep(initial_delay.total_seconds())
                event = asyncio.Event()
                msg = FixedDelayEnvelope(msg=message, event=event)
                await self.ref.tell(msg)
                await event.wait()
            while True:
                await asyncio.sleep(interval.total_seconds())
                event = asyncio.Event()
                msg = FixedDelayEnvelope(msg=message, event=event)
                await self.ref.tell(msg)
                await event.wait()

        async with self.lock:
            if name in self._timers:
                raise ValueError(f"Timer `{name}` already exists.")

            self._timers = self._timers.set(
                name, asyncio.get_event_loop().create_task(_fixed_delay_timer())
            )

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

            self._timers = self._timers.set(
                name, asyncio.get_event_loop().create_task(_single_shot_timer())
            )

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
