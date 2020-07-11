import asyncio
import attr
from attr.validators import instance_of, optional
import contextlib
from datetime import timedelta
import math
from pyrsistent import PMap, PSet, m, s
import time
from typing import Any, Optional
import uuid

from phoenix.actor.timers.protocol import FixedDelayEnvelope
from phoenix.ref import Ref


@attr.s
class TimerFinished(Exception):
    pass


@attr.s
class ManualSingleShotTimer:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    msg: Any = attr.ib()
    delay: timedelta = attr.ib(validator=instance_of(timedelta))
    timer: float = attr.ib(validator=instance_of(float))

    async def tick(self, dt: float):
        self.timer -= dt
        if self.timer < 0 or math.isclose(self.timer, 0):
            await self.ref.tell(self.msg)
            raise TimerFinished


@attr.s
class ManualFixedRateTimer:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    msg: Any = attr.ib()
    interval: timedelta = attr.ib(validator=instance_of(timedelta))
    initial_delay: Optional[timedelta] = attr.ib(
        validator=optional(instance_of(timedelta))
    )
    initial_timer: Optional[float] = attr.ib(validator=optional(instance_of(float)))
    timer: float = attr.ib(validator=instance_of(float))

    async def tick(self, dt: float):
        if self.initial_timer is not None and self.initial_timer > 0 and not math.isclose(self.initial_timer, 0):
            self.initial_timer -= dt
            if self.initial_timer < 0 or math.isclose(self.initial_timer, 0):
                await self.ref.tell(self.msg)
        else:
            self.timer -= dt
            if self.timer < 0 or math.isclose(self.timer, 0):
                await self.ref.tell(self.msg)
                self.timer = float(self.interval.total_seconds())


@attr.s
class ManualFixedDelayTimer:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    msg: Any = attr.ib()
    interval: timedelta = attr.ib(validator=instance_of(timedelta))
    initial_delay: Optional[timedelta] = attr.ib(
        validator=optional(instance_of(timedelta))
    )
    initial_timer: Optional[float] = attr.ib(validator=optional(instance_of(float)))
    timer: float = attr.ib(validator=instance_of(float))
    event: asyncio.Event = attr.ib(validator=instance_of(asyncio.Event))

    @event.validator
    def check(self, attribute: str, value: asyncio.Event):
        if not value.is_set():
            raise ValueError("`event` must be set.")

    async def tick(self, dt: float):
        # If the event is set that means that a message
        # has been processed and therefore we should
        # execute the next interval of the timer.
        if self.event.is_set():
            if self.initial_timer is not None and self.initial_timer > 0 and not math.isclose(self.initial_timer, 0):
                self.initial_timer -= dt
                if self.initial_timer < 0 or math.isclose(self.initial_timer, 0):
                    self.event.clear()
                    await self.ref.tell(FixedDelayEnvelope(msg=self.msg, event=self.event))
            else:
                self.timer -= dt
                if self.timer < 0 or math.isclose(self.timer, 0):
                    self.event.clear()
                    await self.ref.tell(FixedDelayEnvelope(msg=self.msg, event=self.event))
                    self.timer = float(self.interval.total_seconds())
        else:
            self.timer = float(self.interval.total_seconds())


@attr.s
class ManualTimeScheduler:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    # https://bugs.python.org/issue31539
    resolution: float = attr.ib(
        validator=instance_of(float),
        default=time.get_clock_info("monotonic").resolution,
    )
    """
    The resolution of the clock in seconds.
    """
    timers = attr.ib(validator=instance_of(PMap), default=m())
    cancelled = attr.ib(validator=instance_of(PSet), default=s())

    async def advance_time(self, interval: timedelta):
        ticks = int(math.ceil(interval.total_seconds() / self.resolution))
        # FIXME: does this result in deadlock if a timer is scheduled/cancelled during this?
        for _ in range(ticks):
            for name in self.cancelled:
                self.timers = self.timers.remove(name)
                self.cancelled = self.cancelled.remove(name)
            for name, timer in self.timers.items():
                try:
                    await timer.tick(self.resolution)
                except TimerFinished:
                    await self.cancel(name)

    async def cancel(self, name: str):
        self.cancelled = self.cancelled.add(name)

    async def cancel_all(self):
        for name in self.timers.keys():
            self.cancelled = self.cancelled.add(name)

    async def start_single_shot_timer(
        self, message: Any, delay: timedelta, name: Optional[str] = None
    ):
        name = name or str(uuid.uuid1())

        timer = ManualSingleShotTimer(
            ref=self.ref, msg=message, delay=delay, timer=float(delay.total_seconds()),
        )

        if name in self.timers:
            raise ValueError(f"Timer `{name}` already exists.")

        self.timers = self.timers.set(name, timer)

    async def start_fixed_rate_timer(
        self,
        message: Any,
        interval: timedelta,
        name: Optional[str] = None,
        initial_delay: Optional[timedelta] = None,
    ):
        name = name or str(uuid.uuid1())

        timer = ManualFixedRateTimer(
            ref=self.ref,
            msg=message,
            interval=interval,
            initial_delay=initial_delay,
            initial_timer=None
            if initial_delay is None
            else float(initial_delay.total_seconds()),
            timer=float(interval.total_seconds()),
        )

        if name in self.timers:
            raise ValueError(f"Timer `{name}` already exists.")

        self.timers = self.timers.set(name, timer)

    async def start_fixed_delay_timer(
        self,
        message: Any,
        interval: timedelta,
        name: Optional[str] = None,
        initial_delay: Optional[timedelta] = None,
    ):
        name = name or str(uuid.uuid1())

        event = asyncio.Event()
        event.set()
        timer = ManualFixedDelayTimer(
            ref=self.ref,
            msg=message,
            interval=interval,
            initial_delay=initial_delay,
            initial_timer=None
            if initial_delay is None
            else float(initial_delay.total_seconds()),
            timer=float(interval.total_seconds()),
            event=event,
        )

        if name in self.timers:
            raise ValueError(f"Timer `{name}` already exists.")

        self.timers = self.timers.set(name, timer)
