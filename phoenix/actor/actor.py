import asyncio
import attr
from attr.validators import instance_of, optional
import logging
from typing import Optional

from phoenix.actor.context import ActorContext
from phoenix.actor.lifecycle import PreRestart, PostStop, RestartActor, StopActor
from phoenix.actor.timers import Timers
from phoenix.behaviour import (
    Behaviour,
    Ignore,
    Receive,
    Restart,
    Same,
    Setup,
)
from phoenix.persistence.behaviour import Persist
from phoenix.ref import Ref

logger = logging.getLogger(__name__)


@attr.s
class ActorFailed:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    exc: Exception = attr.ib(validator=instance_of(Exception))


@attr.s
class ActorStopped:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorKilled:
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorSpawned:
    parent: Optional[Ref] = attr.ib(validator=optional(instance_of(Ref)))
    behaviour: Behaviour = attr.ib()
    ref: Ref = attr.ib(validator=instance_of(Ref))


@attr.s
class ActorRestarted:
    ref: Ref = attr.ib(validator=instance_of(Ref))
    behaviour: Restart = attr.ib(validator=instance_of(Restart))


@attr.s
class Actor:
    """
    Responsible for executing the behaviours returned
    by the actor's starting behaviour.
    """

    start: Behaviour = attr.ib()
    context: ActorContext = attr.ib(validator=instance_of(ActorContext))

    @start.validator
    def check(self, attribute: str, value: Behaviour):
        if not isinstance(value, (Ignore, Persist, Receive, Restart, Setup)):
            raise ValueError(f"Invalid start behaviour: {value}")

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
        try:
            behaviours = [self.start]
            while behaviours:
                logger.debug("[%s] Main: %s", self.context.ref, behaviours)
                current = behaviours.pop()
                next_ = await current.execute(self.context)
                if isinstance(next_, Same):
                    behaviours.append(current)
                elif next_:
                    behaviours.append(next_)
        except RestartActor as e:
            if current.on_lifecycle:
                behaviour = await current.on_lifecycle(PreRestart())
                if isinstance(behaviour, Same):
                    behaviour = e.behaviour
            else:
                behaviour = e.behaviour
            return ActorRestarted(ref=self.context.ref, behaviour=behaviour)
        except StopActor:
            if current.on_lifecycle:
                await current.on_lifecycle(PostStop())
            return ActorStopped(ref=self.context.ref)
        except asyncio.CancelledError:
            if current.on_lifecycle:
                await current.on_lifecycle(PostStop())
            # We were deliberately cancelled.
            return ActorKilled(ref=self.context.ref)
        except Exception as e:
            if current.on_lifecycle:
                await current.on_lifecycle(PostStop())
            logger.warning(
                "[%s] Behaviour raised unhandled exception",
                self.context.ref,
                exc_info=True,
            )
            return ActorFailed(ref=self.context.ref, exc=e)
