import asyncio
import attr
from attr.validators import instance_of
import janus
import logging
import threading
import uuid

from phoenix.actor.actor import ActorContext
from phoenix.actor.cell import ActorCell
from phoenix.dispatchers.executor.actor import Executor
from phoenix.ref import Ref

logger = logging.getLogger(__name__)


@attr.s
class ExecutorCreated:
    ref: Ref = attr.ib(validator=instance_of(Ref))


class ThreadExecutor(threading.Thread):
    def __init__(self, dispatcher: Ref, system: Ref, registry: Ref):
        super(ThreadExecutor, self).__init__()

        self.dispatcher = dispatcher
        self.system = system
        self.registry = registry

    def run(self):
        logger.debug("Starting ThreadExecutor on thread %s", threading.current_thread())

        async def _run():

            executor = ActorCell(
                behaviour=Executor.start(),
                context=ActorContext(
                    ref=Ref(
                        id=f"{self.dispatcher.id}-executor",
                        inbox=janus.Queue(),
                        thread=threading.current_thread(),
                    ),
                    parent=self.dispatcher,
                    thread=threading.current_thread(),
                    loop=asyncio.get_event_loop(),
                    system=self.system,
                    registry=self.registry,
                ),
            )

            task = asyncio.create_task(executor.run())

            await self.dispatcher.tell(ExecutorCreated(ref=executor.context.ref))

            await task

        asyncio.run(_run())
