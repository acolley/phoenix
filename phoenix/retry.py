import asyncio
import logging
from typing import Any, Callable, Coroutine, Tuple, Union

logger = logging.getLogger(__name__)


def retry(
    max_retries: int = 5,
    backoff: Callable[[int], float] = lambda n: 2 ** n,
    exc: Union[Exception, Tuple[Exception]] = Exception,
):
    async def _retry(func: Callable[[], Coroutine]):
        n = 0
        while n < max_retries:
            try:
                return await func()
            except exc as e:
                cause = e
                wait_for = backoff(n)
                n += 1
                logger.debug(
                    "Retry caught error. Retrying in %f seconds. Retry: %d.",
                    wait_for,
                    n,
                    exc_info=True,
                )
                await asyncio.sleep(wait_for)

        raise cause

    return _retry
