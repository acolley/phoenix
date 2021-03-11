import asyncio
import logging
from phoenix.actor import ActorId
from phoenix.retry import retry
from phoenix.system.system import ActorSystem
import sys

from factorial.factorial import Factorial


async def async_main():
    host, port = sys.argv[1], int(sys.argv[2])
    system = ActorSystem("api", (host, port))
    await system.start()
    result = await retry()(
        asyncio.wait_for(
            system.call(
                ActorId("factorial", "Factorial"),
                lambda reply_to: Factorial.Calculate(reply_to=reply_to, n=10),
            ),
            timeout=3,
        )
    )
    print(result)


def main():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    asyncio.run(async_main())
