import asyncio
import logging
from phoenix.cluster import ClusterNode
import sys


async def async_main():
    node = ClusterNode(host=sys.argv[1], port=int(sys.argv[2]))
    await node.start()
    await node.serve_forever()


def main():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    asyncio.run(async_main())
