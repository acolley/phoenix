import asyncio
import pickle
import struct
from typing import Any, Generic, TypeVar

from phoenix.dataclasses import dataclass


M = TypeVar("M")


@dataclass
class Connection(Generic[M]):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    @property
    def host(self) -> str:
        return self.writer.get_extra_info("peername")[0]

    @property
    def port(self) -> int:
        return self.writer.get_extra_info("peername")[1]

    async def send(self, msg: M):
        encoded = pickle.dumps(msg)
        size = len(encoded)
        header = struct.pack("!i", size)
        self.writer.write(header)
        self.writer.write(encoded)
        await self.writer.drain()

    async def recv(self) -> M:
        header = await self.reader.readexactly(4)
        (size,) = struct.unpack("!i", header)
        encoded = await self.reader.readexactly(size)
        msg = pickle.loads(encoded)
        return msg

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    def __repr__(self) -> str:
        return f"Connection(host={self.host!r}, port={self.port!r})"
