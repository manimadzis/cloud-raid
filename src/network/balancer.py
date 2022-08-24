import heapq
import uuid
from typing import Tuple, Sequence

import aiohttp
from entities import Block
from entities import Disk
from network.yandex_disk.get_disk_size import get_disk_size


class Balancer:
    def __init__(self, disks: Sequence[Disk]):
        self._disks = tuple(disks)
        self._session = aiohttp.ClientSession()
        self._queue = list(disks)
        heapq.heapify(self._queue)

    def disks(self, count) -> Tuple[Disk]:
        """
        Return disks for uploading

        :param count: count of disks
        :return:
        """

        if count > len(self._queue):
            count = len(self._queue)
        disks = [heapq.heappop(self._queue) for _ in range(count)]

        for disk in disks:
            heapq.heappush(self._queue, disk)

        return tuple(disks)

    async def disk_size(self, disk: Disk) -> Tuple[int, int]:
        return await get_disk_size(disk.token, self._session)

    def fill_block(self, block: Block) -> Block:
        block.disk = self.disks(1)[0]
        block.name = str(uuid.uuid4())
        return block

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def close(self):
        await self._session.close()
