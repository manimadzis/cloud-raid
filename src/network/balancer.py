import heapq
import uuid
from typing import Tuple, Sequence

from entities import Block
from entities import Disk


class Balancer:
    def __init__(self, disks: Sequence[Disk]):
        self._disks = tuple(disks)
        self._queue = list(disks)
        heapq.heapify(self._queue)

    def disks(self, count: int) -> Tuple[Disk]:
        if count > len(self._queue):
            count = len(self._queue)
        disks = [heapq.heappop(self._queue) for _ in range(count)]

        for disk in disks:
            heapq.heappush(self._queue, disk)

        return tuple(disks)

    def fill_block(self, block: Block) -> Block:
        block.disk = self.disks(1)[0]
        block.name = str(uuid.uuid4())
        return block
