import heapq
import os
import uuid
from typing import Tuple, Iterable

import entity
import exceptions
from crypto import CipherBase
from .storage_base import StorageBase


class Balancer:
    def __init__(self, storages: Iterable[StorageBase],
                 ciphers: Iterable[CipherBase] = tuple(),
                 min_block_size=1 * 2 ** 20,
                 max_block_size=5 * 2 ** 20,
                 block_size=None,
                 ):
        self._min_block_size = min_block_size
        self._max_block_size = max_block_size
        self._block_size = block_size
        self._ciphers = tuple(ciphers) or None
        self._storages = tuple(storages)
        self._queue = list(storages)

        heapq.heapify(self._queue)

    def _cipher(self) -> CipherBase:
        if self._ciphers:
            return self._ciphers[0]

    def storages(self, count: int) -> Tuple[StorageBase]:
        if not self._storages:
            raise exceptions.NoStorage()

        if count > len(self._queue):
            count = len(self._queue)
        disks = [heapq.heappop(self._queue) for _ in range(count)]

        for disk in disks:
            heapq.heappush(self._queue, disk)

        return tuple(disks)

    def block_size(self, file: entity.File) -> int:
        if self._block_size:
            return self._block_size

        file_size = os.path.getsize(file.path)

        if file_size < self._min_block_size:
            block_size = self._min_block_size
        elif file_size < (self._max_block_size + self._min_block_size) / 2:
            block_size = file_size
        elif file_size < self._max_block_size:
            block_size = file_size // 2 + 1
        else:
            block_size = self._max_block_size

        return block_size

    def fill_block(self, block: entity.Block) -> entity.Block:
        block.storage = self.storages(1)[0]
        block.name = str(uuid.uuid4())
        block.cipher = self._cipher()
        return block
