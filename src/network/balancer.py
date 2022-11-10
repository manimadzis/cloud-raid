import heapq
import math
import os
import random
import uuid
from typing import Tuple, Iterable, Collection, Optional

import entity
import exceptions
from crypto import CipherBase
from .storage_base import StorageBase


class Balancer:
    """
    Balancer assign storage to block
    Using heap sorted by used space in storage
    """

    def __init__(self,
                 storages: Iterable[StorageBase],
                 ciphers: Optional[Iterable[CipherBase]] = None,
                 block_size: int = 5 * 2 ** 20):
        if not storages:
            raise exceptions.NoStorage()

        self._block_size = block_size
        self._storage_queue = list(storages)
        self._ciphers = list(ciphers) if ciphers else None

        heapq.heapify(self._storage_queue)

    def _cipher(self) -> CipherBase:
        if not self._cipher:
            raise exceptions.NoCipher()

        return random.choice(self._ciphers)

    def _storages(self, count: int) -> Tuple[StorageBase]:
        disks = [heapq.heappop(self._storage_queue) for _ in range(count)]

        for disk in disks:
            heapq.heappush(self._storage_queue, disk)

        return tuple(disks)

    @staticmethod
    def _total_blocks(file: entity.File) -> int:
        """
        Count blocks of file (get size and divide by block size)
        """
        size = os.path.getsize(file.path)
        return math.ceil(size / file.block_size)

    def fill_file(self, file: entity.File) -> None:
        """
        Calculate block_size, total_blocks and size by getting file size
        """
        file.block_size = self._block_size
        file.size = os.path.getsize(file.path)
        file.total_blocks = self._total_blocks(file)

    def fill_blocks(self, blocks: Collection[entity.Block]) -> None:
        """
        Assign unique storage and name to every block. Also add cipher if block.file.need_encrypt

        It suppose every block in list belongs to the same file
        Because duplicates of block have to store in different storages
        """
        for block, storage in zip(blocks, self._storages(len(blocks))):
            block.storage = storage
            block.name = str(uuid.uuid4())
            if block.file.need_encrypt:
                block.cipher = self._cipher()
