import asyncio
import math
import os
from typing import Iterator, Tuple, List

import aiohttp
import aiosqlite
from loguru import logger
from tqdm.asyncio import tqdm

import entities
import exceptions
import repository
from .balancer import Balancer
from .storage_base import UploadStatus


class Uploader:
    def __init__(self, balancer: Balancer, blocks_repo: repository.BlockRepo):
        self._balancer = balancer
        self._blocks_repo = blocks_repo
        self._session = None
        self._progress: List[List[int]] = []
        self._chunk_size = 64 * 2 ** 10

    async def _upload_block(self, block: entities.Block) -> Tuple[UploadStatus, entities.Block]:
        if block.cipher:
            block.data = block.cipher.encrypt(block.data)

        status = await block.storage.upload(block.name, block.data, self._session)

        if status == UploadStatus.OK:
            logger.info(f"Upload block: {block}")
        else:
            logger.warning(f"Failed to upload block: {block}: {status}")

        return status, block

    def _block_by_chunk(self, block: entities.Block):
        offset = 0
        while offset < len(block.data):
            self._progress[block.number][0] += 1
            yield block.data[offset:offset + self._chunk_size]
            offset += self._chunk_size

    async def _upload_block_by_chunks(self, block: entities.Block, ) -> Tuple[UploadStatus, entities.Block]:
        if block.cipher:
            block.data = block.cipher.encrypt(block.data)

        # data = self._block_by_chunk(block)
        data = tqdm(self._block_by_chunk(block), disable=True)
        status = await block.storage.upload_by_chunks(block.name, data, self._session)

        if status == UploadStatus.OK:
            logger.info(f"Upload block: {block}")
        else:
            logger.warning(f"Failed to upload block: {block}: {status}")

        return status, block

    @staticmethod
    def _block_generator(file: entities.File) -> Iterator[entities.Block]:
        with open(file.path, "rb") as f:
            data = f.read(file.block_size)
            number = 0
            while data:
                for _ in range(file.duplicate_count):
                    yield entities.Block(file=file, number=number, data=data)
                data = f.read(file.block_size)
                number += 1

    @staticmethod
    def count_blocks(file: entities.File) -> int:
        size = os.path.getsize(file.path)
        block_count = size // file.block_size
        if block_count * file.block_size != size:
            block_count += 1

        return block_count

    async def upload_file(self, file: entities.File, timeout: float = None):
        tasks = []
        block_generator = self._block_generator(file)
        first = True
        done_tasks = 0

        total = math.ceil(file.block_size / self._chunk_size)
        self._progress = [[0, total] for _ in range(math.ceil(file.size / file.block_size))]
        self._progress[-1][1] = math.ceil((file.size % file.block_size) / self._chunk_size)

        try:
            await self._blocks_repo.add_file(file)
        except aiosqlite.IntegrityError as e:
            logger.exception(e)
            raise exceptions.FileAlreadyExists()

        while len(tasks) != 0 or first:
            first = False
            try:
                for _ in range(file.worker_count - len(tasks)):
                    block = next(block_generator)
                    self._balancer.fill_block(block)
                    tasks.append(asyncio.create_task(self._upload_block_by_chunks(block)))
            except StopIteration:
                pass

            done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)

            tasks = list(pending)
            done_task: List[asyncio.Task] = list(done)

            for task in done_task:
                status, block = task.result()

                if status != UploadStatus.OK:
                    logger.error(f"Cannot load block {block}")
                else:
                    done_tasks += 1
                    await self._blocks_repo.add_block(block)
            # yield done_tasks

        await self._blocks_repo.commit()
        logger.info(f"Upload file: {file}")

    @property
    def progress(self):
        return self._progress

    async def __aenter__(self) -> "Uploader":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        await self._session.close()
