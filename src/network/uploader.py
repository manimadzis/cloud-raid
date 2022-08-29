import asyncio
from typing import Iterator, Tuple, List, Optional

import aiohttp
import aiosqlite
from loguru import logger

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

    async def _upload_block(self, block: entities.Block) -> Tuple[UploadStatus, entities.Block]:
        if block.cipher:
            block.data = block.cipher.encrypt(block.data)

        status = await block.storage.upload(block.name, block.data, self._session)

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

    def count_blocks(self, file: entities.File) -> int:
        file.block_size = self._balancer.block_size(file)
        block_generator = Uploader._block_generator(file)
        count = 0
        for _ in block_generator:
            count += 1
        return count

    async def upload_file(self, file: entities.File, timeout: float = None):
        tasks = []
        block_generator = self._block_generator(file)
        first = True
        done_tasks = 0

        try:
            await self._blocks_repo.add_file(file)
        except aiosqlite.IntegrityError as e:
            logger.exception(e)
            raise exceptions.FileAlreadyExists()

        file.block_size = self._balancer.block_size(file)

        while len(tasks) != 0 or first:
            first = False
            try:
                for _ in range(file.worker_count - len(tasks)):
                    block = next(block_generator)
                    self._balancer.fill_block(block)
                    tasks.append(asyncio.create_task(self._upload_block(block)))
            except StopIteration:
                pass

            done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)

            tasks = list(pending)
            done_task: List[asyncio.Task] = list(done)

            for task in done_task:
                status, block = task.result()

                if status != UploadStatus.OK:
                    tasks.append(asyncio.create_task(self._upload_block(block)))
                else:
                    done_tasks += 1
                    await self._blocks_repo.add_block(block)
            yield done_tasks

        await self._blocks_repo.commit()
        logger.info(f"Upload file: {file}")

    async def __aenter__(self) -> "Uploader":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        await self._session.close()
