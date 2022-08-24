import asyncio
from typing import Iterator, Tuple

import aiohttp
from loguru import logger

from entities import File, Block
from network.balancer import Balancer
from network.yandex_disk.upload import upload, UploadStatus
from storage.block_repo import BlockRepo


class Uploader:
    def __init__(self, balancer: Balancer, blocks_repo: BlockRepo):
        self._balancer = balancer
        self._blocks_repo = blocks_repo
        self._session = None

    async def _upload_block(self, block: Block) -> Tuple[UploadStatus, Block]:
        status = await upload(block.disk.token, block.name, block.data, self._session)
        if status == UploadStatus.OK:
            logger.info(f"Upload block: {block}")
        else:
            logger.warning(f"Failed to upload block: {block}: {status}")

        return status, block

    @staticmethod
    def _block_generator(file: File) -> Iterator[Block]:
        with open(file.path, "rb") as f:
            data = f.read(file.block_size)
            number = 0
            while data:
                for _ in range(file.duplicate_count):
                    yield Block(filename=file.path, number=number, data=data)
                data = f.read(file.block_size)
                number += 1

    @staticmethod
    def count_blocks(file: File):
        block_generator = Uploader._block_generator(file)
        count = 0
        for _ in block_generator:
            count += 1
        return count

    async def upload_file(self, file: File, timeout: float = None):
        tasks = []
        block_generator = self._block_generator(file)
        first = True
        done_tasks = 0

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
            done_task = list(done)[0]
            status, block = done_task.result()

            if status != UploadStatus.OK:
                tasks.append(asyncio.create_task(self._upload_block(block)))
            else:
                done_tasks += len(done)
                yield done_tasks

            await self._blocks_repo.add_block(block)
        await self._blocks_repo.commit()
        logger.info(f"Upload file: {file}")

    async def __aenter__(self) -> "Uploader":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        await self._session.close()
