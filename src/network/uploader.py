import asyncio
import math
import os
from typing import Iterator, Tuple, List, Sequence

import aiohttp
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
        while offset < len(block.data) - self._chunk_size:
            self._progress[block.number][0] += 1
            yield block.data[offset:offset + self._chunk_size]
            offset += self._chunk_size

    async def _upload_block_by_chunks(self, block: entities.Block) -> Tuple[UploadStatus, entities.Block]:
        if block.cipher:
            block.data = block.cipher.encrypt(block.data)

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
                    yield entities.Block(file=file, number=number, data=data, size=len(data))
                data = f.read(file.block_size)
                number += 1

    @staticmethod
    def _block_generator_and_filter(file: entities.File, uploaded_blocks: Sequence[entities.Block]):
        block_generator = Uploader._block_generator(file)
        for block in block_generator:
            if block.number not in [block_.number for block_ in uploaded_blocks]:
                yield block

    @staticmethod
    def count_blocks(file: entities.File) -> int:
        size = os.path.getsize(file.path)
        block_count = size // file.block_size
        if block_count * file.block_size != size:
            block_count += 1

        return block_count

    async def _upload_blocks(self, blocks: Iterator[entities.Block], worker_count: int):
        upload_tasks = []
        db_tasks = []
        first = True

        while first or upload_tasks or db_tasks:
            first = False
            try:
                for _ in range(worker_count - len(upload_tasks)):
                    block = next(blocks)
                    self._balancer.fill_block(block)
                    upload_tasks.append(asyncio.create_task(self._upload_block_by_chunks(block)))
            except StopIteration:
                pass
            logger.info(upload_tasks)
            if upload_tasks:
                done, pending = await asyncio.wait(upload_tasks, return_when=asyncio.FIRST_COMPLETED)

                upload_tasks = list(pending)
                done_tasks: List[asyncio.Task] = list(done)

                for task in done_tasks:
                    status, block = task.result()

                    if status == UploadStatus.OK:
                        db_tasks.append(asyncio.create_task(self._blocks_repo.add_block(block)))
                    else:
                        logger.error(f"Cannot load block {block}")

                db_tasks = [task for task in db_tasks if not task.done()]
                db_tasks.append(asyncio.create_task(self._blocks_repo.commit()))

        await self._blocks_repo.commit()

    def _init_progress(self, file):
        total = math.ceil(file.block_size / self._chunk_size)
        self._progress = [[0, total] for _ in range(math.ceil(file.size / file.block_size))]
        self._progress[-1][1] = math.ceil((file.size % file.block_size) / self._chunk_size)

    async def upload_file(self, file: entities.File):
        uploaded_blocks = []
        try:
            db_file = await self._blocks_repo.get_file_by_filename(file.filename)
        except exceptions.UnknownFile as e:
            await self._blocks_repo.add_file(file)
            await self._blocks_repo.commit()
        else:
            file.id = db_file.id
            logger.info(file.block_size)
            logger.info(file.uploaded_block_count)
            if file.block_count == db_file.uploaded_block_count:
                raise exceptions.FileAlreadyExists()
            uploaded_blocks = await self._blocks_repo.get_blocks_by_file(file)

        if uploaded_blocks:
            blocks = self._block_generator_and_filter(file, uploaded_blocks)
        else:
            blocks = self._block_generator(file)

        self._init_progress(file)

        await self._upload_blocks(blocks, file.worker_count)

        logger.info(f"Upload file: {file}")

    @property
    def progress(self):
        return self._progress

    async def __aenter__(self) -> "Uploader":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        await self._session.close()
