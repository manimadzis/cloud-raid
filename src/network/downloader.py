import asyncio
import math
import os
from typing import List, Tuple, Iterable

import aiohttp
from loguru import logger

import entities
import repository
from .storage_base import DownloadStatus


class Downloader:
    def __init__(self, block_repo: repository.BlockRepo, chunk_size: int = 64 * 2 ** 10):
        self._block_repo = block_repo
        self._session = None
        self._progress: List[List[int]] = []
        self._chunk_size = chunk_size

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def _download_block(self, block: entities.Block) -> Tuple[DownloadStatus, entities.Block]:
        status, data = await block.storage.download(block.name, self._session)

        if block.cipher:
            data = block.cipher.decrypt(data)

        block.data = data
        if status == DownloadStatus.OK:
            logger.info(f"Download block: {block}")
        else:
            logger.info(f"Failed to download block: {block}: {status}")

        return status, block

    async def _download_block_by_chunks(self, block: entities.Block) -> Tuple[DownloadStatus, entities.Block]:
        def inc_progress():
            self._progress[block.number][0] += 1

        status, data = await block.storage.download_by_chunks(block.name, self._chunk_size, inc_progress, self._session)

        if block.cipher:
            data = block.cipher.decrypt(data)

        block.data = data
        if status == DownloadStatus.OK:
            logger.info(f"Download block: {block}")
        else:
            logger.info(f"Failed to download block: {block}: {status}")

        return status, block

    @staticmethod
    def _merge_blocks(path: str, blocks: Iterable[entities.Block], temp_dir: str):
        with open(path, "wb") as f:
            for block in blocks:
                with open(os.path.join(temp_dir, block.name), "rb") as ff:
                    data = ff.read()
                f.write(data)

    async def count_blocks(self, file: entities.File) -> int:
        blocks = await self._block_repo.get_blocks_by_file(file)
        return len(blocks)

    async def download_file(self, file: entities.File, temp_dir: str = "") -> None:
        tasks: List[asyncio.Task] = []
        blocks = await self._block_repo.get_blocks_by_file(file)
        logger.info(blocks)
        total = math.ceil(blocks[0].size / self._chunk_size)
        self._progress = [[0, total] for _ in blocks]
        self._progress[-1][0] = file.size - blocks[0].size * (len(blocks) - 1)

        index = 0
        blocks_count = 0
        while index < len(blocks) or tasks:
            for _ in range(file.worker_count - len(tasks)):
                if index >= len(blocks):
                    break
                tasks.append(asyncio.create_task(self._download_block(blocks[index])))
                index += 1
            logger.info(tasks)

            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            done_tasks: List[asyncio.Task] = list(done)
            tasks = list(pending)
            for task in done_tasks:
                status, block = task.result()

                if status == DownloadStatus.OK:
                    block.save(temp_dir)
                    blocks_count += 1
                else:
                    logger.info(f"Cannot load block: {block}")

        self._merge_blocks(file.path, blocks, temp_dir)

    @property
    def progress(self):
        return self._progress
