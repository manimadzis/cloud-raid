import asyncio
import math
import os
from typing import List, Tuple, Iterable, Sequence, Optional

import aiohttp
from loguru import logger

import entity
import repository
import utils
from network.block_progress import BlockProgress
from .storage_base import DownloadStatus


class DownloaderError(Exception):
    pass


class ChecksumNoEqual(DownloaderError):
    pass


class BlockDownloadFailed(DownloaderError):
    pass


class Downloader:
    def __init__(self,
                 block_repo: repository.BlockRepo,
                 chunk_size: int = 64 * 2 ** 10,
                 parallel_num: int = 1):
        self._block_repo = block_repo
        self._session = None
        self._progress: List[BlockProgress] = []
        self._chunk_size = chunk_size
        self._parallel_num = parallel_num

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def _download_block(self, block: entity.Block) -> Tuple[DownloadStatus, entity.Block]:
        status, data = await block.storage.download(block.name, self._session)

        if block.cipher:
            data = block.cipher.decrypt(data)

        block.data = data
        if status == DownloadStatus.OK:
            logger.info(f"Download block: {block}")
        else:
            logger.info(f"Failed to download block: {block}: {status}")

        return status, block

    async def _download_block_by_chunks(self, block: entity.Block) -> DownloadStatus:
        def inc_progress():
            self._progress[block.number].done += 1

        status, data = await block.storage.download_by_chunks(block.name, self._chunk_size, inc_progress, self._session)

        if block.cipher:
            data = block.cipher.decrypt(data)

        block.data = data
        if status == DownloadStatus.OK:
            logger.info(f"Download block: {block}")
        else:
            logger.info(f"Failed to download block: {block}: {status}")

        return status

    @staticmethod
    def _merge_blocks(path: str, blocks: Iterable[entity.Block], temp_dir: str):
        if os.path.exists(path):
            path += "(NEW)"
        with open(path, "wb") as f:
            for block in blocks:
                with open(os.path.join(temp_dir, block.name), "rb") as ff:
                    data = ff.read()
                f.write(data)

    def _init_progress(self, grouped_blocks: Sequence[Sequence[entity.Block]]):
        """
        Reset progress
        """

        self._progress = []
        for blocks in grouped_blocks:
            block = blocks[0]
            self._progress.append(
                    BlockProgress(done=0,
                                  total=math.ceil(block.size / self._chunk_size),
                                  block_number=block.number))

    async def _download_block_by_group(self,
                                       blocks: Sequence[entity.Block]) \
            -> Tuple[Sequence[Tuple[DownloadStatus, entity.Block]], Optional[entity.Block]]:
        history: List[Tuple[DownloadStatus, entity.Block]] = []
        for block in blocks:
            status = await self._download_block_by_chunks(block)
            if status != DownloadStatus.OK:
                history.append((status, block))
            else:
                break
        else:
            return tuple(history), None

        return tuple(history), block

    async def download_file(self, file: entity.File, temp_dir: str = "") -> None:
        tasks: List[asyncio.Task] = []
        blocks = await self._block_repo.get_blocks_grouped_by_number(file)
        self._init_progress(blocks)

        logger.debug(f"Blocks: {blocks}")
        blocks_with_data: List[entity.Block] = [None for _ in range(len(blocks))]
        index = 0
        while index < len(blocks) or tasks:
            for _ in range(self._parallel_num - len(tasks)):
                if index >= len(blocks):
                    break
                tasks.append(asyncio.create_task(self._download_block_by_group(blocks[index])))
                index += 1
            logger.info(tasks)

            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            done_tasks: List[asyncio.Task] = list(done)
            tasks = list(pending)
            for task in done_tasks:
                failed, block = task.result()
                if not block:
                    logger.error(f"Failed to load block: {failed}")
                    raise BlockDownloadFailed
                block.save(temp_dir)
                blocks_with_data[block.number] = block
                block.data = None

        self._merge_blocks(file.path, blocks_with_data, temp_dir)
        result, download_checksum, saved_checksum = self._check_hash(file)
        if not result:
            raise ChecksumNoEqual()

    @staticmethod
    def _check_hash(file: entity.File) -> Tuple[bool, str, str]:
        download_checksum = utils.sha1_checksum(file.path)
        saved_checksum = file.checksum
        return  download_checksum == saved_checksum, download_checksum, saved_checksum

    @property
    def progress(self):
        return self._progress
