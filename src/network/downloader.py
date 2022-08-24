import asyncio
from typing import List, Tuple

import aiohttp
from loguru import logger

from entities import File, Block
from network.yandex_disk.dowload import DownloadStatus, download
from storage.block_repo import BlockRepo


class Downloader:
    def __init__(self, block_repo: BlockRepo):
        self._block_repo = block_repo
        self._session = None

    async def _download_block(self, block: Block) -> Tuple[DownloadStatus, Block]:
        status, data = await download(block.disk.token, block.name, self._session)
        block.data = data
        if status == DownloadStatus.OK:
            logger.info(f"Download block: {block}")
        else:
            logger.info(f"Failed to download block: {block}: {status}")

        return status, block

    async def count_blocks(self, src: str):
        blocks = await self._block_repo.get_blocks(File(filename=src))
        return len(blocks)

    async def download_file(self, file: File, temp_dir: str = "") -> None:
        tasks: List[asyncio.Task] = []
        blocks = await self._block_repo.get_blocks(file)
        index = 0
        blocks_count = 0
        while index < len(blocks) or tasks:
            for _ in range(file.worker_count - len(tasks)):
                if index >= len(blocks):
                    break
                tasks.append(asyncio.create_task(self._download_block(blocks[index])))
                index += 1

            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            done_tasks: List[asyncio.Task] = list(done)
            tasks = list(pending)

            for task in done_tasks:
                status, block = task.result()

                if status != DownloadStatus.OK:
                    tasks.append(asyncio.create_task(self._download_block(blocks[index])))
                else:
                    block.save(temp_dir)
                    blocks_count += 1

            yield blocks_count

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def close(self):
        await self._session.close()
