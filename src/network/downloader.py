import asyncio
from typing import List, Tuple

import aiohttp

from entities import File, Block
from storage.block_repo import BlockRepo
from network.yandex_disk.dowload import DownloadStatus, download


class Downloader:
    def __init__(self, block_repo: BlockRepo):
        self._block_repo = block_repo
        self._session = aiohttp.ClientSession()

    async def download(self, file: File) -> DownloadStatus:
        return await self._download_file(file)

    async def _download_block(self, block: Block) -> Tuple[DownloadStatus, Block]:
        status, data = await download(block.disk.token, block.name, self._session)
        block.data = data
        return status, block

    async def _download_file(self, file: File,
                             worker_count=5,
                             ) -> DownloadStatus:
        tasks: List[asyncio.Task] = []
        blocks = await self._block_repo.get_blocks(file)
        index = 0
        while index < len(blocks) or tasks:

            for _ in range(worker_count - len(tasks)):
                if index >= len(blocks):
                    break
                tasks.append(asyncio.create_task(self._download_block(blocks[index])))
                index += 1

            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            status, block = list(done)[0].result()
            tasks = list(pending)
            print(status, block)
            if status == DownloadStatus.OK:
                block.save()

        return DownloadStatus.OK

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def close(self):
        await self._session.close()
