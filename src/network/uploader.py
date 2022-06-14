import asyncio
from uuid import uuid4

import aiohttp

from src.entities import Disk, File
from yandex_disk.upload import upload, UploadStatus


class Uploader:
    def __init__(self, disk: Disk):
        self._disk = disk
        self._session = aiohttp.ClientSession()

    async def upload(self, file: File, overwrite=False) -> UploadStatus:
        return await upload(self._disk.token, file.filename, file.data, self._session, overwrite=overwrite)

    @staticmethod
    async def _worker(name: str, queue: asyncio.Queue):
        while True:
            task = await queue.get()
            status = await task
            print(name, status)
            queue.task_done()

    async def upload_by_blocks(self, file: File, block_size=10 * 2 ** 20, workers=10, queue_size=10):
        """
        Split file into blocks of size block_size and upload them to cloud

        :param file: need filename only
        :param block_size: block size in bytes
        :param workers: number of workers
        :param queue_size: max pool size (block_size * queue_size bytes of RAM needed)
        :return:
        """

        queue = asyncio.Queue(maxsize=queue_size)
        with open(file.filename, 'rb') as f:
            done = False
            while not done:

                block = f.read(block_size)
                block_file = File(str(uuid4()), block)

                while not queue.full() and block:
                    queue.put_nowait(self.upload(block_file, overwrite=False))
                    block = f.read(block_size)
                    block_file = File(str(uuid4()), block)

                if not block:
                    done = True

                task_pool = []
                for i in range(workers):
                    task_pool.append(asyncio.create_task(self._worker(f"worker-{i}", queue)))

                await queue.join()

                for i in range(workers):
                    task_pool[i].cancel()

                await asyncio.gather(*task_pool, return_exceptions=True)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def close(self):
        await self._session.close()
