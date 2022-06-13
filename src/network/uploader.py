import asyncio
from uuid import uuid4

import aiohttp

from disk import Disk
from file import File
from yandex_disk.upload import upload, UploadStatus


class Uploader:
    def __init__(self, disk: Disk):
        self._disk = disk
        self._session = aiohttp.ClientSession()

    async def upload(self, file: File, overwrite=False) -> UploadStatus:
        return await upload(self._disk.token, file.filename, file.data, self._session, overwrite=overwrite)

    async def upload_by_blocks(self, file: File, overwrite=False, block_size=10 * 2 ** 20):
        queue = asyncio.Queue(maxsize=10)
        with open(file.filename, 'rb') as f:
            block = f.read(block_size)
            block_file = File(str(uuid4()), block)
            is_added = False

            while block:
                if not queue.full():
                    queue.put_nowait(self.upload(block_file, overwrite=overwrite))
                    is_added = True
                else:
                    while not queue.empty():
                        await queue.get_nowait()
                        print(f"add {block[:10]}")
                        queue.task_done()

                # if not queue.empty():
                #     await queue.join()

                if is_added:
                    block = f.read(block_size)
                    block_file = File(str(uuid4()), block)
                    is_added = False

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def close(self):
        await self._session.close()


async def main():
    uploader = Uploader(Disk('AQAAAABd-AuhAADLW8ndhqgT-k6qu5pkxYCYJ54'))
    await uploader.upload_by_blocks(File("[КГ] [У] Роджерс Д. - Алгоритмические основы машинной графики(1).djvu"), block_size=1024)
    await uploader.close()


if __name__ == '__main__':
    asyncio.run(main())