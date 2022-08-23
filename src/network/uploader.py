import asyncio
from typing import Iterator

import aiohttp
from entities import File, Block
from network.balancer import Balancer
from network.yandex_disk.upload import upload, UploadStatus
from storage.block_repo import BlockRepo


class Uploader:
    def __init__(self, balancer: Balancer, blocks_repo: BlockRepo):
        self._balancer = balancer
        self._blocks_repo = blocks_repo
        self._session = aiohttp.ClientSession()

    async def _upload_block(self, block: Block) -> UploadStatus:
        status = await upload(block.disk.token, block.name, block.data, self._session)
        if status != UploadStatus.OK:
            print(f"Bad {status}: ", block)
        else:
            print("Ok:", block)
        return status

    @staticmethod
    def _block_generator(file: File, duplicate_count=1) -> Iterator[Block]:
        with open(file.path, "rb") as f:
            data = f.read(file.block_size)
            number = 0
            while data:
                for _ in range(duplicate_count):
                    yield Block(filename=file.path, number=number, data=data)
                data = f.read(file.block_size)
                number += 1

    async def upload_file(self, file: File, worker_count=10, duplication_count=1) -> UploadStatus:
        tasks = set()
        block_generator = self._block_generator(file, duplication_count)
        first = True
        while len(tasks) != 0 or first:
            first = False
            try:
                for _ in range(worker_count - len(tasks)):
                    block = next(block_generator)
                    self._balancer.fill_block(block)
                    tasks |= {self._upload_block(block)}
            except StopIteration:
                pass
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        return UploadStatus.OK



    async def __aenter__(self) -> "Uploader":
        return self

    async def __aexit__(self, *args):
        await self._session.close()


if __name__ == '__main__':
    u = Uploader()
