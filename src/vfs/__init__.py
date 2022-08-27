from typing import List

from fs.memoryfs import MemoryFS

from entities import File
from repository.block_repo import BlockRepo


class VFS(MemoryFS):
    def __init__(self, block_repo: BlockRepo):
        super(VFS, self).__init__()
        self._block_repo = block_repo
        self._files: List[File] = None

    async def load(self):
        files = await self._block_repo.get_files()
        for file in files:
            self.makedirs(file.filename)
