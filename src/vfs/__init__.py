import io
from typing import List

from fs.memoryfs import MemoryFS
from fs.tree import render
from entities import File
from repository.block_repo import BlockRepo


class VFS(MemoryFS):
    def __init__(self, block_repo: BlockRepo):
        super(VFS, self).__init__()
        self._block_repo = block_repo
        self._files: List[File] = None

    async def load(self):
        self._files = await self._block_repo.get_files()
        for file in self._files:
            self.makedirs(file.filename)

    def tree(self) -> str:
        stream = io.StringIO()
        render(self, file=stream, with_color=True)
        return ''.join(stream.getvalue())
