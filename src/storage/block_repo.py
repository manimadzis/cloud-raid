from typing import Tuple, Generator

import aiosqlite
from loguru import logger

from entities import Block, Disk, File
from storage.abstract_repo import AbstractRepo


class BlockRepo(AbstractRepo):
    def __await__(self) -> Generator[None, None, "BlockRepo"]:
        return self._ainit().__await__()

    async def _create_tables(self) -> None:
        await self.execute("""CREATE TABLE IF NOT EXISTS disks(
        id INTEGER PRIMARY KEY,
        token STRING NOT NULL);
        """)

        await self.execute("""CREATE TABLE IF NOT EXISTS blocks(
        id INTEGER PRIMARY KEY,
        disk_id INTEGER NOT NULL,
        filename STRING NOT NULL,
        number INTEGER NOT NULL,
        name STRING NOT NULL,
        FOREIGN KEY (disk_id) REFERENCES disks(id));
        """)

    async def add_block(self, block: Block) -> None:
        await self.add_row('blocks', {
            'disk_id': block.disk.id_,
            'filename': block.filename,
            'name': block.name,
            'number': block.number,
        })

    async def add_disk(self, disk: Disk) -> None:
        await self.add_row('disks', {
            'token': disk.token
        })

    async def get_disks(self) -> Tuple[Disk]:
        cur = await self.execute('SELECT id, token FROM disks;')

        disks = []
        async for row in cur:
            disks.append(Disk(id_=row['id'], token=row['token']))
        return tuple(disks)

    async def get_token(self, disk_id: int) -> str:
        cur = await self.execute('SELECT token FROM disks WHERE id = ?', (disk_id,))
        try:
            token = (await cur.fetchone())['token']
        except aiosqlite.Error as e:
            logger.error(e)
            token = ""

        return token

    async def get_blocks(self, file: File) -> Tuple[Block]:
        cur = await self.execute('SELECT * FROM blocks '
                                 'WHERE filename = ? '
                                 'ORDER BY number', (file.filename,))
        blocks = []
        async for row in cur:
            token = await self.get_token(row['disk_id'])
            disk = Disk(id_=row['disk_id'], token=token)
            blocks.append(Block(filename=row['filename'],
                                number=row['number'],
                                disk=disk,
                                name=row['name']))
        return tuple(blocks)

# if __name__ == '__main__':
#     repo = BlocksRepo('tmp.sqlite')
#
#     repo.add_disk(Disk('1286573'))
#     repo.add_disk(Disk('1234'))
#     repo.add_disk(Disk('1235'))
#     repo.add_disk(Disk('1237'))
#
#     repo.add_block(Block('name', 1, 10, 1))
#     repo.add_block(Block('name', 1, 10, 2))
#     repo.add_block(Block('name', 1, 10, 3))
#     repo.add_block(Block('name', 1, 10, 4))
#
#     disks = repo.get_disks()
#     print(disks)
#
#     blocks = repo.get_blocks(File('name'))
#     print(blocks)
