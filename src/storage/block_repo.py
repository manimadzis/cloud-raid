import asyncio
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
        token STRING UNIQUE NOT NULL);
        """)

        await self.execute("""CREATE TABLE IF NOT EXISTS files(
        id INTEGER PRIMARY KEY,
        filename STRING NOT NULL,
        size INT NOT NULL);
        """)

        await self.execute("""CREATE TABLE IF NOT EXISTS blocks(
        id INTEGER PRIMARY KEY,
        disk_id INTEGER NOT NULL,
        file_id INTEGER NOT NULL,
        number INTEGER NOT NULL,
        name STRING NOT NULL,
        FOREIGN KEY (disk_id) REFERENCES disks(id),
        FOREIGN KEY (file_id) REFERENCES files(id));
        """)

    async def add_block(self, block: Block) -> None:
        cur = await self.add_row('blocks', {
            'disk_id': block.disk.id,
            'file_id': block.file.id,
            'name': block.name,
            'number': block.number,
        })
        block.id = cur.lastrowid

    async def add_disk(self, disk: Disk) -> None:
        await self.add_row('disks', {
            'token': disk.token
        })

    async def add_file(self, file: File):
        cur = await self.add_row('files', {
            'size': file.size,
            'filename': file.filename,
        })
        file.id = cur.lastrowid

    async def get_files(self) -> Tuple[File]:
        files = []
        cur = await self.execute('SELECT id, filename, size FROM files;')
        for row in await cur.fetchall():
            files.append(File(filename=row['filename'],
                              id=row['id'],
                              size=row['size']))
        return tuple(files)

    async def get_disks(self) -> Tuple[Disk]:
        cur = await self.execute('SELECT id, token FROM disks;')

        disks = []
        async for row in cur:
            disks.append(Disk(id=row['id'], token=row['token']))
        return tuple(disks)

    async def get_token(self, disk: Disk) -> Disk:
        cur = await self.execute('SELECT token FROM disks WHERE id = ?', (disk.id,))
        try:
            disk.token = (await cur.fetchone())['token']
        except aiosqlite.Error as e:
            logger.exception(e)

        return disk

    async def get_file_by_id(self, file: File) -> File:
        cur = await self.execute('SELECT filename, size '
                                 'FROM files '
                                 'WHERE id = ?', (file.id,))
        row = await cur.fetchone()
        file.filename = row['filename']
        file.size = row['size']
        return file

    async def get_file_by_filename(self, file: File) -> File:
        cur = await self.execute('SELECT id, size '
                                 'FROM files '
                                 'WHERE filename = ?', (file.filename,))
        row = await cur.fetchone()
        file.id = row['id']
        file.size = row['size']
        return file

    async def get_disk(self, disk: Disk) -> Disk:
        cur = await self.execute('SELECT token '
                                 'FROM disks '
                                 'WHERE id = ?', (disk.id,))
        row = await cur.fetchone()
        disk.token = row['token']
        return disk

    async def get_blocks(self, file: File) -> Tuple[Block]:
        cur = await self.execute('SELECT id, number, name, disk_id, file_id FROM blocks '
                                 'WHERE file_id = ? '
                                 'ORDER BY number', (file.id,))
        blocks = []
        disk_ids = []
        async for row in cur:
            blocks.append(Block(number=row['number'],
                                name=row['name'],
                                id=row['id']))
            disk_ids.append(row['disk_id'])

        disks = {}
        tasks = []
        for disk_id in disk_ids:
            disks[disk_id] = Disk(id=disk_id)
            tasks.append(asyncio.create_task(self.get_disk(disks[disk_id])))
        await asyncio.gather(*tasks)

        for block, disk_id in zip(blocks, disk_ids):
            block.disk = disks[disk_id]
            block.file = file

        return tuple(blocks)
