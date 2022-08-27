import asyncio
from typing import Tuple, Generator

import aiosqlite
from loguru import logger

from entities import Block, File
from network.storage_base import StorageBase, StorageType
from network.storage_creator import StorageCreator
from repository.abstract_repo import AbstractRepo


class BlockRepo(AbstractRepo):
    def __await__(self) -> Generator[None, None, "BlockRepo"]:
        return self._ainit().__await__()

    async def _create_tables(self) -> None:
        await self.execute("""CREATE TABLE IF NOT EXISTS storages(
        id INTEGER PRIMARY KEY,
        token STRING UNIQUE NOT NULL,
        type STRING NOT NULL);
        """)

        await self.execute("""CREATE TABLE IF NOT EXISTS files(
        id INTEGER PRIMARY KEY,
        filename STRING NOT NULL UNIQUE,
        size INT NOT NULL);
        """)

        await self.execute("""CREATE TABLE IF NOT EXISTS blocks(
        id INTEGER PRIMARY KEY,
        storage_id INTEGER NOT NULL,
        file_id INTEGER NOT NULL,
        number INTEGER NOT NULL,
        name STRING NOT NULL,
        FOREIGN KEY (storage_id) REFERENCES storages(id),
        FOREIGN KEY (file_id) REFERENCES files(id));
        """)

    async def add_block(self, block: Block) -> None:
        cur = await self.add_row('blocks', {
            'storage_id': block.storage.id,
            'file_id': block.file.id,
            'name': block.name,
            'number': block.number,
        })
        block.id = cur.lastrowid

    async def add_storage(self, disk: StorageBase) -> None:
        await self.add_row('storages', {
            'token': disk.token,
            'type': str(disk.type)
        })

    async def add_file(self, file: File):
        cur = await self.add_row('files', {
            'size': file.size,
            'filename': file.filename,
        })
        file.id = cur.lastrowid

    async def get_files(self) -> Tuple[File]:
        files = []
        cur = await self.execute('SELECT id, filename, size '
                                 'FROM files')
        for row in await cur.fetchall():
            files.append(File(filename=row['filename'],
                              id=row['id'],
                              size=row['size']))
        return tuple(files)

    async def get_storages(self) -> Tuple[StorageBase]:
        cur = await self.execute('SELECT id, token, type '
                                 'FROM storages')

        disks = []
        async for row in cur:
            type_ = StorageType.from_str(row['type'])
            storage = StorageCreator.create(type_)
            storage.id = row['id']
            storage.token = row['token']
            disks.append(storage)
        return tuple(disks)

    async def get_token(self, disk: StorageBase) -> StorageBase:
        cur = await self.execute('SELECT token '
                                 'FROM storages '
                                 'WHERE id = ?', (disk.id,))
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

    async def get_disk_by_id(self, id_: int) -> StorageBase:
        cur = await self.execute('SELECT token, type '
                                 'FROM storages '
                                 'WHERE id = ?', (id_,))
        row = await cur.fetchone()
        type_ = StorageType.from_str(row['type'])
        storage = StorageCreator.create(type_)
        storage.id= id_
        storage.token = row['token']
        storage.type = type_
        return storage

    async def get_blocks(self, file: File) -> Tuple[Block]:
        cur = await self.execute('SELECT id, number, name, storage_id, file_id '
                                 'FROM blocks '
                                 'WHERE file_id = ? '
                                 'ORDER BY number', (file.id,))
        blocks = []
        storage_ids = []
        async for row in cur:
            blocks.append(Block(number=row['number'],
                                name=row['name'],
                                id=row['id']))
            storage_ids.append(row['storage_id'])

        storages = {}
        tasks = []

        storage_ids_set = set(storage_ids)
        for storage_id in storage_ids_set:
            tasks.append(asyncio.create_task(self.get_disk_by_id(storage_id)))
        await asyncio.gather(*tasks)

        for task, storage_id in zip(tasks, storage_ids_set):
            storages[storage_id] = task.result()

        for block, storage_id in zip(blocks, storage_ids):
            block.storage = storages[storage_id]
            block.file = file

        return tuple(blocks)

    async def get_storage_by_id(self, id_: int) -> StorageBase:
        cur = await self.execute('SELECT id, token, type '
                                 'FROM storages '
                                 'WHERE id = ?', (id_,))

        row = await cur.fetchone()
        type_ = StorageType.from_str(row['type'])
        storage = StorageCreator.create(type_)
        storage.id = row['id']
        storage.token = row['token']

        return storage

