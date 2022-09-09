from typing import Tuple, Generator

import aiosqlite
from loguru import logger

import exceptions
from crypto.aes import Aes
from entities import Block, File, Key
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
        size INT NOT NULL,
        uploaded_blocks INTEGER NOT NULL,
        total_blocks INTEGER NOT NULL);
        """)

        await self.execute("""CREATE TABLE IF NOT EXISTS keys(
        id INTEGER PRIMARY KEY,
        key STRING NOT NULL UNIQUE);
        """)

        await self.execute("""CREATE TABLE IF NOT EXISTS blocks(
        id INTEGER PRIMARY KEY,
        number INTEGER NOT NULL,
        name STRING NOT NULL,
        size INTEGER NOT NULL,
        storage_id INTEGER NOT NULL,
        file_id INTEGER NOT NULL,
        key_id INTEGER,
        FOREIGN KEY (storage_id) REFERENCES storages(id),
        FOREIGN KEY (key_id) REFERENCES keys(id),
        FOREIGN KEY (file_id) REFERENCES files(id));
        """)

    async def add_block(self, block: Block) -> None:
        key_id = None
        logger.info(block)

        if block.cipher:
            key_id = block.cipher.key().id
        cur = await self.add_row('blocks', {
            'key_id': key_id,
            'storage_id': block.storage.id,
            'file_id': block.file.id,
            'name': block.name,
            'number': block.number,
            'size': block.size,
        })
        block.id = cur.lastrowid

        cur = await self.execute('UPDATE files '
                                 'SET uploaded_blocks = uploaded_blocks + 1 '
                                 'WHERE id = ?', (block.file.id,))


    async def add_storage(self, disk: StorageBase) -> None:
        await self.add_row('storages', {
            'token': disk.token,
            'type': str(disk.type)
        })

    async def add_file(self, file: File) -> None:
        cur = await self.add_row('files', {
            'size': file.size,
            'filename': file.filename,
            'uploaded_blocks': file.uploaded_blocks,
            'total_blocks': file.total_blocks,
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

    async def get_file_by_filename(self, filename: str) -> File:
        cur = await self.execute('SELECT id, size, uploaded_blocks, total_blocks '
                                 'FROM files '
                                 'WHERE filename = ?', (filename,))
        row = await cur.fetchone()
        if not row:
            raise exceptions.UnknownFile()

        file = File()
        file.id = row['id']
        file.size = row['size']
        file.uploaded_blocks = row['uploaded_blocks']
        file.total_blocks = row['total_blocks']
        file.filename = filename
        return file

    async def get_blocks_by_file(self, file: File) -> Tuple[Block]:
        """


        :param file:
        :return:
        """
        query = """
        SELECT
            b.id id,
            number,
            name,
            type,
            token,
            "key",
            key_id,
            storage_id
        FROM
            blocks b
            JOIN storages s ON b.storage_id = s.id
            JOIN keys k ON b.key_id = k.id
        WHERE
            b.file_id = ?
        ORDER BY number
        """

        cur = await self.execute(query, (file.id,))
        blocks = []
        async for row in cur:
            type_ = StorageType.from_str(row['type'])
            storage = StorageCreator.create(type_)
            storage.token = row['token']
            key = Key(id=row['key_id'], key=str(row['key']))
            cipher = Aes(key)
            blocks.append(Block(number=row['number'],
                                name=row['name'],
                                id=row['id'],
                                storage=storage,
                                cipher=cipher,
                                file=file
                                ))

        return tuple(blocks)

    async def get_storage_by_id(self, id_: int) -> StorageBase:
        cur = await self.execute('SELECT id, token, type '
                                 'FROM storages '
                                 'WHERE id = ?', (id_,))

        row = await cur.fetchone()
        if not row:
            raise exceptions.UnknownStorage()

        type_ = StorageType.from_str(row['type'])
        storage = StorageCreator.create(type_)
        storage.id = row['id']
        storage.token = row['token']

        return storage

    async def del_block(self, block: Block):
        cur = await self.execute('DELETE FROM blocks '
                                 'WHERE name = ?', (block.name,))

    async def del_file(self, file: File):
        cur = await self.execute('DELETE FROM files '
                                 'WHERE filename = ?', (file.filename,))

    async def add_key(self, key: Key) -> None:
        cur = await self.add_row('keys', {
            'key': key.key
        })
        key.id = cur.lastrowid

    async def get_keys(self) -> Tuple[Key]:
        cur = await self.execute('SELECT id, key '
                                 'FROM keys')
        keys = []
        async for row in cur:
            keys.append(Key(id=row['id'],
                            key=str(row['key'])))
        return tuple(keys)
