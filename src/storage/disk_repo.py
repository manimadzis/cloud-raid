from typing import Tuple

from abstract_repo import AbstractRepo
from src.entities import Block, Disk, File


class BlocksRepo(AbstractRepo):
    def _create_tables(self) -> None:
        self._cur.execute("""CREATE TABLE IF NOT EXISTS disks(
        id INTEGER PRIMARY KEY,
        token STRING);
        """)

        self._cur.execute("""CREATE TABLE IF NOT EXISTS blocks(
        id INTEGER PRIMARY KEY,
        disk_id INTEGER,
        filename STRING,
        number INTEGER,
        name STRING );
        """)

    def add_block(self, block: Block) -> None:
        self.add_row('blocks', {
            'disk_id': block.disk_id,
            'filename': block.filename,
            'name': block.name,
            'number': block.number,
        })

    def add_disk(self, disk: Disk):
        self.add_row('disks', {
            'token': disk.token
        })

    def get_disks(self) -> Tuple[Disk]:
        cur = self.execute('SELECT token FROM disks;')

        disks = []
        for row in cur:
            disks.append(Disk(token=row['token']))
        return tuple(disks)

    def get_blocks(self, file: File) -> Tuple[Block]:
        cur = self.execute('SELECT * FROM blocks '
                           'WHERE filename = ? '
                           'ORDER BY number', (file.filename,))
        blocks = []
        for row in cur:
            blocks.append(Block(filename=row['filename'],
                                number=row['number'],
                                disk_id=row['disk_id'],
                                name=row['name']))
        return tuple(blocks)

if __name__ == '__main__':
    repo = BlocksRepo('tmp.sqlite3')

    repo.add_disk(Disk('1286573'))
    repo.add_disk(Disk('1234'))
    repo.add_disk(Disk('1235'))
    repo.add_disk(Disk('1237'))

    repo.add_block(Block('name', 1, 10, 1))
    repo.add_block(Block('name', 1, 10, 2))
    repo.add_block(Block('name', 1, 10, 3))
    repo.add_block(Block('name', 1, 10, 4))

    disks = repo.get_disks()
    print(disks)

    blocks = repo.get_blocks(File('name'))
    print(blocks)
