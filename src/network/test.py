import asyncio

from entities import Disk, File
from storage.block_repo import BlockRepo
from network.balancer import Balancer
from network.uploader import Uploader


async def main():
    disks = [
        Disk(token="AQAAAABd-AuhAADLW8ndhqgT-k6qu5pkxYCYJ54", id_=1)
    ]

    b = Balancer(disks)
    r = await BlockRepo("db.sqlite")
    for disk in disks:
        await r.add_disk(disk)
    await r.commit()

    async with Uploader(b, r) as u:
        file = File("test.djvu")
        print(file)
        await u.upload_file(file)

if __name__ == '__main__':
    asyncio.run(main())
