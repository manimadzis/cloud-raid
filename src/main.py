import asyncio

from cli import CLI
from entities import Disk
from network.balancer import Balancer
from storage.block_repo import BlockRepo


async def main():
    block_repo = await BlockRepo("tmp.sqlite")
    # balancer = Balancer(await block_repo.get_disks())
    disks = [
        Disk(token="AQAAAABd-AuhAADLW8ndhqgT-k6qu5pkxYCYJ54", id_=1)
    ]
    balancer = Balancer(disks)

    cli = CLI(block_repo, balancer)
    await cli.start()


if __name__ == '__main__':
    asyncio.run(main())
