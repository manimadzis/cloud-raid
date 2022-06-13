import asyncio
from typing import Tuple

import aiohttp

from disk import Disk
from yandex_disk.get_disk_size import get_disk_size


class Monitor:
    def __init__(self):
        self._session = aiohttp.ClientSession()

    async def disk_size(self, disk: Disk) -> Tuple[int, int]:
        return await get_disk_size(disk.token, self._session)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def close(self):
        await self._session.close()


async def main():
    monitor = Monitor()
    used, total = await monitor.disk_size(Disk('AQAAAABd-AuhAADLW8ndhqgT-k6qu5pkxYCYJ54'))
    print(used, total)
    await monitor.close()


if __name__ == '__main__':
    asyncio.run(main())
