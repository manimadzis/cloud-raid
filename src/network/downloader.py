import aiohttp

from src.entities import Disk, File
from yandex_disk.dowload import DownloadStatus, download


class Downloader:
    def __init__(self, disk: Disk):
        self._disk = disk
        self._session = aiohttp.ClientSession()

    async def download(self, file: File) -> DownloadStatus:
        status, data = await download(self._disk.token, file.filename, self._session)
        if status == DownloadStatus.OK:
            file.data = data
        return status

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def close(self):
        await self._session.close()
