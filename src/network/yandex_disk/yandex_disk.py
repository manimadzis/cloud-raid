from typing import Tuple, Iterator

import aiohttp
from loguru import logger
from tqdm.asyncio import tqdm

from entities import File
from network.storage_base import StorageBase, DownloadStatus, UploadStatus, StorageType, DeleteStatus


class YandexDisk(StorageBase):
    def __init__(self):
        super(YandexDisk, self).__init__()
        self.type = StorageType.YANDEX_DISK

    async def upload(self, filename: str, data: bytes, session: aiohttp.ClientSession) -> UploadStatus:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

        params = {
            'path': filename
        }

        async with session.get('https://cloud-api.yandex.net/v1/disk/resources/upload', headers=headers,
                               params=params) as resp:
            if resp.status == 200:
                json_data = await resp.json()
                put_url = json_data.get('href', '')
            elif resp.status == 409:
                logger.error(f"File '{filename}' already exists")
                return UploadStatus.FILE_EXISTS
            else:
                logger.error(f"Bad response. Code: {resp.status}")
                return UploadStatus.FAILED

        if put_url == '':
            logger.error("Empty PUT URL")
        else:
            async with session.put(put_url, data=data) as resp:
                if resp.status != 201:
                    return UploadStatus.FAILED

        return UploadStatus.OK

    async def upload_by_chunks(self, filename: str, data: Iterator[bytes], session: aiohttp.ClientSession) -> UploadStatus:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

        params = {
            'path': filename
        }

        async with session.get('https://cloud-api.yandex.net/v1/disk/resources/upload', headers=headers,
                               params=params) as resp:
            if resp.status == 200:
                json_data = await resp.json()
                put_url = json_data.get('href', '')
            elif resp.status == 409:
                logger.error(f"File '{filename}' already exists")
                return UploadStatus.FILE_EXISTS
            else:
                logger.error(f"Bad response. Code: {resp.status}")
                return UploadStatus.FAILED

        if put_url == '':
            logger.error("Empty PUT URL")
        else:
            async with session.put(put_url, data=data) as resp:
                if resp.status != 201:
                    return UploadStatus.FAILED

        return UploadStatus.OK

    async def download(self, filename: str, session: aiohttp.ClientSession) -> Tuple[DownloadStatus, bytes]:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

        params = {
            'path': filename
        }

        async with session.get('https://cloud-api.yandex.net/v1/disk/resources/download', headers=headers,
                               params=params) as resp:
            if resp.status == 200:
                json_data = await resp.json()
                download_url = json_data.get('href', '')
            else:
                logger.error(f"Bad download response. Code: {resp.status}")
                return DownloadStatus.FAILED, bytes()

        async with session.get(download_url) as resp:
            if resp.status == 200:
                content = await resp.read()
            else:
                logger.error(f"Failed to download file. Code: {resp.status}")
                return DownloadStatus.FAILED, bytes()

        return DownloadStatus.OK, content

    async def size(self, session: aiohttp.ClientSession) -> Tuple[int, int]:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

        params = {
            'fields': 'total_space,used_space'
        }

        async with session.get('https://cloud-api.yandex.net/v1/disk/', headers=headers,
                               params=params) as resp:
            if resp.status == 200:
                json_data = await resp.json()
                return json_data['used_space'], json_data['total_space']
            else:
                logger.error(f"Bad response. Code: {resp.status}")
                return 0, 0

    async def delete(self, filename: str, session: aiohttp.ClientSession) -> DeleteStatus:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

        params = {
            'path': filename,
            'permanently': 'true',
            'force_async': 'true'
        }

        async with session.delete('https://cloud-api.yandex.net/v1/disk/resources', headers=headers,
                                  params=params) as resp:
            if resp.status in (202, 204):
                return DeleteStatus.OK
            else:
                logger.error(f"Bad response. Code: {resp.status}")
                return DeleteStatus.FAILED

    async def files(self, session: aiohttp.ClientSession) -> Tuple[File]:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

        params = {
            'limit': 1000
        }
        files = []

        async with session.get('https://cloud-api.yandex.net/v1/disk/resources/files', headers=headers,
                               params=params) as resp:
            if resp.status == 200:
                json_data = await resp.json()
                for file in json_data['items']:
                    files.append(
                            File(filename=file['name'], size=file['size'])
                    )
            else:
                logger.error(f"Bad response. Code: {resp.status}")

        return tuple(files)
