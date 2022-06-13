from enum import Enum, auto
from typing import Tuple, Union

import aiohttp
from loguru import logger


class DownloadStatus(Enum):
    OK = auto()
    BAD_DOWNLOAD_URL = auto()
    BAD_DOWNLOAD_REQUEST = auto()


async def download(token: str, filename: str, session: aiohttp.ClientSession) -> Tuple[DownloadStatus, Union[bytes, None]]:
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'OAuth {token}'
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
            return DownloadStatus.BAD_DOWNLOAD_URL, None

    async with session.get(download_url) as resp:
        if resp.status == 200:
            content = await resp.read()
        else:
            logger.error(f"Failed to download file. Code: {resp.status}")
            return DownloadStatus.BAD_DOWNLOAD_REQUEST, None

    return DownloadStatus.OK, content
