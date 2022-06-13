from enum import Enum, auto

import aiohttp
from loguru import logger


class UploadStatus(Enum):
    OK = auto()
    FILE_ALREADY_EXISTS = auto()
    BAD_PUT_URL = auto()
    BAD_PUT_REQUEST = auto()
    BAD_DATA = auto()


async def upload(token: str, filename: str, data: bytes, session: aiohttp.ClientSession,
                 overwrite=False) -> UploadStatus:
    
    if data is None or len(data) == 0:
        return UploadStatus.BAD_DATA

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'OAuth {token}'
    }

    params = {
        'path': filename
    }

    if overwrite:
        params['overwrite'] = 'true'

    async with session.get('https://cloud-api.yandex.net/v1/disk/resources/upload', headers=headers,
                           params=params) as resp:
        if resp.status == 200:
            json_data = await resp.json()
            put_url = json_data.get('href', '')
        elif resp.status == 409:
            logger.error(f"File '{filename}' already exists")
            return UploadStatus.FILE_ALREADY_EXISTS
        else:
            logger.error(f"Bad response. Code: {resp.status}")
            return UploadStatus.BAD_PUT_URL

    if put_url == '':
        logger.error("Empty PUT URL")
    else:
        async with session.put(put_url, data=data) as resp:
            if resp.status != 201:
                return UploadStatus.BAD_PUT_REQUEST

    return UploadStatus.OK
