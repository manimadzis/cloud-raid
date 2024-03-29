from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Union, Tuple, Generator, Any, Iterator, Dict, List, Callable

import aiohttp
from tqdm.asyncio import tqdm
import entity


class StorageType(Enum):
    YANDEX_DISK = 'yandex-disk'

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s: str) -> Union["StorageType", None]:
        for _, v in StorageType.__members__.items():
            if v.value == s:
                return v
        return None


class UploadStatus(Enum):
    OK = 'Ok'
    FAILED = 'Failed'
    FILE_EXISTS = 'File Exists'


class DownloadStatus(Enum):
    OK = 'Ok'
    FAILED = 'Failed'
    FILE_DOESNT_EXITS = 'File Doesn\'t Exist'

class DeleteStatus(Enum):
    OK = 'Ok'
    FAILED = 'Failed'


@dataclass(kw_only=True)
class StorageBase(ABC):
    id: int = 0
    token: str = field(default="", compare=False, repr=False)

    used_space: int = field(default=0, repr=False)
    total_space: int = field(default=0, repr=False)

    type: StorageType = None

    def __lt__(self, other):
        return self.used_space/self.total_space < other.used_space/other.total_space

    @abstractmethod
    async def upload(self, filename: str, data: bytes, session: aiohttp.ClientSession) -> UploadStatus:
        pass

    @abstractmethod
    async def upload_by_chunks(self, filename: str, data: tqdm, session: aiohttp.ClientSession) -> UploadStatus:
        pass

    @abstractmethod
    async def download(self, filename: str, session: aiohttp.ClientSession) -> DownloadStatus:
        pass

    @abstractmethod
    async def download_by_chunks(self, filename: str, chunk_size: int, inc_progress: Callable[[], None], session: aiohttp.ClientSession) -> Tuple[DownloadStatus, bytes]:
        pass

    @abstractmethod
    async def size(self, session: aiohttp.ClientSession) -> Tuple[int, int]:
        pass

    @abstractmethod
    async def delete(self, filename: str, session: aiohttp.ClientSession) -> DeleteStatus:
        pass

    @abstractmethod
    async def files(self, session: aiohttp.ClientSession) -> Tuple[DownloadStatus, Tuple[entity.File]]:
        pass
