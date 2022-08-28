import os
from copy import copy
from dataclasses import dataclass, field


@dataclass(kw_only=True)
class File:
    id: int = 0
    filename: str = ""
    path: str = ""
    size: int = 0

    # setting
    block_size: int = 0
    duplicate_count: int = 1
    worker_count: int = 10


import network.storage_base


@dataclass(kw_only=True)
class Block:
    id: int = 0
    name: str = ""
    number: int = 0

    file: File = None
    storage: network.storage_base.StorageBase = None
    data: bytes = field(default=None, repr=False)

    def copy(self):
        return copy(self)

    def save(self, path: str = ""):
        filename = os.path.join(path, self.name)
        with open(filename, "wb") as file:
            file.write(self.data)
