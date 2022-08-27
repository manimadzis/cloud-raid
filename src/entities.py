import os
from copy import copy
from dataclasses import dataclass, field

import network.storage as _storage


@dataclass(kw_only=True)
class File:
    id: int = 0
    filename: str = ""
    path: str = ""
    size: int = 0

    # setting
    block_size: int = int(2 * 2 ** 20)
    duplicate_count: int = 1
    worker_count: int = 10


@dataclass(kw_only=True)
class Block:
    id: int = 0
    name: str = ""
    number: int = 0

    file: File = None
    storage: _storage.StorageBase = None
    data: bytes = field(default=None, repr=False)

    def copy(self):
        return copy(self)

    def save(self, path: str = ""):
        filename = os.path.join(path, self.name)
        with open(filename, "wb") as file:
            file.write(self.data)

