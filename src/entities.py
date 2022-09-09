import os
from copy import copy
from dataclasses import dataclass, field




@dataclass(kw_only=True)
class File:
    id: int = 0
    filename: str = ""
    size: int = 0
    total_blocks: int = 0
    uploaded_blocks: int = 0

    path: str = ""

    # setting
    block_size: int = 0
    duplicate_count: int = 1
    worker_count: int = 5


@dataclass(kw_only=True)
class Key:
    id: int = 0
    key: str = ""


import network.storage_base
import crypto

@dataclass(kw_only=True)
class Block:
    id: int = 0
    name: str = ""
    number: int = 0
    size: int = 0

    file: File = None
    storage: network.storage_base.StorageBase = None
    cipher: crypto.CipherBase = None

    data: bytes = field(default=None, repr=False)
    def copy(self):
        return copy(self)

    def save(self, path: str = ""):
        filename = os.path.join(path, self.name)
        with open(filename, "wb") as file:
            file.write(self.data)
