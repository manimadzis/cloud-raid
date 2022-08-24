import os
from copy import copy
from dataclasses import dataclass, field
from typing import Union


@dataclass(order=True, kw_only=True)
class Disk:
    id_: int = 0
    token: str = field(default="", compare=False, repr=False)
    used_space: int = field(default=0, repr=False)
    total_space: int = field(default=0, repr=False)


@dataclass(kw_only=True)
class Block:
    filename: str = ""
    number: int = 0
    name: str = ""
    disk: Union[Disk, None] = None
    data: Union[bytes, None] = field(default=None, repr=False)

    def copy(self):
        return copy(self)

    def save(self, path: str = ""):
        filename = os.path.join(path, self.name)
        with open(filename, "wb") as file:
            file.write(self.data)


@dataclass(kw_only=True)
class File:
    filename: str
    path: str = ""

    # setting
    block_size: int = int(0.5 * 2 ** 20)
    duplicate_count: int = 1
    worker_count: int = 10
