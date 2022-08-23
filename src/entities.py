from copy import copy
from dataclasses import dataclass, field
from typing import Union


@dataclass(order=True)
class Disk:
    token: str = field(compare=False, repr=False)
    id_: int
    used_space: int = field(default=0, repr=False)
    total_space: int = field(default=0, repr=False)


@dataclass
class Block:
    filename: str = ""
    number: int = 0
    name: str = ""
    disk: Union[Disk, None] = None
    data: Union[bytes, None] = field(default=None, repr=False)

    def copy(self):
        return copy(self)


@dataclass
class File:
    filename: str
    path: str
    block_size: int = 2 * 2 ** 20
    duplicate_count: int = 1
