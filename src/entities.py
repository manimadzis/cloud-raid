from dataclasses import dataclass
from typing import Union


@dataclass
class Block:
    filename: str
    number: int
    disk_id: int
    name: int


@dataclass
class Disk:
    token: str


@dataclass
class File:
    filename: str
    data: Union[bytes, None] = None
