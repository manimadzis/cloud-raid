from dataclasses import dataclass
from typing import Union


@dataclass
class File:
    filename: str
    data: Union[bytes, None] = None
