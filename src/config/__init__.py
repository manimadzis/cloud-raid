from dataclasses import dataclass

import toml


@dataclass
class Config:
    db_path: str
    min_block_size: int
    max_block_size: int

    @staticmethod
    def load(path: str) -> "Config":
        with open(path) as file:
            data = toml.load(file)
        # print(data)
        return Config(db_path='tmp.sqlite',
                      min_block_size=10, max_block_size=100)
