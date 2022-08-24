from dataclasses import dataclass

import toml


@dataclass
class Config:
    db_path: str

    @staticmethod
    def load(path: str) -> "Config":
        with open(path) as file:
            data = toml.load(file)
        # print(data)
        return Config(db_path='tmp.sqlite')
