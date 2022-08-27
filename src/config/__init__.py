import dataclasses
from dataclasses import dataclass
import json
import toml


@dataclass(kw_only=True)
class Config:
    db_path: str = "db.sqlite"
    min_block_size: int = int(0.5 * 2 ** 20)
    max_block_size: int = 20 * 2 ** 20


    @staticmethod
    def load(path: str) -> "Config":
        with open(path) as file:
            data = toml.load(file)
            
        return Config(db_path=data.get("db_path") or Config.db_path,
                      min_block_size=data.get("min_block_size") or Config.min_block_size, 
                      max_block_size=data.get("max_block_size") or Config.max_block_size)

    def save(self, path: str):
        with open(path, "w") as f:
            toml.dump(dataclasses.asdict(self), f)
