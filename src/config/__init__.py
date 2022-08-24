from dataclasses import dataclass

@dataclass
class Config:
    db_path: str

    @staticmethod
    def load(path: str) -> "Config":
        return Config(db_path='tmp.sqlite')