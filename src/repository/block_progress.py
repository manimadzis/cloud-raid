import dataclasses

@dataclasses.dataclass(init=True, kw_only=True)
class BlockProgress:
    done: int
    total: int
    block_number: int
    duplicate_number: int = 0
