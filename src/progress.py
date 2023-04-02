
class Progress:
    def __init__(self, all):
        self._all = all
        self._done: int = 0

    def add(self, inc: int):
        self._done += inc

    @property
    def ratio(self) -> float:
        return self._done / self._all

    @property
    def all(self) -> int:
        return self._all

    @all.setter
    def all(self, value: int) -> None:
        if value <= 0:
            raise ValueError(f"value should be > 0, but {value} is given")
        self._all = value


class ProgressGroup:
    def __init__(self, count: int):
        self._progresses = [Progress(0) for _ in range(count)]
    
    def add(self, index: int, value: int):
        self._progresses[index].add(value)
    
    @property
    def all(self, index) -> int:
        return self._progress[index].all

    @all.setter
    def all(self, index: int, value: int) -> None:
        if value <= 0:
            raise ValueError(f"value should be > 0, but {value} is given")
        self._progresses[index].all = value
    