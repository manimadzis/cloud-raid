from abc import ABC, abstractmethod

import entity


class CipherBase(ABC):
    @abstractmethod
    def encrypt(self, data: bytes) -> bytes:
        pass

    @abstractmethod
    def decrypt(self, data: bytes) -> bytes:
        pass

    @abstractmethod
    def key(self) -> entity.Key:
        pass
