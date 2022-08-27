from .storage import StorageBase, StorageType
from .yandex_disk import YandexDisk


class StorageCreator:
    @staticmethod
    def create(storage_type: StorageType) -> StorageBase:
        if storage_type == StorageType.YANDEX_DISK:
            return YandexDisk()
