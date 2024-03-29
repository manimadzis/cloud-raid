import argparse
from typing import Callable, Coroutine


class Parser(argparse.ArgumentParser):
    def __init__(self):
        super(Parser, self).__init__()

        self.add_argument('-d', '--db', dest='db_path', default="db.sqlite", help="Path to config file")
        self.add_argument('--debug', action="store_true", help="Enable debug output")
        self.add_argument('--log', help="Path to log file", default="log.txt", dest="log_path")
        self.add_argument('-w', '--worker-count', help="Count of simultaneous workers (connections)", default=5,
                          type=int, dest="worker_count")

        subparsers = self.add_subparsers(parser_class=argparse.ArgumentParser)
        # UPLOAD
        self._upload = subparsers.add_parser("upload", help="Upload file")

        self._upload.add_argument("src", help="Path to file")
        self._upload.add_argument("dst", help="Filename in system", nargs='?', default="")
        self._upload.add_argument("-b", "--block-size", help="Size of block in bytes", type=int,
                                  default=20 * 2 ** 20, dest="block_size")


        self._upload.add_argument("-e", "--encrypt", action="store_true", dest="need_encrypt")

        # DOWNLOAD
        self._download = subparsers.add_parser("download", help="Download file")

        self._download.add_argument("src", help="Path to file")
        self._download.add_argument("dst", help="Filename in system", nargs='?', default="")
        self._download.add_argument("--temp-dir", help="Path to temp directory", default="blocks",
                                    dest="temp_dir")

        # STORAGE ADD/LIST/FILES/DELETE/WIPE
        self._storage = subparsers.add_parser("storage", help="Storage options")
        storage_subparsers = self._storage.add_subparsers(parser_class=argparse.ArgumentParser)
        self._storage_add = storage_subparsers.add_parser("add", help="Add new storage")
        self._storage_list = storage_subparsers.add_parser("list", help="List storages")
        self._storage_files = storage_subparsers.add_parser("files", help="List files on storage")
        self._storage_delete = storage_subparsers.add_parser("delete", help="Delete files from storage")
        self._storage_wipe = storage_subparsers.add_parser("wipe", help="Wipe storage")

        self._storage_add.add_argument("type", help="Type of storage", choices=["yandex-disk"])
        self._storage_add.add_argument("token", help="Authorization token")

        self._storage_files.add_argument("storage_id")

        self._storage_delete.add_argument("storage_id")
        self._storage_delete.add_argument("filenames", nargs="+")

        self._storage_wipe.add_argument("storage_id")
        # LIST
        self._list = subparsers.add_parser("list", help="Show list of files")
        self._list.add_argument("-s", "--size", action="store_true", help="Show size of files")
        self._list.add_argument("-c", "--color", action="store_true", help="Show colorized")

        # DELETE
        self._delete = subparsers.add_parser("delete", help="Delete file")
        self._delete.add_argument("filenames", nargs="+")

        # KEY ADD/LIST
        self._key = subparsers.add_parser("key", help="Key options")
        key_subparsers = self._key.add_subparsers()
        self._key_add = key_subparsers.add_parser("add", help="Add new key")
        self._key_generate = key_subparsers.add_parser("generate", help="Generate new key")

        self._key_list = key_subparsers.add_parser("list", help="List keys")

        self._key_add.add_argument("key", help="New key")

    def set_upload_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._upload.set_defaults(func=func)

    def set_download_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._download.set_defaults(func=func)

    def set_list_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._list.set_defaults(func=func)

    def set_storage_add_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._storage_add.set_defaults(func=func)

    def set_storage_list_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._storage_list.set_defaults(func=func)

    def set_storage_files_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._storage_files.set_defaults(func=func)

    def set_storage_delete_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._storage_delete.set_defaults(func=func)

    def set_storage_wipe_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._storage_wipe.set_defaults(func=func)

    def set_delete_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._delete.set_defaults(func=func)

    def set_key_add_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._key_add.set_defaults(func=func)

    def set_key_generate_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._key_generate.set_defaults(func=func)

    def set_key_list_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._key_list.set_defaults(func=func)

    def set_empty_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self.set_defaults(func=func)
