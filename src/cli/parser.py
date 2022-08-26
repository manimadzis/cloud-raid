import argparse
from typing import Callable, Coroutine

from entities import File


class Parser(argparse.ArgumentParser):
    def __init__(self):
        super(Parser, self).__init__()

        self.add_argument('--config', dest='config_path', default="config.toml", help="Path to config file")
        self.add_argument('--debug', action="store_true", help="Enable debug output")
        self.add_argument('--log', help="Path to log file", default="log.txt")

        subparsers = self.add_subparsers(parser_class=argparse.ArgumentParser)
        self._upload_subparser = subparsers.add_parser("upload", help="Upload file")
        self._download_subparser = subparsers.add_parser("download", help="Download file")
        self._adddisk_subparser = subparsers.add_parser("adddisk", help="Add new disks")
        self._list_subparser = subparsers.add_parser("list", help="Show list of files")
        self._listdisk_subparser = subparsers.add_parser("listdisk", help="Show list of disks")

        self._upload_subparser.add_argument("src", help="Path to file")
        self._upload_subparser.add_argument("dst", help="Filename in system", nargs='?', default="")
        self._upload_subparser.add_argument("-b", "--block-size", help="Size of block in bytes", type=int,
                                            default=File.block_size)

        self._download_subparser.add_argument("src", help="Path to file")
        self._download_subparser.add_argument("dst", help="Filename in system", nargs='?', default="")
        self._download_subparser.add_argument("--temp-dir", help="Path to temp directory", default="blocks",
                                              dest="temp_dir")

        self._adddisk_subparser.add_argument("tokens", help="List of tokens", nargs="+")

    def set_upload_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._upload_subparser.set_defaults(func=func)

    def set_download_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._download_subparser.set_defaults(func=func)

    def set_adddisk_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._adddisk_subparser.set_defaults(func=func)

    def set_list_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._list_subparser.set_defaults(func=func)

    def set_listdisk_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._listdisk_subparser.set_defaults(func=func)
