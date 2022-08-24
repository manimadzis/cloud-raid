import argparse
from typing import Callable, Coroutine


class Parser(argparse.ArgumentParser):
    def __init__(self):
        super(Parser, self).__init__()

        self.add_argument('--config', dest='config_path', default="config.toml", help="Path to config file")
        self.add_argument('--debug', action="store_true", help="Enable debug output")

        subparsers = self.add_subparsers(parser_class=argparse.ArgumentParser)
        self._upload_subparser = subparsers.add_parser("upload", help="Upload file")
        self._download_subparser = subparsers.add_parser("download", help="Download file")

        self._upload_subparser.add_argument("src", help="Path to file")
        self._upload_subparser.add_argument("dst", help="Filename in system", nargs='?', default="")

        self._download_subparser.add_argument("src", help="Path to file")
        self._download_subparser.add_argument("dst", help="Filename in system", nargs='?', default="")

    def set_upload_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._upload_subparser.set_defaults(func=func)

    def set_download_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._download_subparser.set_defaults(func=func)
