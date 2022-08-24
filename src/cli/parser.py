import argparse
from typing import Callable, Coroutine


class Parser(argparse.ArgumentParser):
    def __init__(self):
        super(Parser, self).__init__()

        self.add_argument('--config', dest='config_path', default="config.toml")

        subparsers = self.add_subparsers(parser_class=argparse.ArgumentParser)
        self._upload_subparser = subparsers.add_parser("upload")
        self._download_subparser = subparsers.add_parser("download")

        self._upload_subparser.add_argument("src")
        self._upload_subparser.add_argument("dst")

    def set_upload_handler(self, func: Callable[[], Coroutine[argparse.Action, None, None]]):
        self._upload_subparser.set_defaults(func=func)

    def set_download_handler(self, func: Callable[[argparse.Action], None]):
        self._download_subparser.set_defaults(func=func)
