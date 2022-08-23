import argparse
import asyncio
import os

from entities import File
from loguru import logger
from network.balancer import Balancer
from network.uploader import Uploader
from network.yandex_disk.upload import UploadStatus
from storage.block_repo import BlockRepo


class CLI:
    def __init__(self, block_repo: BlockRepo, balancer: Balancer):
        self._block_repo = block_repo
        self._balancer = balancer

        self._parser = argparse.ArgumentParser()
        self._subparsers = self._parser.add_subparsers()

        self._upload_subparser = self._subparsers.add_parser("upload")
        self._download = self._subparsers.add_parser("download")

        self._upload_subparser.add_argument("src")
        self._upload_subparser.add_argument("dst")
        self._upload_subparser.set_defaults(func=self._upload_accessor)

    async def _upload_accessor(self, args):
        print(type(args))
        src, dst = args.src, args.dst
        status = await self._upload_file(src, dst)
        logger.info(status)

    async def _upload_file(self, src: str, dst: str = None) -> UploadStatus:
        async with Uploader(self._balancer, await self._block_repo) as u:
            if not dst:
                _, dst = os.path.split(src)

            file = File(filename=dst, path=src)
            return await u.upload_file(file)

    def _parse(self):
        return self._parser.parse_args()

    async def start(self):
        args = self._parse()
        await args.func(args)

