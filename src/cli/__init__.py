import argparse
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from loguru import logger

from cli.parser import Parser
from config import Config
from entities import File
from network.balancer import Balancer
from network.downloader import Downloader
from network.uploader import Uploader
from network.yandex_disk.dowload import DownloadStatus
from network.yandex_disk.upload import UploadStatus
from storage.block_repo import BlockRepo


class CLI:
    def __init__(self, config: Config, parser: Parser):
        pool = ThreadPoolExecutor()

        async def wrapper(block_repo: BlockRepo):
            return await block_repo

        self._block_repo = pool.submit(asyncio.run, wrapper(BlockRepo(config.db_path))).result()
        disks = pool.submit(asyncio.run, (self._block_repo.get_disks())).result()
        self._balancer = Balancer(disks)

        self._parser = parser

        self._init_parser()

    def _init_parser(self):
        self._parser.set_upload_handler(self._upload_accessor)
        self._parser.set_download_handler(self._download_accessor)

    async def _upload_accessor(self, args: argparse.Action):
        src, dst = args.src, args.dst
        status = await self._upload_file(src, dst)
        logger.info(status)

    async def _download_accessor(self, args: argparse.Action):
        src, dst = args.src, args.dst
        status = await self._download_file(src, dst)
        logger.info(status)

    async def _download_file(self, src: str, dst: str) -> DownloadStatus:
        async with Downloader(self._block_repo) as downloader:
            status = await downloader.download(File(filename=src, path=dst))

        return status

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
