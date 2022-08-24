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
from storage.block_repo import BlockRepo


class CLI:
    def __init__(self, config: Config, parser: Parser):
        async def wrapper(block_repo: BlockRepo):
            return await block_repo

        #
        with ThreadPoolExecutor() as pool:
            self._block_repo = pool.submit(asyncio.run, wrapper(BlockRepo(config.db_path))).result()
            disks = pool.submit(asyncio.run, (self._block_repo.get_disks())).result()

        self._balancer = Balancer(disks)

        self._parser = parser

        self._init_parser()

    @staticmethod
    def _replace_line(s: str):
        print("\r" + s, end='')

    def _init_parser(self):
        self._parser.set_upload_handler(self._upload_handler)
        self._parser.set_download_handler(self._download_handler)

    async def _upload_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        if not dst:
            _, dst = os.path.split(src)
        print(f"Upload file {repr(src)} like {repr(dst)}")
        await self._upload_file(src, dst)
        print("\nFile successfully uploaded")

    async def _download_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        if not dst:
            _, dst = os.path.split(src)
        print(f"Start downloading file {repr(src)} to {repr(dst)}")
        await self._download_file(src, dst)
        print(f"File {src} successfully downloaded to {dst}")

    async def _download_file(self, src: str, dst: str) -> None:
        async with Downloader(self._block_repo) as downloader:
            total_count = downloader.count_blocks(src)
            async for done in await downloader.download_file(File(filename=src, path=dst)):
                self._replace_line(f"{done}/{total_count} blocks downloaded")

    async def _upload_file(self, src: str, dst: str) -> None:
        async with Uploader(self._balancer, await self._block_repo) as u:
            file = File(filename=dst, path=src)
            total_blocks = u.count_blocks(file)

            try:
                async for done in u.upload_file(file):
                    self._replace_line(f"{done}/{total_blocks} blocks uploaded")
            except Exception as e:
                logger.exception(e)
                raise

    def _parse(self):
        return self._parser.parse_args()

    async def start(self):
        args = self._parse()
        await args.func(args)
