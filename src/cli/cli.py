import argparse
import os

import aiosqlite
from loguru import logger

from cli.parser import Parser
from config import Config
from entities import File, Disk
from network.balancer import Balancer
from network.downloader import Downloader
from network.uploader import Uploader
from storage.block_repo import BlockRepo
from vfs import VFS


class CLI:
    def __init__(self, config: Config, parser: Parser):
        self._balancer: Balancer = None
        self._block_repo: BlockRepo = None
        self._vfs: VFS = None
        self._parser = parser
        self.config = config
        self._init_parser()

    async def init(self):
        self._block_repo = await BlockRepo(self.config.db_path)
        disks = await self._block_repo.get_disks()
        self._balancer = Balancer(disks)

    @staticmethod
    def _replace_line(s: str):
        print("\r" + s, end='')

    def _init_parser(self):
        self._parser.set_upload_handler(self._upload_handler)
        self._parser.set_download_handler(self._download_handler)
        self._parser.set_adddisk_handler(self._adddisk_handler)
        self._parser.set_list_handler(self._list_handler)

    async def _upload_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        if not dst:
            _, dst = os.path.split(src)
        print(f"Upload file {repr(src)} like {repr(dst)}")
        await self._upload_file(src, dst)
        print("\nFile successfully uploaded")

    async def _download_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        temp_dir = args.temp_dir
        if not dst:
            _, dst = os.path.split(src)

        if not os.path.isdir(temp_dir):
            os.makedirs(temp_dir)

        print(f"Start downloading file {repr(src)} to {repr(dst)}")
        await self._download_file(src, dst, temp_dir)
        print(f"\nFile {src} successfully downloaded to {dst}")

    async def _adddisk_handler(self, args: argparse.Action):
        tokens = args.tokens
        for token in tokens:
            try:
                await self._block_repo.add_disk(Disk(token=token))
                print(f"Token {token} added")
            except aiosqlite.IntegrityError as e:
                print(f"Token {token} already exists")
                logger.exception(e)
        await self._block_repo.commit()

    async def _list_handler(self, args: argparse.Action):
        self._vfs = VFS(self._block_repo)
        await self._vfs.load()
        self._vfs.tree()

    async def _download_file(self, src: str, dst: str, temp_dir: str) -> None:
        async with Downloader(self._block_repo) as downloader:
            file = await self._block_repo.get_file_by_filename(File(filename=src))
            file.path = dst

            total_count = await downloader.count_blocks(file)

            async for done in downloader.download_file(file, temp_dir=temp_dir):
                self._replace_line(f"{done}/{total_count} blocks downloaded")

    async def _upload_file(self, src: str, dst: str) -> None:
        async with Uploader(self._balancer, self._block_repo) as u:
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
        await self._block_repo.close()
